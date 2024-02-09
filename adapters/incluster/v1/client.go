package incluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/multierr"

	mapset "github.com/deckarep/golang-set/v2"
	jsonpatch "github.com/evanphx/json-patch"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

type BatchProcessingFunc func(context.Context, *Client, domain.BatchItems) error

// resourceVersionGetter is an interface used to get resource version from events.
type resourceVersionGetter interface {
	GetResourceVersion() string
}

type Client struct {
	client              dynamic.Interface
	account             string
	cluster             string
	kind                *domain.Kind
	callbacks           domain.Callbacks
	res                 schema.GroupVersionResource
	ShadowObjects       map[string][]byte
	Strategy            domain.Strategy
	batchProcessingFunc map[domain.BatchType]BatchProcessingFunc
}

var errWatchClosed = errors.New("watch channel closed")

func NewClient(client dynamic.Interface, account, cluster string, r config.Resource) *Client {
	res := schema.GroupVersionResource{Group: r.Group, Version: r.Version, Resource: r.Resource}
	return &Client{
		account: account,
		client:  client,
		cluster: cluster,
		kind: &domain.Kind{
			Group:    res.Group,
			Version:  res.Version,
			Resource: res.Resource,
		},
		res:           res,
		ShadowObjects: map[string][]byte{},
		Strategy:      r.Strategy,
		batchProcessingFunc: map[domain.BatchType]BatchProcessingFunc{
			domain.DefaultBatch:        defaultBatchProcessingFunc, // regular processing, when batch type is not set
			domain.ReconciliationBatch: reconcileBatchProcessingFunc,
		},
	}
}

var _ adapters.Client = (*Client)(nil)

func (c *Client) Start(ctx context.Context) error {
	ctx = utils.ContextFromGeneric(ctx, domain.Generic{})
	logger.L().Info("starting incluster client", helpers.String("resource", c.res.Resource))
	watchOpts := metav1.ListOptions{}
	// for our storage, we need to list all resources and get them one by one
	// as list returns objects with empty spec
	// and watch does not return existing objects
	if c.res.Group == "spdx.softwarecomposition.kubescape.io" {
		if err := backoff.RetryNotify(func() error {
			var err error
			watchOpts.ResourceVersion, err = c.getExistingStorageObjects(ctx)
			return err
		}, utils.NewBackOff(), func(err error, d time.Duration) {
			logger.L().Ctx(ctx).Warning("get existing storage objects", helpers.Error(err),
				helpers.String("resource", c.res.Resource),
				helpers.String("retry in", d.String()))
		}); err != nil {
			return fmt.Errorf("giving up get existing storage objects: %w", err)
		}
	}
	// begin watch
	eventQueue := utils.NewCooldownQueue(utils.DefaultQueueSize, utils.DefaultTTL)
	go c.watchRetry(ctx, watchOpts, eventQueue)
	// process events
	for event := range eventQueue.ResultChan {
		// skip non-objects
		d, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			continue
		}

		// skip non-standalone resources
		if hasParent(d) {
			continue
		}
		id := domain.KindName{
			Kind:            c.kind,
			Name:            d.GetName(),
			Namespace:       d.GetNamespace(),
			ResourceVersion: domain.ToResourceVersion(d.GetResourceVersion()),
		}

		switch {
		case event.Type == watch.Added:
			logger.L().Debug("added resource", helpers.String("id", id.String()))
			newObject, err := c.getObjectFromUnstructured(d)
			if err != nil {
				logger.L().Ctx(ctx).Error("cannot get object", helpers.Error(err), helpers.String("id", id.String()))
				continue
			}
			err = c.callVerifyObject(ctx, id, newObject)
			if err != nil {
				logger.L().Ctx(ctx).Error("cannot handle added resource", helpers.Error(err), helpers.String("id", id.String()))
			}
		case event.Type == watch.Deleted:
			logger.L().Debug("deleted resource", helpers.String("id", id.String()))
			err := c.callbacks.DeleteObject(ctx, id)
			if err != nil {
				logger.L().Ctx(ctx).Error("cannot handle deleted resource", helpers.Error(err), helpers.String("id", id.String()))
			}
			if c.Strategy == domain.PatchStrategy {
				// remove from known resources
				delete(c.ShadowObjects, id.String())
			}
		case event.Type == watch.Modified:
			logger.L().Debug("modified resource", helpers.String("id", id.String()))
			newObject, err := c.getObjectFromUnstructured(d)
			if err != nil {
				logger.L().Ctx(ctx).Error("cannot get object", helpers.Error(err), helpers.String("id", id.String()))
				continue
			}
			err = c.callPutOrPatch(ctx, id, nil, newObject)
			if err != nil {
				logger.L().Ctx(ctx).Error("cannot handle modified resource", helpers.Error(err), helpers.String("id", id.String()))
			}
		}
	}
	return nil
}

func (c *Client) IsRelated(ctx context.Context, id domain.ClientIdentifier) bool {
	return c.account == id.Account && c.cluster == id.Cluster
}

func (c *Client) Stop(_ context.Context) error {
	return nil
}

func (c *Client) watchRetry(ctx context.Context, watchOpts metav1.ListOptions, eventQueue *utils.CooldownQueue) {
	if err := backoff.RetryNotify(func() error {
		watcher, err := c.client.Resource(c.res).Namespace("").Watch(context.Background(), watchOpts)
		if err != nil {
			return fmt.Errorf("client resource: %w", err)
		}
		logger.L().Info("starting watch", helpers.String("resource", c.res.Resource))
		for {
			event, chanActive := <-watcher.ResultChan()
			// set resource version to resume watch from
			// inspired by https://github.com/kubernetes/client-go/blob/5a0a4247921dd9e72d158aaa6c1ee124aba1da80/tools/watch/retrywatcher.go#L157
			if metaObject, ok := event.Object.(resourceVersionGetter); ok {
				watchOpts.ResourceVersion = metaObject.GetResourceVersion()
			}
			if eventQueue.Closed() {
				watcher.Stop()
				return backoff.Permanent(errors.New("event queue closed"))
			}
			if !chanActive {
				// channel closed, retry
				return errWatchClosed
			}
			if event.Type == watch.Error {
				return fmt.Errorf("watch error: %s", event.Object)
			}
			eventQueue.Enqueue(event)
		}
	}, utils.NewBackOff(), func(err error, d time.Duration) {
		if !errors.Is(err, errWatchClosed) {
			logger.L().Ctx(ctx).Warning("watch", helpers.Error(err),
				helpers.String("resource", c.res.Resource),
				helpers.String("retry in", d.String()))
		}
	}); err != nil {
		logger.L().Ctx(ctx).Fatal("giving up watch", helpers.Error(err),
			helpers.String("resource", c.res.Resource))
	}
}

// hasParent returns true if workload has a parent
// based on https://github.com/kubescape/k8s-interface/blob/2855cc94bd7666b227ad9e5db5ca25cb895e6cee/k8sinterface/k8sdynamic.go#L219
func hasParent(workload *unstructured.Unstructured) bool {
	if workload == nil {
		return false
	}
	// filter out non-controller workloads
	if !slices.Contains([]string{"Pod", "Job", "ReplicaSet"}, workload.GetKind()) {
		return false
	}
	// check if workload has owner
	ownerReferences := workload.GetOwnerReferences() // OwnerReferences in workload
	if len(ownerReferences) > 0 {
		return slices.Contains([]string{"apps/v1", "batch/v1", "batch/v1beta1"}, ownerReferences[0].APIVersion)
	}
	// check if workload is Pod with pod-template-hash label
	if workload.GetKind() == "Pod" {
		if podLabels := workload.GetLabels(); podLabels != nil {
			if podHash, ok := podLabels["pod-template-hash"]; ok && podHash != "" {
				return true
			}
		}
	}
	return false
}

func (c *Client) callPutOrPatch(ctx context.Context, id domain.KindName, baseObject []byte, newObject []byte) error {
	if c.Strategy == domain.PatchStrategy {
		if len(baseObject) > 0 {
			// update reference object
			c.ShadowObjects[id.String()] = baseObject
		}
		if oldObject, ok := c.ShadowObjects[id.String()]; ok {
			// calculate checksum
			checksum, err := utils.CanonicalHash(newObject)
			if err != nil {
				return fmt.Errorf("calculate checksum: %w", err)
			}
			// calculate patch
			patch, err := jsonpatch.CreateMergePatch(oldObject, newObject)
			if err != nil {
				return fmt.Errorf("create merge patch: %w", err)
			}
			err = c.callbacks.PatchObject(ctx, id, checksum, patch)
			if err != nil {
				return fmt.Errorf("send patch object: %w", err)
			}
		} else {
			err := c.callbacks.PutObject(ctx, id, newObject)
			if err != nil {
				return fmt.Errorf("send put object: %w", err)
			}
		}
		// add/update known resources
		c.ShadowObjects[id.String()] = newObject
	} else {
		err := c.callbacks.PutObject(ctx, id, newObject)
		if err != nil {
			return fmt.Errorf("send put object: %w", err)
		}
	}
	return nil
}

func (c *Client) callVerifyObject(ctx context.Context, id domain.KindName, object []byte) error {
	// calculate checksum
	checksum, err := utils.CanonicalHash(object)
	if err != nil {
		return fmt.Errorf("calculate checksum: %w", err)
	}
	err = c.callbacks.VerifyObject(ctx, id, checksum)
	if err != nil {
		return fmt.Errorf("send checksum: %w", err)
	}
	return nil
}

func (c *Client) DeleteObject(_ context.Context, id domain.KindName) error {
	if c.Strategy == domain.PatchStrategy {
		// remove from known resources
		delete(c.ShadowObjects, id.String())
	}
	return c.client.Resource(c.res).Namespace(id.Namespace).Delete(context.Background(), id.Name, metav1.DeleteOptions{})
}

func (c *Client) GetObject(ctx context.Context, id domain.KindName, baseObject []byte) error {
	obj, err := c.client.Resource(c.res).Namespace(id.Namespace).Get(context.Background(), id.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get resource: %w", err)
	}
	newObject, err := utils.FilterAndMarshal(obj)
	if err != nil {
		return fmt.Errorf("marshal resource: %w", err)
	}
	return c.callPutOrPatch(ctx, id, baseObject, newObject)
}

func (c *Client) PatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	baseObject, err := c.patchObject(ctx, id, checksum, patch)
	if err != nil {
		logger.L().Ctx(ctx).Warning("patch object, sending get object", helpers.Error(err), helpers.String("id", id.String()))
		return c.callbacks.GetObject(ctx, id, baseObject)
	}
	return nil
}

func (c *Client) patchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) ([]byte, error) {
	if c.Strategy != domain.PatchStrategy {
		return nil, fmt.Errorf("patch strategy not enabled for resource %s", id.Kind.String())
	}
	obj, err := c.client.Resource(c.res).Namespace(id.Namespace).Get(context.Background(), id.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get resource: %w", err)
	}
	object, err := utils.FilterAndMarshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal resource: %w", err)
	}
	// apply patch
	modified, err := jsonpatch.MergePatch(object, patch)
	if err != nil {
		return object, fmt.Errorf("apply patch: %w", err)
	}
	// verify checksum
	newChecksum, err := utils.CanonicalHash(modified)
	if err != nil {
		return object, fmt.Errorf("calculate checksum: %w", err)
	}
	if newChecksum != checksum {
		return object, fmt.Errorf("checksum mismatch: %s != %s", newChecksum, checksum)
	}
	// update known resources
	c.ShadowObjects[id.String()] = modified
	// save object
	return object, c.PutObject(ctx, id, modified)
}

func (c *Client) PutObject(_ context.Context, id domain.KindName, object []byte) error {
	var obj unstructured.Unstructured
	err := obj.UnmarshalJSON(object)
	if err != nil {
		return fmt.Errorf("unmarshal object: %w", err)
	}
	// use apply to create or update object, we want to overwrite existing objects
	_, err = c.client.Resource(c.res).Namespace(id.Namespace).Apply(context.Background(), id.Name, &obj, metav1.ApplyOptions{FieldManager: "application/apply-patch"})
	if err != nil {
		return fmt.Errorf("apply resource: %w", err)
	}
	return nil
}

func (c *Client) RegisterCallbacks(_ context.Context, callbacks domain.Callbacks) {
	c.callbacks = callbacks
}

func (c *Client) Callbacks(_ context.Context) (domain.Callbacks, error) {
	return c.callbacks, nil
}

func (c *Client) VerifyObject(ctx context.Context, id domain.KindName, newChecksum string) error {
	baseObject, err := c.verifyObject(id, newChecksum)
	if err != nil {
		logger.L().Ctx(ctx).Warning("verify object, sending get object", helpers.Error(err), helpers.String("id", id.String()))
		return c.callbacks.GetObject(ctx, id, baseObject)
	}
	return nil
}

func (c *Client) Batch(ctx context.Context, kind domain.Kind, batchType domain.BatchType, items domain.BatchItems) error {
	if f, ok := c.batchProcessingFunc[batchType]; ok {
		logger.L().Debug("batch processing", helpers.String("batch type", string(batchType)))
		return f(ctx, c, items)
	}

	return fmt.Errorf("batch type %s not supported", batchType)
}

func (c *Client) verifyObject(id domain.KindName, newChecksum string) ([]byte, error) {
	obj, err := c.client.Resource(c.res).Namespace(id.Namespace).Get(context.Background(), id.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get resource: %w", err)
	}
	object, err := utils.FilterAndMarshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal resource: %w", err)
	}
	checksum, err := utils.CanonicalHash(object)
	if err != nil {
		return object, fmt.Errorf("calculate checksum: %w", err)
	}
	if checksum != newChecksum {
		return object, fmt.Errorf("checksum mismatch: %s != %s", newChecksum, checksum)
	}
	return object, nil
}

func (c *Client) getExistingStorageObjects(ctx context.Context) (string, error) {
	logger.L().Debug("getting existing objects from storage", helpers.String("resource", c.res.Resource))
	list, err := c.client.Resource(c.res).Namespace("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list resources: %w", err)
	}
	for _, d := range list.Items {
		id := domain.KindName{
			Kind:            c.kind,
			Name:            d.GetName(),
			Namespace:       d.GetNamespace(),
			ResourceVersion: domain.ToResourceVersion(d.GetResourceVersion()),
		}
		obj, err := c.client.Resource(c.res).Namespace(d.GetNamespace()).Get(context.Background(), d.GetName(), metav1.GetOptions{})
		if err != nil {
			logger.L().Ctx(ctx).Error("cannot get object", helpers.Error(err), helpers.String("id", id.String()))
			continue
		}
		newObject, err := utils.FilterAndMarshal(obj)
		if err != nil {
			logger.L().Ctx(ctx).Error("cannot marshal object", helpers.Error(err), helpers.String("id", id.String()))
			continue
		}
		err = c.callVerifyObject(ctx, id, newObject)
		if err != nil {
			logger.L().Ctx(ctx).Error("cannot handle added resource", helpers.Error(err), helpers.String("id", id.String()))
			continue
		}
	}
	// set resource version to watch from
	return list.GetResourceVersion(), nil
}

func (c *Client) getObjectFromUnstructured(d *unstructured.Unstructured) ([]byte, error) {
	if c.res.Group == "spdx.softwarecomposition.kubescape.io" {
		obj, err := c.client.Resource(c.res).Namespace(d.GetNamespace()).Get(context.Background(), d.GetName(), metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get resource: %w", err)
		}
		return utils.FilterAndMarshal(obj)
	}
	return utils.FilterAndMarshal(d)
}

// Batch processing functions
func reconcileBatchProcessingFunc(ctx context.Context, c *Client, items domain.BatchItems) error {
	logger.L().Debug("reconciliation batch started", helpers.String("resource", c.res.Resource))
	if len(items.NewChecksum) == 0 {
		return fmt.Errorf("reconciliation batch (%s) was empty - expected at least one NewChecksum message", c.res.Resource)
	}

	// create a map of resources from the client
	list, err := c.client.Resource(c.res).Namespace("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list resources: %w", err)
	}
	clientItems := map[string]unstructured.Unstructured{}
	clientItemsSet := mapset.NewSet[string]()
	for _, item := range list.Items {
		k := fmt.Sprintf("%s/%s", item.GetNamespace(), item.GetName())
		clientItems[k] = item
		clientItemsSet.Add(k)
	}

	// create a map of resources from the server
	serverItems := map[string]domain.NewChecksum{}
	serverItemsSet := mapset.NewSet[string]()
	for _, item := range items.NewChecksum {
		k := fmt.Sprintf("%s/%s", item.Namespace, item.Name)
		serverItems[k] = item
		serverItemsSet.Add(k)
	}

	// resources that should not be in server, send delete
	for _, k := range serverItemsSet.Difference(clientItemsSet).ToSlice() {
		item := serverItems[k]

		id := domain.KindName{
			Kind:            c.kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}

		logger.L().Debug("resource should not be in server, sending delete message",
			helpers.String("resource", item.Kind.String()),
			helpers.String("name", item.Name),
			helpers.String("namespace", item.Namespace))
		err = multierr.Append(err, c.callbacks.DeleteObject(ctx, id))
	}

	// resources in common, check resource version
	for _, k := range serverItemsSet.Intersect(clientItemsSet).ToSlice() {
		item := serverItems[k]
		resource := clientItems[k]
		currentVersion := domain.ToResourceVersion(resource.GetResourceVersion())
		if currentVersion == item.ResourceVersion {
			// resource has same version, skipping
			logger.L().Debug("resource has same version, skipping",
				helpers.String("resource", item.Kind.String()),
				helpers.String("name", item.Name),
				helpers.String("namespace", item.Namespace),
				helpers.Int("resource version", currentVersion))
			continue
		}

		// resource has changed, sending a put message
		logger.L().Debug("resource has changed, sending put message",
			helpers.String("resource", item.Kind.String()),
			helpers.String("name", item.Name),
			helpers.String("namespace", item.Namespace),
			helpers.Int("batch resource version", item.ResourceVersion),
			helpers.Int("current resource version", currentVersion))
		newObject, marshalErr := c.getObjectFromUnstructured(&resource)
		if marshalErr != nil {
			err = multierr.Append(err, fmt.Errorf("marshal resource: %w", marshalErr))
			continue
		}
		id := domain.KindName{
			Kind:            c.kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}
		err = multierr.Append(err, c.callbacks.PutObject(ctx, id, newObject))
	}

	// resources missing in server, send verify checksum
	for _, k := range clientItemsSet.Difference(serverItemsSet).ToSlice() {
		item := clientItems[k]

		resourceVersion := domain.ToResourceVersion(item.GetResourceVersion())
		id := domain.KindName{
			Kind:            c.kind,
			Name:            item.GetName(),
			Namespace:       item.GetNamespace(),
			ResourceVersion: resourceVersion,
		}

		newObject, marshalErr := utils.FilterAndMarshal(&item)
		if marshalErr != nil {
			err = multierr.Append(err, fmt.Errorf("marshal resource: %w", marshalErr))
			continue
		}

		logger.L().Debug("resource missing in server, sending verify message",
			helpers.String("resource", c.res.Resource),
			helpers.String("name", item.GetName()),
			helpers.String("namespace", item.GetNamespace()))
		// remove cached object
		delete(c.ShadowObjects, id.String())
		// send verify message
		err = multierr.Append(err, c.callVerifyObject(ctx, id, newObject))
	}

	return nil
}

func findResourceInList(list []unstructured.Unstructured, namespace, name string) int {
	for i, resource := range list {
		if resource.GetName() == name && resource.GetNamespace() == namespace {
			return i
		}
	}

	return -1
}

func defaultBatchProcessingFunc(ctx context.Context, c *Client, items domain.BatchItems) error {
	var err error
	for _, item := range items.GetObject {
		id := domain.KindName{
			Kind:            c.kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}
		err = multierr.Append(err, c.GetObject(ctx, id, []byte(item.BaseObject)))
	}

	for _, item := range items.NewChecksum {
		id := domain.KindName{
			Kind:            c.kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}
		err = multierr.Append(err, c.VerifyObject(ctx, id, item.Checksum))
	}

	for _, item := range items.ObjectDeleted {
		id := domain.KindName{
			Kind:            c.kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}
		err = multierr.Append(err, c.DeleteObject(ctx, id))
	}

	for _, item := range items.PatchObject {
		id := domain.KindName{
			Kind:            c.kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}
		err = multierr.Append(err, c.PatchObject(ctx, id, item.Checksum, []byte(item.Patch)))
	}

	for _, item := range items.PutObject {
		id := domain.KindName{
			Kind:            c.kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}
		err = multierr.Append(err, c.PutObject(ctx, id, []byte(item.Object)))
	}
	return err
}
