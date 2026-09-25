package incluster

import (
	"context"
	"sync"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

const namespaceFiltersKey = "namespaceFilters.json"

var configMapResource = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

type namespaceFilterWatcher struct {
	informer        cache.SharedIndexInformer
	filters         *namespaceFilters
	namespace, name string
	ready           chan struct{}
	once            sync.Once
}

func newNamespaceFilterWatcher(client dynamic.Interface, namespace, name string, filters *namespaceFilters) (*namespaceFilterWatcher, error) {
	w := &namespaceFilterWatcher{filters: filters, namespace: namespace, name: name, ready: make(chan struct{})}
	w.informer = dynamicinformer.NewFilteredDynamicInformer(client, configMapResource, namespace, 0, cache.Indexers{}, func(options *metav1.ListOptions) {
		options.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
	}).Informer()
	_, err := w.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.apply,
		UpdateFunc: func(_, obj any) { w.apply(obj) },
		DeleteFunc: func(_ any) {
			logger.L().Warning("namespace filter ConfigMap deleted; retaining last valid filters", helpers.String("configMap", name))
		},
	})
	return w, err
}

func (w *namespaceFilterWatcher) apply(obj any) {
	cm, ok := obj.(*unstructured.Unstructured)
	if !ok || cm.GetNamespace() != w.namespace || cm.GetName() != w.name {
		return
	}
	data, found, err := unstructured.NestedString(cm.Object, "data", namespaceFiltersKey)
	if err != nil || !found {
		logger.L().Warning("namespace filter ConfigMap is missing valid namespaceFilters.json; retaining last valid filters", helpers.String("configMap", w.name))
		return
	}
	changed, err := w.filters.update([]byte(data))
	if err != nil {
		logger.L().Warning("namespace filter ConfigMap rejected; retaining last valid filters", helpers.String("configMap", w.name), helpers.Error(err))
		return
	}
	if changed {
		logger.L().Info("namespace filters updated", helpers.String("configMap", w.name), helpers.String("resourceVersion", cm.GetResourceVersion()))
	}
	w.once.Do(func() { close(w.ready) })
}

func (w *namespaceFilterWatcher) run(ctx context.Context) {
	logger.L().Info("waiting for valid namespace filter ConfigMap", helpers.String("configMap", w.name), helpers.String("namespace", w.namespace))
	w.informer.Run(ctx.Done())
}

func (w *namespaceFilterWatcher) waitForReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.ready:
		return ctx.Err()
	}
}
