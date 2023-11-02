package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/armosec/armoapi-go/identifiers"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/messaging/pulsar/common/synchronizer"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
	"go.uber.org/zap"
)

const (
	SynchronizerServerProducerKey = "SynchronizerServerProducer"
)

type Client struct {
	callbacks       domain.Callbacks
	cfg             config.Config
	pulsarClient    pulsarconnector.Client
	topicToProducer map[pulsarconnector.TopicName]pulsarconnector.Producer

	synchronizerConsumer pulsarconnector.Consumer
	synchronizerChannel  chan pulsar.ConsumerMessage
	k8sObjectsConsumer   pulsarconnector.Consumer
	k8sObjectsChannel    chan pulsar.ConsumerMessage
}

func NewClient(cfg config.Config, pulsarClient pulsarconnector.Client) *Client {
	return &Client{
		cfg:          cfg,
		pulsarClient: pulsarClient,
	}
}

var _ adapters.Client = (*Client)(nil)

func (c *Client) Init(ctx context.Context) error {
	var err error
	if err := c.initPulsarProducers(); err != nil {
		return err
	}

	c.k8sObjectsChannel = make(chan pulsar.ConsumerMessage)
	c.k8sObjectsConsumer, err = c.pulsarClient.NewConsumer(pulsarconnector.WithTopic(c.cfg.Backend.Topic),
		pulsarconnector.WithMessageChannel(c.k8sObjectsChannel),
		pulsarconnector.WithSubscriptionName(c.cfg.Backend.Subscription))
	if err != nil {
		return fmt.Errorf("failed to create k8sObjectsConsumer: %w", err)
	}
	c.synchronizerChannel = make(chan pulsar.ConsumerMessage)
	c.synchronizerConsumer, err = c.pulsarClient.NewConsumer(pulsarconnector.WithTopic(c.cfg.Backend.SyncTopic),
		pulsarconnector.WithMessageChannel(c.synchronizerChannel),
		pulsarconnector.WithSubscriptionName(c.cfg.Backend.Subscription))
	if err != nil {
		return fmt.Errorf("failed to create synchronizerConsumer: %w", err)
	}
	return nil
}

func (c *Client) Start(mainCtx context.Context) error {

	defer c.k8sObjectsConsumer.Close()
	defer c.synchronizerConsumer.Close()
	for {
		select {
		case <-mainCtx.Done():
			close(c.k8sObjectsChannel)
			close(c.synchronizerChannel)
			return nil
		case msg := <-c.k8sObjectsChannel: // same as k8s_objects_ingester
			if msg.Key() == SynchronizerServerProducerKey {
				if err := c.k8sObjectsConsumer.Ack(msg); err != nil {
					logger.L().Error("failed to ack message", helpers.Error(err))
				}
				continue
			}
			c.handleSingleK8sObjectMessage(msg)
		case msg := <-c.synchronizerChannel: // dedicated topic for synchronizer_ingester
			if msg.Key() == SynchronizerServerProducerKey {
				if err := c.synchronizerConsumer.Ack(msg); err != nil {
					logger.L().Error("failed to ack message", helpers.Error(err))
				}
				continue
			}
			c.handleSingleSynchronizerMessage(msg)
		}
	}
}

// based on github.com/kubescape/event-ingester-service/ingesters/k8s_objects_ingester/k8s_objects.go:func handleSingleK8sObjectMessage
func (c *Client) handleSingleK8sObjectMessage(msg pulsar.ConsumerMessage) {
	msgID := utils.PulsarMessageIDtoString(msg.ID())
	msgTopic := msg.Topic()
	logger.L().Debug("Received message from pulsar",
		helpers.String("msgId", msgID),
		helpers.String("subscriptionName", c.cfg.Backend.Subscription),
		helpers.String("topicName", string(c.cfg.Backend.Topic)),
		helpers.String("msgTopic", msgTopic))
	// TODO: implement
	c.k8sObjectsConsumer.Ack(msg)

	// props := msg.Properties()
	// kindStr := props[identifiers.AttributeKind]
	// kindLower := Kind(strings.ToLower(kindStr))
	// if kindLower == "" {
	// kindLower = kindClusterConnection
	// }
	// kindStr = string(kindLower)
	// customerGUID := props[identifiers.AttributeCustomerGUID]
	// clusterName := props["clusterName"]
	// if clusterName == "" {
	// clusterName = props[identifiers.AttributeCluster]
	// }
	//itemKey := fmt.Sprintf("%s:%s/%s/%s/%s/%s/%s", msg.Topic(), customerGUID, msg.ID().String(), clusterName, props["objID"], kindStr, props["actionType"])
	// isDelete := props["actionType"] == "delete"

	// fmt.Println(customerGUID, isDelete)
	// FIXME do we need the session handler here?
	//var nackErr error

	//switch kindLower {
	//case kindClusterConnection, kindCluster:
	//	connectionTime, err := time.Parse(time.RFC3339Nano, props["connectionTime"])
	//	if err != nil {
	//		sh.LogError("Failed to parse connectionTime", err)
	//
	//		connectionTime = time.Now()
	//	}
	//	switch props["status"] {
	//	case connectionRenew:
	//		nackErr = markClusterReconnect(sh, clusterName, connectionTime)
	//	case connectionLost:
	//		diconnectionTime, err := time.Parse(time.RFC3339Nano, props["disconnectedTime"])
	//		if err != nil {
	//			sh.LogError("Failed to parse disconnectionTime", err)
	//
	//			diconnectionTime = time.Now()
	//		}
	//		nackErr = markClusterDisconnected(sh, clusterName, connectionTime, diconnectionTime)
	//	default:
	//		sh.LogError("Received unknown message from pulsar", fmt.Errorf("unknown status %s", props["status"]))
	//	}
	//case kindAPIServerVersion:
	//	nackErr = updateClusterAPIServerInfo(sh, msg, clusterName)
	//	if nackErr == nil {
	//
	//		_, nackErr = sh.GetConfigServiceConnector().GetOrCreateCluster(clusterName, customerGUID)
	//	}
	//case kindInstallationData:
	//	clusterObj, err := sh.GetConfigServiceConnector().GetOrCreateCluster(clusterName, customerGUID)
	//	if err != nil {
	//		nackErr = fmt.Errorf("in UpdateInstallationDataInfo failed to GetOrCreateCluster %w", err)
	//	} else {
	//		nackErr = updateInstallationDataInfo(sh, clusterObj, msg, clusterName)
	//	}
	//case kindNamespace:
	//	nackErr = processK8sNamespace(sh, msg, clusterName, isDelete)
	//case kindMicroservice:
	//	nackErr = processMicroservice(sh, msg, clusterName, isDelete)
	//}
	//if nackErr != nil {
	//	sh.LogError("Failed to process message", nackErr)
	//	consumer.Nack(msg)
	//} else {
	//	consumer.Ack(msg)
	//}
}

func (c *Client) handleSingleSynchronizerMessage(msg pulsar.ConsumerMessage) {
	msgID := utils.PulsarMessageIDtoString(msg.ID())
	msgTopic := msg.Topic()
	logger.L().Debug("Received message from pulsar",
		helpers.String("msgId", msgID),
		helpers.String("subscriptionName", c.cfg.Backend.Subscription),
		helpers.String("topicName", string(c.cfg.Backend.Topic)),
		helpers.String("msgTopic", msgTopic))

	// TODO: implement
}

// FIXME no need to implement callPutOrPatch because we don't send patches from backend
// so you just call c.callbacks.PutObject instead

func (c *Client) callVerifyObject(ctx context.Context, id domain.ClusterKindName, object []byte) error {
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

func (c *Client) DeleteObject(ctx context.Context, id domain.ClusterKindName) error {
	// deleting the object is delegated to the ingester
	return c.sendDeleteObjectMessage(ctx, id)
}

func (c *Client) GetObject(ctx context.Context, id domain.ClusterKindName, baseObject []byte) error {
	// getting the object is delegated to the ingester, we will send it to cluster once we get a NewObject message from Pulsar
	return c.sendGetObjectMessage(ctx, id, baseObject)
}

func (c *Client) PatchObject(ctx context.Context, id domain.ClusterKindName, checksum string, patch []byte) error {
	// patching the object is delegated to the ingester, it might emit a GetObject message if the patching fails
	return c.sendPatchObjectMessage(ctx, id, checksum, patch)
}

func (c *Client) PutObject(ctx context.Context, id domain.ClusterKindName, object []byte) error {
	// putting the object into the data store is delegated to the ingester
	return c.sendPutObjectMessage(ctx, id, object)
}

func (c *Client) RegisterCallbacks(callbacks domain.Callbacks) {
	c.callbacks = callbacks
}

func (c *Client) VerifyObject(ctx context.Context, id domain.ClusterKindName, checksum string) error {
	// verifying the object is delegated to the ingester, it might emit a GetObject message if the verification fails
	return c.sendVerifyObjectMessage(ctx, id, checksum)
}

func (c *Client) sendDeleteObjectMessage(ctx context.Context, id domain.ClusterKindName) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := synchronizer.DeleteObjectMessage{
		ClusterName:  id.Cluster,
		CustomerGUID: id.Account,
		Depth:        depth + 1,
		Kind:         id.Kind.String(),
		MsgId:        msgId,
		Name:         id.Name,
	}
	logger.L().Debug("Sending delete object message to pulsar",
		helpers.String("cluster", msg.ClusterName),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal delete object message: %w", err)
	}

	return c.producePulsarMessage(ctx, c.cfg.Backend.Topic, id, data)
}

func (c *Client) sendGetObjectMessage(ctx context.Context, id domain.ClusterKindName, baseObject []byte) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := synchronizer.GetObjectMessage{
		BaseObject:   baseObject,
		ClusterName:  id.Cluster,
		CustomerGUID: id.Account,
		Depth:        depth + 1,
		Kind:         id.Kind.String(),
		MsgId:        msgId,
		Name:         id.Name,
	}
	logger.L().Debug("Sending get object message to pulsar",
		helpers.String("cluster", msg.ClusterName),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.Int("base object size", len(msg.BaseObject)))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal get object message: %w", err)
	}

	// sending to synchronizer topic
	return c.producePulsarMessage(ctx, c.cfg.Backend.SyncTopic, id, data)
}

func (c *Client) sendPatchObjectMessage(ctx context.Context, id domain.ClusterKindName, checksum string, patch []byte) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := synchronizer.PatchObjectMessage{
		Checksum:     checksum,
		ClusterName:  id.Cluster,
		CustomerGUID: id.Account,
		Depth:        depth + 1,
		Kind:         id.Kind.String(),
		MsgId:        msgId,
		Name:         id.Name,
		Patch:        patch,
	}
	logger.L().Debug("Sending patch object message to pulsar",
		helpers.String("cluster", msg.ClusterName),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.String("checksum", msg.Checksum),
		helpers.Int("patch size", len(msg.Patch)))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal patch object message: %w", err)
	}

	// sending to synchronizer topic
	return c.producePulsarMessage(ctx, c.cfg.Backend.SyncTopic, id, data)
}

func (c *Client) sendPutObjectMessage(ctx context.Context, id domain.ClusterKindName, object []byte) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := synchronizer.PutObjectMessage{
		ClusterName:  id.Cluster,
		CustomerGUID: id.Account,
		Depth:        depth + 1,
		Kind:         id.Kind.String(),
		MsgId:        msgId,
		Name:         id.Name,
		Object:       object,
	}
	logger.L().Debug("Sending put object message to pulsar",
		helpers.String("cluster", msg.ClusterName),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.Int("object size", len(msg.Object)))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal put object message: %w", err)
	}

	// sending to k8s_objects topic
	return c.producePulsarMessage(ctx, c.cfg.Backend.Topic, id, data)
}

func (c *Client) sendVerifyObjectMessage(ctx context.Context, id domain.ClusterKindName, checksum string) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := synchronizer.VerifyObjectMessage{
		Checksum:     checksum,
		ClusterName:  id.Cluster,
		CustomerGUID: id.Account,
		Depth:        depth + 1,
		Kind:         id.Kind.String(),
		MsgId:        msgId,
		Name:         id.Name,
	}
	logger.L().Debug("Sending verify object message to pulsar",
		helpers.String("cluster", msg.ClusterName),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.String("checksum", msg.Checksum))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal verify object message: %w", err)
	}

	// sending to synchronizer topic
	return c.producePulsarMessage(ctx, c.cfg.Backend.SyncTopic, id, data)
}

func getCommonProducerOptions(topic string) pulsar.ProducerOptions {
	// get pod name
	podName := os.Getenv("HOSTNAME")
	return pulsar.ProducerOptions{
		CompressionType:  pulsar.ZSTD,
		CompressionLevel: 1,
		Properties: map[string]string{
			"podName": podName,
		},
		Topic: topic,
	}
}

func (c *Client) initPulsarProducers() error {
	if c.pulsarClient == nil {
		return fmt.Errorf("pulsar client is nil, not initializing producer")
	}

	c.topicToProducer = make(map[pulsarconnector.TopicName]pulsarconnector.Producer)

	if producer, err := initPulsarProducer(c.pulsarClient, string(c.cfg.Backend.Topic)); err != nil {
		return err
	} else {
		c.topicToProducer[c.cfg.Backend.Topic] = producer
	}

	if producer, err := initPulsarProducer(c.pulsarClient, string(c.cfg.Backend.SyncTopic)); err != nil {
		return err
	} else {
		c.topicToProducer[c.cfg.Backend.SyncTopic] = producer
	}

	return nil
}

func initPulsarProducer(pulsarClient pulsarconnector.Client, topic string) (pulsarconnector.Producer, error) {
	fulTopic := pulsarconnector.BuildPersistentTopic(pulsarClient.GetConfig().Tenant, pulsarClient.GetConfig().Namespace, pulsarconnector.TopicName(topic))
	ksr, err := pulsarClient.CreateProducer(getCommonProducerOptions(fulTopic))
	if err != nil {
		return nil, fmt.Errorf("failed to create producer for topic '%s': %w", topic, err)
	}
	return ksr, nil
}

func logSendErrors(msgID pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
	if err != nil {
		zap.L().Error("failed to send message to pulsar", zap.Error(err))
	} else {
		zap.L().Debug("successfully sent message to pulsar", zap.String("messageID", msgID.String()), zap.Any("messageProperties", message.Properties))
	}
}

func (c *Client) producePulsarMessage(ctx context.Context, topic pulsarconnector.TopicName, id domain.ClusterKindName, payload []byte) error {
	producer := c.topicToProducer[topic]
	if producer == nil {
		return fmt.Errorf("no producer for topic '%s'", topic)
	}

	producerMessageProperties := map[string]string{
		"timestamp":                       time.Now().Format(time.RFC3339Nano),
		identifiers.AttributeCustomerGUID: id.Account,
		"clusterName":                     id.Cluster,
	}
	producerMessage := &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: producerMessageProperties,
		// this will be used to filter out messages by the synchronizer server consumer (so it doesn't process its own messages)
		Key: SynchronizerServerProducerKey,
	}
	producer.SendAsync(ctx, producerMessage, logSendErrors)
	return nil
}
