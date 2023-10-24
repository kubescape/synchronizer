package backend

import (
	"context"
	"fmt"
	"log"
	"strings"

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
)

type Kind string

var (
	kindMicroservice      = Kind("microservice")
	kindCluster           = Kind("cluster")
	kindNamespace         = Kind("namespace")
	kindAPIServerVersion  = Kind("clusterapiserverversion")
	kindInstallationData  = Kind("installationdata")
	kindClusterConnection = Kind("clusterconnect")
)

const (
	connectionRenew = "newConnection"
	connectionLost  = "disconnected"
)

type BackendAdapter struct {
	callbacks    domain.Callbacks
	cfg          config.Config
	ctx          context.Context
	pulsarClient pulsarconnector.Client
}

func NewBackendAdapter(ctx context.Context, cfg config.Config, pulsarClient pulsarconnector.Client) *BackendAdapter {
	return &BackendAdapter{
		cfg:          cfg,
		ctx:          ctx,
		pulsarClient: pulsarClient,
	}
}

var _ adapters.Adapter = (*BackendAdapter)(nil)

func (b BackendAdapter) DeleteObject(id domain.ClusterKindName) error {
	return b.sendDeleteObjectMessage(id)
}

func (b BackendAdapter) GetObject(id domain.ClusterKindName, baseObject []byte) error {
	return b.sendGetObjectMessage(id, baseObject)
}

func (b BackendAdapter) PatchObject(id domain.ClusterKindName, checksum string, patch []byte) error {
	return b.sendPatchObjectMessage(id, checksum, patch)
}

func (b BackendAdapter) PutObject(id domain.ClusterKindName, object []byte) error {
	return b.sendPutObjectMessage(id, object)
}

func (b BackendAdapter) RegisterCallbacks(callbacks domain.Callbacks) {
	b.callbacks = callbacks
}

func (b BackendAdapter) Start() error {
	go b.consumePulsarMessages(b.ctx)
	return nil
}

func (b BackendAdapter) VerifyObject(id domain.ClusterKindName, checksum string) error {
	return b.sendVerifyObjectMessage(id, checksum)
}

func (b BackendAdapter) consumePulsarMessages(ctx context.Context) {
	k8sObjectsChannel := make(chan pulsar.ConsumerMessage)
	k8sObjectsConsumer, err := b.pulsarClient.NewConsumer(pulsarconnector.WithTopic(b.cfg.Backend.Topic),
		pulsarconnector.WithMessageChannel(k8sObjectsChannel),
		pulsarconnector.WithSubscriptionName(b.cfg.Backend.Subscription))
	if err != nil {
		log.Fatal(err) // FIXME is it fatal?
	}
	defer k8sObjectsConsumer.Close()
	synchronizerChannel := make(chan pulsar.ConsumerMessage)
	synchronizerConsumer, err := b.pulsarClient.NewConsumer(pulsarconnector.WithTopic(synchronizer.SynchronizerTopic),
		pulsarconnector.WithMessageChannel(synchronizerChannel),
		pulsarconnector.WithSubscriptionName(b.cfg.Backend.Subscription))
	if err != nil {
		log.Fatal(err) // FIXME is it fatal?
	}
	defer synchronizerConsumer.Close()
	for {
		select {
		case <-ctx.Done():
			close(k8sObjectsChannel)
			close(synchronizerChannel)
			return
		case msg := <-k8sObjectsChannel: // same as k8s_objects_ingester
			b.handleSingleK8sObjectMessage(msg)
		case msg := <-synchronizerChannel: // dedicated topic for synchronizer_ingester
			b.handleSingleSynchronizerMessage(msg)
		}
	}
}

// based on github.com/kubescape/event-ingester-service/ingesters/k8s_objects_ingester/k8s_objects.go:func handleSingleK8sObjectMessage
func (b BackendAdapter) handleSingleK8sObjectMessage(msg pulsar.ConsumerMessage) {
	msgID := utils.PulsarMessageIDtoString(msg.ID())
	msgTopic := msg.Topic()
	logger.L().Debug("Received message from pulsar",
		helpers.String("msgId", msgID),
		helpers.String("subscriptionName", b.cfg.Backend.Subscription),
		helpers.String("topicName", string(b.cfg.Backend.Topic)),
		helpers.String("msgTopic", msgTopic))
	props := msg.Properties()
	kindStr := props[identifiers.AttributeKind]
	kindLower := Kind(strings.ToLower(kindStr))
	if kindLower == "" {
		kindLower = kindClusterConnection
	}
	kindStr = string(kindLower)
	customerGUID := props[identifiers.AttributeCustomerGUID]
	clusterName := props["clusterName"]
	if clusterName == "" {
		clusterName = props[identifiers.AttributeCluster]
	}
	//itemKey := fmt.Sprintf("%s:%s/%s/%s/%s/%s/%s", msg.Topic(), customerGUID, msg.ID().String(), clusterName, props["objID"], kindStr, props["actionType"])
	isDelete := props["actionType"] == "delete"

	fmt.Println(customerGUID, isDelete)
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

func (b BackendAdapter) handleSingleSynchronizerMessage(msg pulsar.ConsumerMessage) {
	msgID := utils.PulsarMessageIDtoString(msg.ID())
	logger.L().Debug("Received message from pulsar",
		helpers.String("msgId", msgID),
		helpers.String("subscriptionName", b.cfg.Backend.Subscription),
		helpers.String("topicName", synchronizer.SynchronizerTopic))
}

// this message is sent to the k8s_objects topic
func (b BackendAdapter) sendDeleteObjectMessage(id domain.ClusterKindName) error {
	// TODO implement me
	return nil
}

// this message is sent to the synchronizer topic
func (b BackendAdapter) sendGetObjectMessage(id domain.ClusterKindName, object []byte) error {
	// TODO implement me
	return nil
}

// this message is sent to the synchronizer topic
func (b BackendAdapter) sendPatchObjectMessage(id domain.ClusterKindName, checksum string, patch []byte) error {
	// TODO implement me
	return nil
}

// this message is sent to the k8s_objects topic
func (b BackendAdapter) sendPutObjectMessage(id domain.ClusterKindName, object []byte) error {
	// TODO implement me
	return nil
}

// this message is sent to the synchronizer topic
func (b BackendAdapter) sendVerifyObjectMessage(id domain.ClusterKindName, checksum string) error {
	// TODO implement me
	return nil
}
