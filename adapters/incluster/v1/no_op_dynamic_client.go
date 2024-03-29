package incluster

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

// implement k8s.io/client-go/dynamic.Interface to be able to use it in the adapter

type NoOpDynamicClient struct {
}

type NoOpDynamicResource struct {
	gvr schema.GroupVersionResource
}

// watch.Interface
type NoOpWatch struct {
	eventChan chan watch.Event
}

func (n *NoOpDynamicClient) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return &NoOpDynamicResource{gvr: gvr}
}

func (n *NoOpDynamicResource) Namespace(namespace string) dynamic.ResourceInterface {
	return n
}

func (n *NoOpDynamicResource) Create(ctx context.Context, obj *unstructured.Unstructured, options metav1.CreateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return obj, nil
}
func (n *NoOpDynamicResource) Update(ctx context.Context, obj *unstructured.Unstructured, options metav1.UpdateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return obj, nil
}
func (n *NoOpDynamicResource) UpdateStatus(ctx context.Context, obj *unstructured.Unstructured, options metav1.UpdateOptions) (*unstructured.Unstructured, error) {
	return obj, nil
}
func (n *NoOpDynamicResource) Delete(ctx context.Context, name string, options metav1.DeleteOptions, subresources ...string) error {
	return nil
}
func (n *NoOpDynamicResource) DeleteCollection(ctx context.Context, options metav1.DeleteOptions, listOptions metav1.ListOptions) error {
	return nil
}
func (n *NoOpDynamicResource) Get(ctx context.Context, name string, options metav1.GetOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return &unstructured.Unstructured{Object: map[string]interface{}{}}, nil
}
func (n *NoOpDynamicResource) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	return &unstructured.UnstructuredList{Object: map[string]interface{}{}, Items: []unstructured.Unstructured{{Object: map[string]interface{}{}}}}, nil
}
func (n *NoOpDynamicResource) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return &NoOpWatch{eventChan: make(chan watch.Event)}, nil
}
func (n *NoOpDynamicResource) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, options metav1.PatchOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return &unstructured.Unstructured{Object: map[string]interface{}{}}, nil
}
func (n *NoOpDynamicResource) Apply(ctx context.Context, name string, obj *unstructured.Unstructured, options metav1.ApplyOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return obj, nil
}
func (n *NoOpDynamicResource) ApplyStatus(ctx context.Context, name string, obj *unstructured.Unstructured, options metav1.ApplyOptions) (*unstructured.Unstructured, error) {
	return obj, nil
}

func (n *NoOpWatch) Stop() {
	close(n.eventChan)
}
func (n *NoOpWatch) ResultChan() <-chan watch.Event {
	return n.eventChan
}
