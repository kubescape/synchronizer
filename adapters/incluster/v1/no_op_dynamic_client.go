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
var _ dynamic.Interface = &NoOpDynamicClient{}

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

func (n *NoOpDynamicResource) Namespace(_ string) dynamic.ResourceInterface {
	return n
}

func (n *NoOpDynamicResource) Create(_ context.Context, obj *unstructured.Unstructured, _ metav1.CreateOptions, _ ...string) (*unstructured.Unstructured, error) {
	return obj, nil
}
func (n *NoOpDynamicResource) Update(_ context.Context, obj *unstructured.Unstructured, _ metav1.UpdateOptions, _ ...string) (*unstructured.Unstructured, error) {
	return obj, nil
}
func (n *NoOpDynamicResource) UpdateStatus(_ context.Context, obj *unstructured.Unstructured, _ metav1.UpdateOptions) (*unstructured.Unstructured, error) {
	return obj, nil
}
func (n *NoOpDynamicResource) Delete(_ context.Context, _ string, _ metav1.DeleteOptions, _ ...string) error {
	return nil
}
func (n *NoOpDynamicResource) DeleteCollection(_ context.Context, _ metav1.DeleteOptions, _ metav1.ListOptions) error {
	return nil
}
func (n *NoOpDynamicResource) Get(_ context.Context, _ string, _ metav1.GetOptions, _ ...string) (*unstructured.Unstructured, error) {
	return &unstructured.Unstructured{Object: map[string]interface{}{}}, nil
}
func (n *NoOpDynamicResource) List(_ context.Context, _ metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	return &unstructured.UnstructuredList{Object: map[string]interface{}{}, Items: []unstructured.Unstructured{{Object: map[string]interface{}{}}}}, nil
}
func (n *NoOpDynamicResource) Watch(_ context.Context, _ metav1.ListOptions) (watch.Interface, error) {
	return &NoOpWatch{eventChan: make(chan watch.Event)}, nil
}
func (n *NoOpDynamicResource) Patch(_ context.Context, _ string, _ types.PatchType, _ []byte, _ metav1.PatchOptions, _ ...string) (*unstructured.Unstructured, error) {
	return &unstructured.Unstructured{Object: map[string]interface{}{}}, nil
}
func (n *NoOpDynamicResource) Apply(_ context.Context, _ string, obj *unstructured.Unstructured, _ metav1.ApplyOptions, _ ...string) (*unstructured.Unstructured, error) {
	return obj, nil
}
func (n *NoOpDynamicResource) ApplyStatus(_ context.Context, _ string, obj *unstructured.Unstructured, _ metav1.ApplyOptions) (*unstructured.Unstructured, error) {
	return obj, nil
}

func (n *NoOpWatch) Stop() {
	close(n.eventChan)
}
func (n *NoOpWatch) ResultChan() <-chan watch.Event {
	return n.eventChan
}
