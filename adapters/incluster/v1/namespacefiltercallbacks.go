package incluster

import (
	"context"

	"github.com/kubescape/synchronizer/domain"
)

// Live filtering supplements existing static checks, preserving legacy behavior
// when disabled. Operator-namespace and cluster-scoped resources stay exempt.
func (c *Client) liveNamespaceExcluded(namespace string) bool {
	return c.namespaceFilters != nil && namespace != "" && namespace != c.operatorNamespace && c.namespaceFilters.skip(namespace)
}

// Check immediately before dispatch, including fallback requests carrying base
// objects. An update does not recall a callback that has already been dispatched.
func (c *Client) sendDeleteObject(ctx context.Context, id domain.KindName) error {
	if c.liveNamespaceExcluded(id.Namespace) {
		return nil
	}
	return c.callbacks.DeleteObject(ctx, id)
}

func (c *Client) sendGetObject(ctx context.Context, id domain.KindName, baseObject []byte) error {
	if c.liveNamespaceExcluded(id.Namespace) {
		return nil
	}
	return c.callbacks.GetObject(ctx, id, baseObject)
}

// The caller must not advance a patch baseline when dispatch was suppressed.
func (c *Client) dispatchPatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) (bool, error) {
	if c.liveNamespaceExcluded(id.Namespace) {
		return false, nil
	}
	return true, c.callbacks.PatchObject(ctx, id, checksum, patch)
}

func (c *Client) sendPutObject(ctx context.Context, id domain.KindName, checksum string, object []byte) error {
	_, err := c.dispatchPutObject(ctx, id, checksum, object)
	return err
}

// The caller must not advance a patch baseline when dispatch was suppressed.
func (c *Client) dispatchPutObject(ctx context.Context, id domain.KindName, checksum string, object []byte) (bool, error) {
	if c.liveNamespaceExcluded(id.Namespace) {
		return false, nil
	}
	return true, c.callbacks.PutObject(ctx, id, checksum, object)
}

func (c *Client) sendVerifyObject(ctx context.Context, id domain.KindName, checksum string) error {
	if c.liveNamespaceExcluded(id.Namespace) {
		return nil
	}
	return c.callbacks.VerifyObject(ctx, id, checksum)
}
