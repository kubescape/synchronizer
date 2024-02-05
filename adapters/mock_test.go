package adapters

import (
	"context"
	"testing"

	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
)

var (
	kindDeployment = domain.KindName{
		Kind:      domain.KindFromString(context.TODO(), "apps/v1/Deployment"),
		Name:      "name",
		Namespace: "namespace",
	}
	object1        = []byte(`{"kind":"kind","metadata":{"name":"name","resourceVersion":"1"}}`)
	object2        = []byte(`{"kind":"kind","metadata":{"name":"name","resourceVersion":"2"}}`)
	objectClientV2 = []byte(`{"kind":"kind","metadata":{"name":"client","resourceVersion":"2"}}`)
)

func TestCallDeleteObject(t *testing.T) {
	tests := []struct {
		name          string
		id            domain.KindName
		patchStrategy bool
		want          []domain.KindName
		wantErr       bool
	}{
		{
			name:          "delete object",
			id:            kindDeployment,
			patchStrategy: true,
			want:          []domain.KindName{kindDeployment},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			deletedIDs := []domain.KindName{}
			m := &MockAdapter{
				callbacks: domain.Callbacks{
					DeleteObject: func(ctx context.Context, id domain.KindName) error {
						deletedIDs = append(deletedIDs, id)
						return nil
					},
				},
				patchStrategy: tt.patchStrategy,
				Resources:     map[string][]byte{},
				shadowObjects: map[string][]byte{},
			}
			if err := m.TestCallDeleteObject(ctx, tt.id); (err != nil) != tt.wantErr {
				t.Errorf("TestCallDeleteObject() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.want, deletedIDs)
		})
	}
}

func TestCallPutOrPatch(t *testing.T) {
	tests := []struct {
		name          string
		id            domain.KindName
		baseObject    []byte
		newObject     []byte
		patchStrategy bool
		wantCheckSums []string
		wantPatches   [][]byte
		wantPatched   []domain.KindName
		wantPut       []domain.KindName
		wantErr       bool
	}{
		{
			name:          "no patch strategy",
			id:            kindDeployment,
			patchStrategy: false,
			wantCheckSums: []string{},
			wantPatches:   [][]byte{},
			wantPatched:   []domain.KindName{},
			wantPut:       []domain.KindName{kindDeployment},
		},
		{
			name:          "no base object",
			id:            kindDeployment,
			patchStrategy: true,
			wantCheckSums: []string{},
			wantPatches:   [][]byte{},
			wantPatched:   []domain.KindName{},
			wantPut:       []domain.KindName{kindDeployment},
		},
		{
			name:          "base object -> patch",
			id:            kindDeployment,
			baseObject:    object1,
			newObject:     object2,
			patchStrategy: true,
			wantCheckSums: []string{"a32626ea7056f8a30220a6a526e7968d2fc33a8ad0d9a1036ca357b56660239b"},
			wantPatches:   [][]byte{[]byte(`{"metadata":{"resourceVersion":"2"}}`)},
			wantPatched:   []domain.KindName{kindDeployment},
			wantPut:       []domain.KindName{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			checksums := []string{}
			patches := [][]byte{}
			patchedIDs := []domain.KindName{}
			putIDs := []domain.KindName{}
			m := &MockAdapter{
				callbacks: domain.Callbacks{
					PatchObject: func(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
						checksums = append(checksums, checksum)
						patches = append(patches, patch)
						patchedIDs = append(patchedIDs, id)
						return nil
					},
					PutObject: func(ctx context.Context, id domain.KindName, object []byte) error {
						putIDs = append(putIDs, id)
						return nil
					},
				},
				patchStrategy: tt.patchStrategy,
				Resources:     map[string][]byte{},
				shadowObjects: map[string][]byte{},
			}
			if err := m.TestCallPutOrPatch(ctx, tt.id, tt.baseObject, tt.newObject); (err != nil) != tt.wantErr {
				t.Errorf("TestCallPutOrPatch() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.wantCheckSums, checksums)
			assert.Equal(t, tt.wantPatches, patches)
			assert.Equal(t, tt.wantPatched, patchedIDs)
			assert.Equal(t, tt.wantPut, putIDs)
		})
	}
}

func TestCallVerifyObject(t *testing.T) {
	tests := []struct {
		name          string
		id            domain.KindName
		object        []byte
		wantCheckSums []string
		wantErr       bool
	}{
		{
			name:          "error calculating checksum",
			id:            kindDeployment,
			object:        []byte(`not a valid json`),
			wantCheckSums: []string{},
			wantErr:       true,
		},
		{
			name:          "send verify",
			id:            kindDeployment,
			object:        object1,
			wantCheckSums: []string{"b8e83bb8d6a990924cd950530b53a09485b18ff40b68e8245f648e77cbc3e9f1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			checksums := []string{}
			m := &MockAdapter{
				callbacks: domain.Callbacks{
					VerifyObject: func(ctx context.Context, id domain.KindName, checksum string) error {
						checksums = append(checksums, checksum)
						return nil
					},
				},
				patchStrategy: false,
				Resources:     map[string][]byte{},
				shadowObjects: map[string][]byte{},
			}
			if err := m.TestCallVerifyObject(ctx, tt.id, tt.object); (err != nil) != tt.wantErr {
				t.Errorf("TestCallVerifyObject() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.wantCheckSums, checksums)
		})
	}
}

func TestMockAdapter_DeleteObject(t *testing.T) {
	tests := []struct {
		name    string
		id      domain.KindName
		wantErr bool
	}{
		{
			name: "delete object",
			id:   kindDeployment,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMockAdapter(false)
			m.Resources[tt.id.String()] = []byte{}
			assert.Equal(t, len(m.Resources), 1)
			if err := m.DeleteObject(context.TODO(), tt.id); (err != nil) != tt.wantErr {
				t.Errorf("DeleteObject() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, len(m.Resources), 0)
		})
	}
}

func TestMockAdapter_GetObject(t *testing.T) {
	tests := []struct {
		name           string
		id             domain.KindName
		baseObject     []byte
		patchStrategy  bool
		resources      map[string][]byte
		wantCheckSums  []string
		wantPatches    [][]byte
		wantPatchedIDs []domain.KindName
		wantPutIDs     []domain.KindName
		wantPutObjects [][]byte
		wantErr        bool
	}{
		{
			name:           "unknown object",
			id:             kindDeployment,
			patchStrategy:  false,
			resources:      map[string][]byte{},
			wantCheckSums:  []string{},
			wantPatches:    [][]byte{},
			wantPatchedIDs: []domain.KindName{},
			wantPutIDs:     []domain.KindName{},
			wantPutObjects: [][]byte{},
			wantErr:        true,
		},
		{
			name:           "no patch strategy",
			id:             kindDeployment,
			patchStrategy:  false,
			resources:      map[string][]byte{kindDeployment.String(): object1},
			wantCheckSums:  []string{},
			wantPatches:    [][]byte{},
			wantPatchedIDs: []domain.KindName{},
			wantPutIDs:     []domain.KindName{kindDeployment},
			wantPutObjects: [][]byte{object1},
		},
		{
			name:           "no base object",
			id:             kindDeployment,
			patchStrategy:  true,
			resources:      map[string][]byte{kindDeployment.String(): object1},
			wantCheckSums:  []string{},
			wantPatches:    [][]byte{},
			wantPatchedIDs: []domain.KindName{},
			wantPutIDs:     []domain.KindName{kindDeployment},
			wantPutObjects: [][]byte{object1},
		},
		{
			name:           "base object -> patch",
			id:             kindDeployment,
			baseObject:     object1,
			resources:      map[string][]byte{kindDeployment.String(): object2},
			patchStrategy:  true,
			wantCheckSums:  []string{"a32626ea7056f8a30220a6a526e7968d2fc33a8ad0d9a1036ca357b56660239b"},
			wantPatches:    [][]byte{[]byte(`{"metadata":{"resourceVersion":"2"}}`)},
			wantPatchedIDs: []domain.KindName{kindDeployment},
			wantPutIDs:     []domain.KindName{},
			wantPutObjects: [][]byte{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			checksums := []string{}
			patches := [][]byte{}
			patchedIDs := []domain.KindName{}
			putIDs := []domain.KindName{}
			putObjects := [][]byte{}
			m := &MockAdapter{
				callbacks: domain.Callbacks{
					PatchObject: func(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
						checksums = append(checksums, checksum)
						patches = append(patches, patch)
						patchedIDs = append(patchedIDs, id)
						return nil
					},
					PutObject: func(ctx context.Context, id domain.KindName, object []byte) error {
						putIDs = append(putIDs, id)
						putObjects = append(putObjects, object)
						return nil
					},
				},
				patchStrategy: tt.patchStrategy,
				Resources:     tt.resources,
				shadowObjects: map[string][]byte{},
			}
			if err := m.GetObject(ctx, tt.id, tt.baseObject); (err != nil) != tt.wantErr {
				t.Errorf("GetObject() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.wantCheckSums, checksums)
			assert.Equal(t, tt.wantPatches, patches)
			assert.Equal(t, tt.wantPatchedIDs, patchedIDs)
			assert.Equal(t, tt.wantPutIDs, putIDs)
			assert.Equal(t, tt.wantPutObjects, putObjects)
		})
	}
}

func TestMockAdapter_PatchObject(t *testing.T) {
	tests := []struct {
		name            string
		id              domain.KindName
		newChecksum     string
		patch           []byte
		resources       map[string][]byte
		wantResources   map[string][]byte
		wantGotIDs      []domain.KindName
		wantBaseObjects [][]byte
		wantErr         bool
	}{
		{
			name:            "unknown object",
			id:              kindDeployment,
			newChecksum:     "a32626ea7056f8a30220a6a526e7968d2fc33a8ad0d9a1036ca357b56660239b",
			resources:       map[string][]byte{},
			wantResources:   map[string][]byte{},
			wantGotIDs:      []domain.KindName{kindDeployment},
			wantBaseObjects: [][]byte{[]uint8(nil)},
		},
		{
			name:            "error applying patch",
			id:              kindDeployment,
			newChecksum:     "a32626ea7056f8a30220a6a526e7968d2fc33a8ad0d9a1036ca357b56660239b",
			patch:           []byte(`invalid patch`),
			resources:       map[string][]byte{kindDeployment.String(): object1},
			wantResources:   map[string][]byte{kindDeployment.String(): object1},
			wantGotIDs:      []domain.KindName{kindDeployment},
			wantBaseObjects: [][]byte{object1},
		},
		{
			name:            "checksum mismatch",
			id:              kindDeployment,
			newChecksum:     "not the right checksum",
			patch:           []byte(`{"metadata":{"resourceVersion":"2"}}`),
			resources:       map[string][]byte{kindDeployment.String(): object1},
			wantResources:   map[string][]byte{kindDeployment.String(): object1},
			wantGotIDs:      []domain.KindName{kindDeployment},
			wantBaseObjects: [][]byte{object1},
		},
		{
			name:            "not newer object",
			id:              kindDeployment,
			newChecksum:     "b8e83bb8d6a990924cd950530b53a09485b18ff40b68e8245f648e77cbc3e9f1",
			patch:           []byte(`{"metadata":{"resourceVersion":"1"}}`),
			resources:       map[string][]byte{kindDeployment.String(): object2},
			wantResources:   map[string][]byte{kindDeployment.String(): object2},
			wantGotIDs:      []domain.KindName{},
			wantBaseObjects: [][]byte{},
		},
		{
			name:            "all good",
			id:              kindDeployment,
			newChecksum:     "a32626ea7056f8a30220a6a526e7968d2fc33a8ad0d9a1036ca357b56660239b",
			patch:           []byte(`{"metadata":{"resourceVersion":"2"}}`),
			resources:       map[string][]byte{kindDeployment.String(): object1},
			wantResources:   map[string][]byte{kindDeployment.String(): object2},
			wantGotIDs:      []domain.KindName{},
			wantBaseObjects: [][]byte{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			gotIDs := []domain.KindName{}
			baseObjects := [][]byte{}
			m := &MockAdapter{
				callbacks: domain.Callbacks{
					GetObject: func(ctx context.Context, id domain.KindName, baseObject []byte) error {
						gotIDs = append(gotIDs, id)
						baseObjects = append(baseObjects, baseObject)
						return nil
					},
				},
				checkResourceVersion: true,
				patchStrategy:        true,
				Resources:            tt.resources,
				shadowObjects:        map[string][]byte{},
			}
			if err := m.PatchObject(ctx, tt.id, tt.newChecksum, tt.patch); (err != nil) != tt.wantErr {
				t.Errorf("PatchObject() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.wantResources, m.Resources)
			assert.Equal(t, tt.wantGotIDs, gotIDs)
			assert.Equal(t, tt.wantBaseObjects, baseObjects)
		})
	}
}

func TestMockAdapter_PutObject(t *testing.T) {
	tests := []struct {
		name          string
		id            domain.KindName
		object        []byte
		oldObject     []byte
		wantResources map[string][]byte
		wantErr       bool
	}{
		{
			name:          "newer object",
			id:            kindDeployment,
			object:        object2,
			oldObject:     object1,
			wantResources: map[string][]byte{kindDeployment.String(): object2},
		},
		{
			name:          "same resource version",
			id:            kindDeployment,
			object:        objectClientV2,
			oldObject:     object2,
			wantResources: map[string][]byte{kindDeployment.String(): object2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			m := NewMockAdapter(false)
			m.Resources[tt.id.String()] = tt.oldObject
			if err := m.PutObject(ctx, tt.id, tt.object); (err != nil) != tt.wantErr {
				t.Errorf("PutObject() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.wantResources, m.Resources)
		})
	}
}

func TestMockAdapter_VerifyObject(t *testing.T) {
	tests := []struct {
		name            string
		id              domain.KindName
		newChecksum     string
		resources       map[string][]byte
		wantGotIDs      []domain.KindName
		wantBaseObjects [][]byte
		wantErr         bool
	}{
		{
			name:            "unknown object",
			id:              kindDeployment,
			newChecksum:     "a32626ea7056f8a30220a6a526e7968d2fc33a8ad0d9a1036ca357b56660239b",
			resources:       map[string][]byte{},
			wantGotIDs:      []domain.KindName{kindDeployment},
			wantBaseObjects: [][]byte{[]uint8(nil)},
		},
		{
			name:            "error calculating checksum",
			id:              kindDeployment,
			newChecksum:     "a32626ea7056f8a30220a6a526e7968d2fc33a8ad0d9a1036ca357b56660239b",
			resources:       map[string][]byte{kindDeployment.String(): []byte(`not a valid json`)},
			wantGotIDs:      []domain.KindName{kindDeployment},
			wantBaseObjects: [][]byte{[]uint8(nil)},
		},
		{
			name:            "checksum mismatch",
			id:              kindDeployment,
			newChecksum:     "not a valid checksum",
			resources:       map[string][]byte{kindDeployment.String(): object1},
			wantGotIDs:      []domain.KindName{kindDeployment},
			wantBaseObjects: [][]byte{object1},
		},
		{
			name:            "checksum match",
			id:              kindDeployment,
			newChecksum:     "b8e83bb8d6a990924cd950530b53a09485b18ff40b68e8245f648e77cbc3e9f1",
			resources:       map[string][]byte{kindDeployment.String(): object1},
			wantGotIDs:      []domain.KindName{},
			wantBaseObjects: [][]byte{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			gotIDs := []domain.KindName{}
			baseObjects := [][]byte{}
			m := &MockAdapter{
				callbacks: domain.Callbacks{
					GetObject: func(ctx context.Context, id domain.KindName, baseObject []byte) error {
						gotIDs = append(gotIDs, id)
						baseObjects = append(baseObjects, baseObject)
						return nil
					},
				},
				patchStrategy: false,
				Resources:     tt.resources,
				shadowObjects: map[string][]byte{},
			}
			if err := m.VerifyObject(ctx, tt.id, tt.newChecksum); (err != nil) != tt.wantErr {
				t.Errorf("VerifyObject() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.wantGotIDs, gotIDs)
			assert.Equal(t, tt.wantBaseObjects, baseObjects)
		})
	}
}

func TestMockAdapter_Batch(t *testing.T) {
	const (
		put    = "put"
		get    = "get"
		patch  = "patch"
		delete = "delete"
		verify = "verify"
	)

	tests := []struct {
		name           string
		items          domain.BatchItems
		newChecksum    string
		resources      map[string][]byte
		patchStrategy  bool
		wantIDs        []domain.KindName
		wantOperations []string
		wantResources  map[string][]byte
		wantErr        bool
	}{
		{
			name:      "batch - get",
			resources: map[string][]byte{kindDeployment.String(): object1},
			items: domain.BatchItems{
				GetObject: []domain.GetObject{
					{
						Name:       kindDeployment.Name,
						Namespace:  kindDeployment.Namespace,
						BaseObject: string(object1),
					},
				},
			},
			wantIDs:        []domain.KindName{kindDeployment},
			wantOperations: []string{put},
			wantResources:  map[string][]byte{kindDeployment.String(): object1},
		},
		{
			name:      "batch - put",
			resources: map[string][]byte{},
			items: domain.BatchItems{
				PutObject: []domain.PutObject{
					{
						Name:      kindDeployment.Name,
						Namespace: kindDeployment.Namespace,
						Object:    string(object1),
					},
				},
			},
			wantIDs:        []domain.KindName{},
			wantOperations: []string{},
			wantResources:  map[string][]byte{kindDeployment.String(): object1},
		},
		{
			name:      "batch - delete",
			resources: map[string][]byte{kindDeployment.String(): object1},
			items: domain.BatchItems{
				ObjectDeleted: []domain.ObjectDeleted{
					{
						Name:      kindDeployment.Name,
						Namespace: kindDeployment.Namespace,
					},
				},
			},
			wantIDs:        []domain.KindName{},
			wantOperations: []string{},
			wantResources:  map[string][]byte{},
		},
		{
			name:          "batch - patch existing",
			patchStrategy: true,
			resources:     map[string][]byte{kindDeployment.String(): object1},
			items: domain.BatchItems{
				PatchObject: []domain.PatchObject{
					{
						Name:      kindDeployment.Name,
						Namespace: kindDeployment.Namespace,
						Checksum:  "a32626ea7056f8a30220a6a526e7968d2fc33a8ad0d9a1036ca357b56660239b",
						Patch:     string(object2),
					},
				},
			},
			wantIDs:        []domain.KindName{},
			wantOperations: []string{},
			wantResources:  map[string][]byte{kindDeployment.String(): object2},
		},
		{
			name:          "batch - patch missing",
			patchStrategy: true,
			resources:     map[string][]byte{},
			items: domain.BatchItems{
				PatchObject: []domain.PatchObject{
					{
						Name:      kindDeployment.Name,
						Namespace: kindDeployment.Namespace,
						Checksum:  "a32626ea7056f8a30220a6a526e7968d2fc33a8ad0d9a1036ca357b56660239b",
						Patch:     string(object2),
					},
				},
			},
			wantIDs:        []domain.KindName{kindDeployment},
			wantOperations: []string{get},
			wantResources:  map[string][]byte{},
		},
		{
			name:          "batch - new checksum (missing) -> get",
			patchStrategy: true,
			resources:     map[string][]byte{},
			items: domain.BatchItems{
				NewChecksum: []domain.NewChecksum{
					{
						Name:      kindDeployment.Name,
						Namespace: kindDeployment.Namespace,
						Checksum:  "a32626ea7056f8a30220a6a526e7968d2fc33a8ad0d9a1036ca357b56660239b",
					},
				},
			},
			wantIDs:        []domain.KindName{kindDeployment},
			wantOperations: []string{get},
			wantResources:  map[string][]byte{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()

			checksums := []string{}
			gotIDs := []domain.KindName{}
			operations := []string{}

			m := &MockAdapter{
				callbacks: domain.Callbacks{
					VerifyObject: func(ctx context.Context, id domain.KindName, checksum string) error {
						checksums = append(checksums, checksum)
						operations = append(operations, verify)
						gotIDs = append(gotIDs, id)
						return nil
					},
					DeleteObject: func(ctx context.Context, id domain.KindName) error {
						operations = append(operations, delete)
						gotIDs = append(gotIDs, id)
						return nil
					},
					PatchObject: func(ctx context.Context, id domain.KindName, checksum string, patchObj []byte) error {
						checksums = append(checksums, checksum)
						operations = append(operations, patch)
						gotIDs = append(gotIDs, id)
						return nil
					},
					PutObject: func(ctx context.Context, id domain.KindName, object []byte) error {
						operations = append(operations, put)
						gotIDs = append(gotIDs, id)
						return nil
					},

					GetObject: func(ctx context.Context, id domain.KindName, baseObject []byte) error {
						operations = append(operations, get)
						gotIDs = append(gotIDs, id)
						return nil
					},
					Batch: func(ctx context.Context, kind domain.Kind, batchType domain.BatchType, items domain.BatchItems) error {
						panic("should not be called")
					},
				},
				patchStrategy: tt.patchStrategy,
				Resources:     tt.resources,
				shadowObjects: map[string][]byte{},
			}
			err := m.Batch(ctx, *domain.KindFromString(context.TODO(), "apps/v1/Deployment"), domain.BatchType(tt.batchType), tt.items)
			if (err != nil) != tt.wantErr {
				t.Errorf("Batch() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err != nil {
				return
			}

			assert.Equal(t, tt.wantOperations, operations)
			assert.Equal(t, tt.wantIDs, gotIDs)
			assert.Equal(t, tt.wantResources, m.Resources)
		})
	}
}
