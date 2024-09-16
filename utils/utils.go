package utils

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"github.com/SergJa/jsonhash"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cenkalti/backoff/v4"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	spdxv1beta1 "github.com/kubescape/storage/pkg/generated/clientset/versioned/typed/softwarecomposition/v1beta1"
	"github.com/kubescape/synchronizer/domain"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/assert"
	"go.uber.org/multierr"
	"golang.org/x/mod/semver"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func CanonicalHash(in []byte) (string, error) {
	hash, err := jsonhash.CalculateJsonHash(in, []string{
		".status.conditions", // avoid Pod.status.conditions.lastProbeTime: null
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hash[:]), nil
}

func NewMsgId() string {
	return uuid.NewString()
}

func ContextFromGeneric(parent context.Context, generic domain.Generic) context.Context {
	ctx := context.WithValue(parent, domain.ContextKeyDepth, generic.Depth)
	ctx = context.WithValue(ctx, domain.ContextKeyMsgId, generic.MsgId)
	return ctx
}

func ContextFromIdentifiers(parent context.Context, id domain.ClientIdentifier) context.Context {
	return context.WithValue(parent, domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: id.Account,
		Cluster: id.Cluster,
	})
}

func ClientIdentifierFromContext(ctx context.Context) domain.ClientIdentifier {
	return ctx.Value(domain.ContextKeyClientIdentifier).(domain.ClientIdentifier)
}

func DeptMsgIdFromContext(ctx context.Context) (int, string) {
	depth := ctx.Value(domain.ContextKeyDepth)
	if depth == nil {
		depth = 0
	}
	msgId := ctx.Value(domain.ContextKeyMsgId)
	if msgId == nil {
		msgId = NewMsgId()
	}
	return depth.(int), msgId.(string)
}

//goland:noinspection GoUnusedExportedFunction
func CompareJson(a, b []byte) bool {
	var aData interface{}
	var bData interface{}
	err := json.Unmarshal(a, &aData)
	if err != nil {
		logger.L().Error("cannot unmarshal a", helpers.Error(err))
		return false
	}
	err = json.Unmarshal(b, &bData)
	if err != nil {
		logger.L().Error("cannot unmarshal b", helpers.Error(err))
		return false
	}
	equal := assert.ObjectsAreEqual(aData, bData)
	if !equal {
		fmt.Println(diff(aData, bData))
	}
	return equal
}

func diff(expected interface{}, actual interface{}) string {
	if expected == nil || actual == nil {
		return ""
	}

	et, ek := typeAndKind(expected)
	at, _ := typeAndKind(actual)

	if et != at {
		return ""
	}

	if ek != reflect.Struct && ek != reflect.Map && ek != reflect.Slice && ek != reflect.Array && ek != reflect.String {
		return ""
	}

	var e, a string

	switch et {
	case reflect.TypeOf(""):
		e = reflect.ValueOf(expected).String()
		a = reflect.ValueOf(actual).String()
	case reflect.TypeOf(time.Time{}):
		e = spewConfigStringerEnabled.Sdump(expected)
		a = spewConfigStringerEnabled.Sdump(actual)
	default:
		e = spewConfig.Sdump(expected)
		a = spewConfig.Sdump(actual)
	}

	diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(e),
		B:        difflib.SplitLines(a),
		FromFile: "Expected",
		FromDate: "",
		ToFile:   "Actual",
		ToDate:   "",
		Context:  1,
	})

	return "\n\nDiff:\n" + diff
}

var spewConfig = spew.ConfigState{
	Indent:                  " ",
	DisablePointerAddresses: true,
	DisableCapacities:       true,
	SortKeys:                true,
	DisableMethods:          true,
	MaxDepth:                10,
}

var spewConfigStringerEnabled = spew.ConfigState{
	Indent:                  " ",
	DisablePointerAddresses: true,
	DisableCapacities:       true,
	SortKeys:                true,
	MaxDepth:                10,
}

func typeAndKind(v interface{}) (reflect.Type, reflect.Kind) {
	t := reflect.TypeOf(v)
	k := t.Kind()

	if k == reflect.Ptr {
		t = t.Elem()
		k = t.Kind()
	}
	return t, k
}

func NewClient() (dynamic.Interface, spdxv1beta1.SpdxV1beta1Interface, error) {
	clusterConfig, err := getConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get cluster config: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(clusterConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}
	storageClient, err := spdxv1beta1.NewForConfig(clusterConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create storage client: %w", err)
	}
	return dynamicClient, storageClient, nil
}

func getConfig() (*rest.Config, error) {
	// try in-cluster config first
	clusterConfig, err := rest.InClusterConfig()
	if err == nil {
		return clusterConfig, nil
	}
	// fallback to kubeconfig
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()
	clusterConfig, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err == nil {
		return clusterConfig, nil
	}
	// nothing works
	return nil, errors.New("unable to find config")
}

// PulsarMessageIDtoString is taken from github.com/kubescape/event-ingester-service/utils/common.go:func PulsarMessageIDtoString
func PulsarMessageIDtoString(msgID pulsar.MessageID) string {
	batchStr := strconv.Itoa(int(msgID.BatchIdx()))
	msgIDstr := msgID.String() + ":" + batchStr
	return msgIDstr
}

func ServePprof() {
	if logger.L().GetLevel() == helpers.DebugLevel.String() {
		logger.L().Info("starting pprof server", helpers.String("port", "6060"))
		pprofMux := http.NewServeMux()
		pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
		pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		pprofMux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		pprofMux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		pprofMux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
		pprofMux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		pprofMux.Handle("/debug/pprof/block", pprof.Handler("block"))
		pprofMux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
		go func() {
			if err := http.ListenAndServe(":6060", pprofMux); err != nil {
				logger.L().Error("failed to start pprof server", helpers.Error(err))
			}
		}()
	}
}

func StartLivenessProbe() {
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	go func() {
		if err := http.ListenAndServe(":7888", nil); err != nil {
			logger.L().Error("failed to start liveness probe", helpers.Error(err))
		}
	}()
}

func StringValueBigger(s1, s2 string) bool {
	i1, err := strconv.Atoi(s1)
	if err != nil {
		return false
	}
	i2, err := strconv.Atoi(s2)
	if err != nil {
		return false
	}
	return i1 > i2
}

func RemoveManagedFields(d metav1.Object) {
	// Remove managed fields
	d.SetManagedFields(nil)
	// Remove last-applied-configuration annotation
	ann := d.GetAnnotations()
	delete(ann, "kubectl.kubernetes.io/last-applied-configuration")
	d.SetAnnotations(ann)
}

func RemoveSpecificFields(d *unstructured.Unstructured, fields [][]string) error {
	var errs error
	for _, f := range fields {
		err := unstructured.SetNestedField(d.Object, nil, f...)
		if err != nil {
			errs = multierr.Append(errs, fmt.Errorf("failed to remove field %s: %w", f, err))
		}
	}
	return errs
}

func NewBackOff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	// never stop retrying (unless PermanentError is returned)
	b.MaxElapsedTime = 0
	return b
}

// GreaterOrEqualVersion returns true if a version is greater or equal to b
func GreaterOrEqualVersion(a string, b string) bool {
	return semver.Compare(a, b) >= 0
}

func IsBatchMessageSupported(version string) bool {
	const minimumSupportedVersion = "v0.0.57"

	if version == "" {
		return false
	}

	return GreaterOrEqualVersion(version, minimumSupportedVersion)
}

func FileToUnstructured(path string) *unstructured.Unstructured {
	b, _ := os.ReadFile(path)
	u := &unstructured.Unstructured{}
	_ = u.UnmarshalJSON(b)
	return u
}

func FileContent(path string) []byte {
	b, _ := os.ReadFile(path)
	return b
}
