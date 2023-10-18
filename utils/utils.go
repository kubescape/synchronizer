package utils

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/SergJa/jsonhash"
	"github.com/davecgh/go-spew/spew"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/assert"
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

func KeyToNsName(key string) (string, string) {
	split := strings.Split(key, "/")
	return split[0], split[1]
}

func NsNameToKey(ns, name string) string {
	return strings.Join([]string{ns, name}, "/")
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

func NewClient() (dynamic.Interface, error) {
	clusterConfig, err := getConfig()
	if err != nil {
		return nil, err
	}
	dynClient, err := dynamic.NewForConfig(clusterConfig)
	if err != nil {
		return nil, err
	}
	return dynClient, nil
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
