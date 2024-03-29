package httpendpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type Adapter struct {
	callbacks      domain.Callbacks
	cfg            config.Config
	clients        map[string]adapters.Client
	httpMux        *http.ServeMux
	httpServer     *http.Server
	supportedPaths map[domain.Strategy]map[string]map[string]map[string]bool
	isStarted      bool
}

func NewHTTPEndpointAdapter(cfg config.Config) *Adapter {
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.HTTPEndpoint.ServerPort),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
		Handler:      httpMux,
	}
	a := &Adapter{
		cfg:        cfg,
		clients:    map[string]adapters.Client{},
		httpMux:    httpMux,
		httpServer: server,
	}
	httpMux.Handle("/", a)
	return a
}

// ensure that the Adapter struct satisfies the adapters.Adapter interface at compile-time
var _ adapters.Adapter = (*Adapter)(nil)

func (a *Adapter) GetConfig() config.Config {
	return a.cfg
}

// No-OP functions for functions needed only for backend re-sync
func (a *Adapter) DeleteObject(ctx context.Context, id domain.KindName) error {
	return nil
}

func (a *Adapter) GetObject(ctx context.Context, id domain.KindName, baseObject []byte) error {
	return nil
}

func (a *Adapter) PatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	return nil
}

func (a *Adapter) PutObject(ctx context.Context, id domain.KindName, object []byte) error {
	return nil
}

func (a *Adapter) VerifyObject(ctx context.Context, id domain.KindName, checksum string) error {
	return nil
}

func (a *Adapter) Batch(ctx context.Context, kind domain.Kind, batchType domain.BatchType, items domain.BatchItems) error {
	return nil
}

func (a *Adapter) RegisterCallbacks(mainCtx context.Context, callbacks domain.Callbacks) {
	a.httpServer.BaseContext = func(_ net.Listener) context.Context {
		return mainCtx
	}
	a.callbacks = callbacks
}

func (a *Adapter) Callbacks(_ context.Context) (domain.Callbacks, error) {
	return a.callbacks, nil
}

func (a *Adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.URL.Path == "/readyz" {
		if a.isStarted {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		return
	}
	logger.L().Ctx(r.Context()).Info("httpendpoint request", helpers.String("path", r.URL.Path), helpers.String("method", r.Method))
	// TODO: add tracing span
	// validate the request verb + path
	if a.supportedPaths == nil {
		w.WriteHeader(http.StatusInternalServerError)
		logger.L().Ctx(r.Context()).Warning("httpendpoint supportedPaths is nil")
		return
	}
	// validate the request verb + path
	// URL path should be in the format of /apis/v1/<group>/<version>/<resource-kind>
	// validate the request path
	lowerCasePath := strings.ToLower(r.URL.Path)
	pathSlices := strings.Split(lowerCasePath, "/")
	if len(pathSlices) != 6 {
		w.WriteHeader(http.StatusBadRequest)
		logger.L().Ctx(r.Context()).Warning("httpendpoint request path is invalid", helpers.String("path", r.URL.Path))
		return
	}
	if pathSlices[0] != "" || pathSlices[1] != "apis" || pathSlices[2] != "v1" || pathSlices[3] == "" || pathSlices[4] == "" || pathSlices[5] == "" {
		w.WriteHeader(http.StatusBadRequest)
		logger.L().Ctx(r.Context()).Warning("httpendpoint error #2. Request path is invalid", helpers.String("path", r.URL.Path))
		return
	}
	pathSlices = pathSlices[3:]
	// validate the request path against the supported paths
	strategy := domain.Strategy(strings.ToLower(r.Method))
	switch r.Method {
	case http.MethodPut:
		strategy = domain.PatchStrategy
	case http.MethodPost:
		strategy = domain.CopyStrategy
	}
	if _, ok := a.supportedPaths[strategy]; !ok {
		w.WriteHeader(http.StatusMethodNotAllowed)
		logger.L().Ctx(r.Context()).Warning("httpendpoint request method is not supported", helpers.String("method", r.Method))
		return
	}
	if _, ok := a.supportedPaths[strategy][pathSlices[0]]; !ok {
		w.WriteHeader(http.StatusNotFound)
		logger.L().Ctx(r.Context()).Warning("httpendpoint request group path is not supported", helpers.String("path", r.URL.Path))
		return
	}
	if _, ok := a.supportedPaths[strategy][pathSlices[0]][pathSlices[1]]; !ok {
		w.WriteHeader(http.StatusNotFound)
		logger.L().Ctx(r.Context()).Warning("httpendpoint request version path is not supported", helpers.String("path", r.URL.Path))
		return
	}
	if _, ok := a.supportedPaths[strategy][pathSlices[0]][pathSlices[1]][pathSlices[2]]; !ok {
		w.WriteHeader(http.StatusNotFound)
		logger.L().Ctx(r.Context()).Warning("httpendpoint request resource path is not supported", helpers.String("path", r.URL.Path))
		return
	}

	// read the request body
	if r.Body == nil {
		w.WriteHeader(http.StatusBadRequest)
		logger.L().Ctx(r.Context()).Warning("httpendpoint request body is empty")
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logger.L().Ctx(r.Context()).Warning("httpendpoint request body read error", helpers.Error(err))
		return
	}
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal(bodyBytes, obj); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logger.L().Ctx(r.Context()).Warning("httpendpoint request body read error", helpers.Error(err))
		return
	}
	kindName := domain.FromUnstructured(obj)
	// validate the request body against the URL path
	// URL path should be in the format of /apis/v1/<group>/<version>/<resource-kind>
	if kindName.Kind.Group != pathSlices[0] || kindName.Kind.Version != pathSlices[1] || kindName.Kind.Resource != pathSlices[2] {
		w.WriteHeader(http.StatusBadRequest)
		logger.L().Ctx(r.Context()).Warning("httpendpoint request body does not match the URL path", helpers.String("path", r.URL.Path), helpers.Interface("kindName", kindName))
		return
	}

	// call the PutObject callback
	switch strategy {
	case domain.PatchStrategy:
		if err := a.callbacks.PatchObject(r.Context(), kindName, "", bodyBytes); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			logger.L().Ctx(r.Context()).Warning("httpendpoint PatchObject callback error", helpers.Error(err))
			return
		}
	case domain.CopyStrategy:
		if err := a.callbacks.PutObject(r.Context(), kindName, bodyBytes); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			logger.L().Ctx(r.Context()).Warning("httpendpoint PutObject callback error", helpers.Error(err))
			return
		}
	}
	w.WriteHeader(http.StatusAccepted)
}

func (a *Adapter) Start(ctx context.Context) error {
	// In order to validate the kind is supported by resources list in the config we will build a map of supported verbs, group, version and resource
	// build the map:
	a.supportedPaths = map[domain.Strategy]map[string]map[string]map[string]bool{}
	for _, resource := range a.cfg.HTTPEndpoint.Resources {
		lowerCaseStrategy := domain.Strategy(strings.ToLower(string(resource.Strategy)))
		if _, ok := a.supportedPaths[lowerCaseStrategy]; !ok {
			a.supportedPaths[lowerCaseStrategy] = map[string]map[string]map[string]bool{}
		}
		if _, ok := a.supportedPaths[lowerCaseStrategy][resource.Group]; !ok {
			a.supportedPaths[lowerCaseStrategy][resource.Group] = map[string]map[string]bool{}
		}
		if _, ok := a.supportedPaths[lowerCaseStrategy][resource.Group][resource.Version]; !ok {
			a.supportedPaths[lowerCaseStrategy][resource.Group][resource.Version] = map[string]bool{}
		}
		a.supportedPaths[lowerCaseStrategy][resource.Group][resource.Version][resource.Resource] = true
	}
	go func() {
		if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L().Ctx(ctx).Fatal("httpendpoint server error", helpers.Error(err))
		}
		logger.L().Ctx(ctx).Info("httpendpoint server stopped")
	}()
	a.isStarted = true
	logger.L().Ctx(ctx).Info("httpendpoint server started", helpers.String("port", a.cfg.HTTPEndpoint.ServerPort))
	return nil
}

func (a *Adapter) Stop(ctx context.Context) error {
	if err := a.httpServer.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}

func (a *Adapter) IsRelated(ctx context.Context, id domain.ClientIdentifier) bool {
	return a.cfg.InCluster.Account == id.Account && a.cfg.InCluster.ClusterName == id.Cluster
}
