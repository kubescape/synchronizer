package main

import (
	"net/http"

	"github.com/gobwas/ws"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/core"
)

func main() {
	logger.L().SetLevel("debug")
	// config
	//cfg, err := config.LoadConfig("./configuration")
	//if err != nil {
	//	logger.L().Fatal("unable to load configuration", helpers.Error(err))
	//}
	// backend adapter
	//ctx, cancel := context.WithCancel(context.Background())
	//defer cancel()
	//adapter := backend.NewBackendAdapter(ctx, cfg, nil)
	// mock adapter
	adapter := adapters.NewMockAdapter()
	// websocket server
	http.ListenAndServe(":8080", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			logger.L().Error("unable to upgrade connection", helpers.Error(err))
			return
		}
		go func() {
			defer conn.Close()
			synchronizer := core.NewSynchronizerServer(adapter, conn)
			err = synchronizer.Start()
			if err != nil {
				logger.L().Error("error during sync", helpers.Error(err))
				return
			}
		}()
	}))
}
