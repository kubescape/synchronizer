package main

import (
	"context"

	"github.com/gobwas/ws"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters/incluster/v1"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/utils"
)

func main() {
	logger.L().SetLevel("debug")
	// config
	cfg, err := config.LoadConfig("./configuration")
	if err != nil {
		logger.L().Fatal("unable to load configuration", helpers.Error(err))
	}
	// k8s client
	k8sclient, err := utils.NewClient()
	if err != nil {
		logger.L().Fatal("unable to create k8s client", helpers.Error(err))
	}
	// in-cluster adapter
	adapter := incluster.NewInClusterAdapter(cfg, k8sclient)
	// websocket client
	conn, _, _, err := ws.DefaultDialer.Dial(context.Background(), cfg.Backend)
	if err != nil {
		logger.L().Fatal("unable to create websocket connection", helpers.Error(err))
	}
	defer conn.Close()
	// synchronizer
	synchronizer := core.NewSynchronizerClient(adapter, conn)
	err = synchronizer.Start()
	if err != nil {
		logger.L().Fatal("error during sync", helpers.Error(err))
	}
}
