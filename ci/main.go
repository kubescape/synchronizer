package main

import (
	"context"
	"dagger.io/dagger"
	"fmt"
	"os"

	"github.com/kubescape/synchronizer/ci/lib"
)

func main() {
	ctx := context.Background()
	// Initialize Dagger client
	client, src, err := lib.InitDagger(ctx)
	if err != nil {
		fmt.Println(err)
	}

	defer func(client *dagger.Client) {
		_ = client.Close()
	}(client)

	_ = os.Setenv("GO_VERSION", "1.21")
	_ = os.Setenv("CGO_ENABLED", "0")
	_ = os.Setenv("BUILD_PATH", "./...")
	_ = os.Setenv("UNIT_TESTS_PATH", "./...")
	_ = os.Setenv("FAILEDTHRESHOLD", "50")

	err = lib.PrCreated(ctx, client, src)
	if err != nil {
		fmt.Println(err)
	}

	/*
		err = lib.PrMerged(ctx, client, src)
		if err != nil {
			fmt.Println(err)
		}
	*/
}
