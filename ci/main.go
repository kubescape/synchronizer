package main

import (
	"context"
	"fmt"

	"github.com/kubescape/synchronizer/ci/lib"
)

func main() {
	ctx := context.Background()
	// Initialize Dagger client
	client, src, err := lib.InitDagger(ctx)
	if err != nil {
		fmt.Println(err)
	}

	defer client.Close()

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
