package main

import (
	"context"
	"fmt"
	"os"

	"dagger.io/dagger"
)

// the platforms to build for and push in a multi-platform image
var platforms = []dagger.Platform{
	"linux/amd64",
	"linux/arm64",
}

// the system tests to run on the newly built image
var TestNames = []string{
	"unit",
}

// the container registry for the multi-platform image
const imageRepo = "quay.io/matthiasb_1/synchronizer"

func main() {
	ctx := context.Background()
	// Initialize Dagger client
	client, src, err := InitDagger(ctx)
	if err != nil {
		fmt.Println(err)
	}

	defer client.Close()

	err = PrMerged(ctx, client, src)
	if err != nil {
		fmt.Println(err)
	}
}

func InitDagger(ctx context.Context) (*dagger.Client, *dagger.Directory, error) {
	client, err := dagger.Connect(ctx, dagger.WithLogOutput(os.Stdout))
	if err != nil {
		return nil, nil, err
	}

	// get reference to the local project
	// git checkout is performed by the calling CI system (e.g. GitHub Actions)
	src := client.Host().Directory(".")
	return client, src, nil
}

func PrMerged(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
	// Build image
	imagePrerelease, err := DockerBuild(ctx, client, src)
	if err != nil {
		return err
	}
	fmt.Println(imagePrerelease)

	// run system tests
	if err := SystemTest(ctx, client, imagePrerelease); err != nil {
		return err
	}

	// create release and retag image
	imageRelease, err := ReleaseRetag(ctx, client, imagePrerelease)
	if err != nil {
		return err
	}
	fmt.Println(imageRelease)

	return nil
}
