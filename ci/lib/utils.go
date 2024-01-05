package lib

import (
	"context"
	"os"

	"dagger.io/dagger"
)

// the platforms to build for and push in a multi-platform image
var platforms = []dagger.Platform{
	"linux/amd64",
	"linux/arm64",
}

// TestNames contains the system tests to run on the newly built image
var TestNames = []string{
	"unit",
}

// the container registry for the multi-platform image
const imageRepo = "quay.io/matthiasb_1/synchronizer"

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
