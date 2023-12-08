package lib

import (
	"context"
	"fmt"
	"os"

	"dagger.io/dagger"
)

// Run basic tests on PR
func PrCreated(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
	basicTestsInputs := BasicTestsInputs{
		GOVERSION:     os.Getenv("GO_VERSION"),
		CGOENABLED:    os.Getenv("CGO_ENABLED") == "1",
		BUILDPATH:     os.Getenv("BUILD_PATH"),
		UNITTESTSPATH: os.Getenv("UNITTESTS_PATH"),
	}

	basicTestsSecrets := BasicTestsSecrets{
		SNYK_TOKEN:          os.Getenv("SNYK_TOKEN"),
		GITGUARDIAN_API_KEY: os.Getenv("GITGUARDIAN_API_KEY"),
	}

	fmt.Printf("GOVERSION: %s\n", basicTestsInputs.GOVERSION)
	fmt.Printf("CGOENABLED: %t\n", basicTestsInputs.CGOENABLED)
	fmt.Printf("BUILDPATH: %s\n", basicTestsInputs.BUILDPATH)
	fmt.Printf("UNITTESTSPATH: %s\n", basicTestsInputs.UNITTESTSPATH)

	if err := BasicTests(ctx, client, src, basicTestsInputs, basicTestsSecrets); err != nil {
		return err
	}

	return nil
}
