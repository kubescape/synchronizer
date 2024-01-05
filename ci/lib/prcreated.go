package lib

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"dagger.io/dagger"
)

// PrCreated Run basic tests on PR
func PrCreated(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
	failedThreshold, err := strconv.Atoi(os.Getenv("FAILEDTHRESHOLD"))
	if err != nil {
		failedThreshold = 50
	}
	basicTestsInputs := BasicTestsInputs{
		GoVersion:       os.Getenv("GO_VERSION"),
		CgoEnabled:      os.Getenv("CGO_ENABLED") == "1",
		BuildPath:       os.Getenv("BUILD_PATH"),
		UnittestsPath:   os.Getenv("UNIT_TESTS_PATH"),
		FailedThreshold: failedThreshold,
	}

	basicTestsSecrets := BasicTestsSecrets{
		SnykToken:         os.Getenv("SNYK_TOKEN"),
		GitguardianApiKey: os.Getenv("GITGUARDIAN_API_KEY"),
	}

	fmt.Printf("basicTestsInputs: %+v\n", basicTestsInputs)

	basicTests := BasicTests{
		Client:  client,
		Ctx:     ctx,
		Inputs:  basicTestsInputs,
		Secrets: basicTestsSecrets,
		Src:     src,
	}

	if err := basicTests.Run(); err != nil {
		return fmt.Errorf("basicTests.Run() failed: %w", err)
	}

	return nil
}
