package lib

import (
	"context"
	"fmt"

	"dagger.io/dagger"
)

type BasicTestsInputs struct {
	GOVERSION        string
	GO111MODULE      string
	CGOENABLED       bool
	BUILDPATH        string
	UNITTESTSPATH    string
	FAILED_THRESHOLD int
}

type BasicTestsSecrets struct {
	SNYK_TOKEN          string
	GITGUARDIAN_API_KEY string
}

// Check if secrets are set
func CheckSecrets(secrets BasicTestsSecrets) bool {
	if secrets.GITGUARDIAN_API_KEY == "" || secrets.SNYK_TOKEN == "" {
		return false
	}
	return true
}

func UnitTest(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
	fmt.Println("Running unit tests...")
	// create a cache volume
	goBuildCache := client.CacheVolume("goBuild")
	goPkgCache := client.CacheVolume("goPkg")
	// run tests
	out, err := client.Container().
		From("golang:1.20-bullseye").
		WithDirectory("/src", src).
		WithMountedCache("/go/pkg", goPkgCache).
		WithMountedCache("/root/.cache/go-build", goBuildCache).
		WithWorkdir("/src").
		WithExec([]string{"go", "test", "-v", "./..."}).
		Stderr(ctx)
	if err != nil {
		return err
	}
	fmt.Println(out)
	return nil
}

func EnvironmentTest(ctx context.Context, client *dagger.Client, src *dagger.Directory, inputs BasicTestsInputs) error {
	testCmd := []string{"go", "test", "-v"}
	if inputs.CGOENABLED {
		testCmd = append(testCmd, "-race", "./...")
	} else {
		testCmd = append(testCmd, "./...")
	}

	fmt.Println("Running environment tests...")

	// Create a cache volume
	goBuildCache := client.CacheVolume("goBuild")
	goPkgCache := client.CacheVolume("goPkg")

	// Run tests
	out, err := client.Container().
		From("golang:1.20-bullseye").
		WithDirectory("/src", src).
		WithMountedCache("/go/pkg", goPkgCache).
		WithMountedCache("/root/.cache/go-build", goBuildCache).
		WithWorkdir("/src").
		WithExec(testCmd).
		Stderr(ctx)

	if err != nil {
		return err
	}

	fmt.Println(out)
	return nil
}

func BasicTest(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
	// TODO: Write the code for the Basic-Test job
	return nil
}

func BasicTests(ctx context.Context, client *dagger.Client, src *dagger.Directory, inputs BasicTestsInputs, secrets BasicTestsSecrets) error {
	// Step 1: Check if the secrets are set
	secretsSet := CheckSecrets(secrets)
	if !secretsSet {
		fmt.Println("Secrets are not set")
		fmt.Printf("SNYK_TOKEN: %s\n", secrets.SNYK_TOKEN)
		fmt.Printf("GITGUARDIAN_API_KEY: %s\n", secrets.GITGUARDIAN_API_KEY)

		// return errors.New("secrets are not set")
	}

	err := EnvironmentTest(ctx, client, src, inputs)
	if err != nil {
		fmt.Println("Environment test failed")
	}

	return nil
}
