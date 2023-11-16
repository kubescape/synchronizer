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
var testNames = []string{
	"unit",
}

// the container registry for the multi-platform image
const imageRepo = "quay.io/matthiasb_1/synchronizer"

func main() {
	if err := prMerged(context.Background()); err != nil {
		fmt.Println(err)
	}
}

func prCreated(ctx context.Context) error {
	// initialize Dagger client
	client, src, err := initDagger(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	// run basic tests on PR
	if err := basicTests(ctx, client, src); err != nil {
		return err
	}

	return nil
}

func basicTests(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
	// TODO implement
	return nil
}

func prMerged(ctx context.Context) error {
	// initialize Dagger client
	client, src, err := initDagger(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	// build image
	imagePrerelease, err := dockerBuild(ctx, client, src)
	if err != nil {
		return err
	}
	fmt.Println(imagePrerelease)

	// run system tests
	if err := systemTest(ctx, client, imagePrerelease); err != nil {
		return err
	}

	// create release and retag image
	imageRelease, err := releaseRetag(ctx, client, imagePrerelease)
	if err != nil {
		return err
	}
	fmt.Println(imageRelease)

	return nil
}

func initDagger(ctx context.Context) (*dagger.Client, *dagger.Directory, error) {
	client, err := dagger.Connect(ctx, dagger.WithLogOutput(os.Stdout))
	if err != nil {
		return nil, nil, err
	}

	// get reference to the local project
	// git checkout is performed by the calling CI system (e.g. GitHub Actions)
	src := client.Host().Directory(".")
	return client, src, nil
}

func releaseRetag(ctx context.Context, client *dagger.Client, prerelease string) (string, error) {
	// TODO implement
	return "", nil
}

func systemTest(ctx context.Context, client *dagger.Client, imageDigest string) error {
	// TODO implement
	return nil
}

func dockerBuild(ctx context.Context, client *dagger.Client, src *dagger.Directory) (string, error) {
	// TODO prerelease image tag
	// run unit tests
	if err := unitTest(ctx, client, src); err != nil {
		return "", err
	}
	// build and push the multi-platform image
	imageDigest, err := buildPush(ctx, client, src)
	if err != nil {
		return "", err
	}
	// TODO sign image with cosign
	return imageDigest, nil
}

func unitTest(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
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
		WithExec([]string{"go", "test", "./..."}).
		Stderr(ctx)
	if err != nil {
		return err
	}
	fmt.Println(out)
	return nil
}

func buildPush(ctx context.Context, client *dagger.Client, src *dagger.Directory) (string, error) {
	fmt.Println("Building multi-platform image...")
	platformVariants := make([]*dagger.Container, 0, len(platforms))
	for _, platform := range platforms {
		ctr := src.
			DockerBuild(dagger.DirectoryDockerBuildOpts{
				Dockerfile: "build/Dockerfile",
				Platform:   platform,
			})
		platformVariants = append(platformVariants, ctr)
	}

	imageDigest, err := client.
		Container().
		Publish(ctx, imageRepo, dagger.ContainerPublishOpts{
			PlatformVariants: platformVariants,
			// Some registries may require explicit use of docker mediatypes
			// rather than the default OCI mediatypes
			// MediaTypes: dagger.Dockermediatypes,
		})
	if err != nil {
		return "", err
	}
	fmt.Println("Pushed multi-platform image w/ digest: ", imageDigest)
	return imageDigest, nil
}
