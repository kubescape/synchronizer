package main

import (
	"context"

	"dagger.io/dagger"
)

func CheckSecrets(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
	// TODO: Write the code for the Check-secret job
	return nil
}

func EnvironmentTest(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
	// TODO: Write the code for the Environment-Test job
	return nil
}

func BasicTest(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
	// TODO: Write the code for the Basic-Test job
	return nil
}

func BasicTests(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
	// TODO implement
	return nil
}
