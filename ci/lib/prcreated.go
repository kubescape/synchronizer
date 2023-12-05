package lib

import (
	"context"

	"dagger.io/dagger"
)

func PrCreated(ctx context.Context, client *dagger.Client, src *dagger.Directory) error {
	// run basic tests on PR
	if err := BasicTests(ctx, client, src); err != nil {
		return err
	}

	return nil
}
