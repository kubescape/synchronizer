package lib

import (
	"context"
	"dagger.io/dagger"
	"fmt"
	"strings"
)

type BasicTests struct {
	Client         *dagger.Client
	Ctx            context.Context
	Inputs         BasicTestsInputs
	Secrets        BasicTestsSecrets
	Src            *dagger.Directory
	runGitguardian bool
	runSnyk        bool
}

type BasicTestsInputs struct {
	GoVersion       string
	Go111Module     string
	CgoEnabled      bool
	BuildPath       string
	UnittestsPath   string
	FailedThreshold int
}

type BasicTestsSecrets struct {
	SnykToken         string
	GitguardianApiKey string
}

// CheckSecrets checks if secrets are set and enables/disables the corresponding tasks
func (b *BasicTests) CheckSecrets() {
	if b.Secrets.GitguardianApiKey != "" {
		b.runGitguardian = true
	}
	if b.Secrets.SnykToken != "" {
		b.runSnyk = true
	}
}

func (b *BasicTests) EnvironmentTest() error {
	fmt.Println("Running environment tests...")

	// Go cache volume
	goBuildCache := b.Client.CacheVolume("goBuild")
	goPkgCache := b.Client.CacheVolume("goPkg")

	// Run tests
	testCmd := []string{"go", "test", "-v"}
	if b.Inputs.CgoEnabled {
		fmt.Println("Test race conditions")
		testCmd = append(testCmd, "-race", "./...")
	} else {
		fmt.Println("Test without race conditions")
		testCmd = append(testCmd, "./...")
	}
	out, err := b.Client.Container().
		From("golang:"+b.Inputs.GoVersion+"-bullseye").
		WithDirectory("/src", b.Src).
		WithMountedCache("/go/pkg", goPkgCache).
		WithMountedCache("/root/.cache/go-build", goBuildCache).
		WithWorkdir("/src").
		WithExec(testCmd).
		Stderr(b.Ctx)
	if err != nil {
		return err
	}
	fmt.Println(out)

	// CodeQL analysis
	// FIXME doesn't work, Microsoft's image is missing go executable
	// TODO consider running codeql analysis in a separate workflow using the codeql-action
	//skipEntrypoint := dagger.ContainerWithExecOpts{SkipEntrypoint: true}
	//out, err = b.Client.Container().
	//	From("mcr.microsoft.com/cstsectools/codeql-container").
	//	WithUser("root").
	//	WithDirectory("/opt/src", b.Src).
	//	WithMountedCache("/go/pkg", goPkgCache).
	//	WithMountedCache("/root/.cache/go-build", goBuildCache).
	//	WithWorkdir("/opt/src").
	//	WithExec(strings.Split("/usr/local/codeql-home/codeql/codeql database create --language=go /tmp/source_db -s /opt/src", " "), skipEntrypoint).
	//	WithExec(strings.Split("/usr/local/codeql-home/codeql/codeql database upgrade /tmp/source_db", " "), skipEntrypoint).
	//	WithExec(strings.Split("/usr/local/codeql-home/codeql/codeql database analyze --format=sarifv2 --output=/tmp/issues.sarif /tmp/source_db ${language}-security-and-quality.qls", " "), skipEntrypoint).
	//	WithEnvVariable("CODEQL_PLATFORM", "linux64").
	//	WithEnvVariable("CODEQL_EXTRACTOR_GO_ROOT", "/opt/hostedtoolcache/CodeQL/2.15.4/x64/codeql/go").
	//	WithExec(strings.Split("/usr/local/codeql-home/codeql/codeql database init --db-cluster /tmp/source_db --source-root=/opt/src --extractor-include-aliases --language=go --begin-tracing --trace-process-name=Runner.Worker.exe --calculate-language-specific-baseline --sublanguage-file-coverage", " "), skipEntrypoint).
	//	WithExec(strings.Split("/usr/local/codeql-home/codeql/go/tools/autobuild.sh", " "), skipEntrypoint).
	//	WithExec(strings.Split("/usr/local/codeql-home/codeql/codeql database finalize --finalize-dataset --threads=4 --ram=14567 /tmp/source_db/go", " "), skipEntrypoint).
	//	WithExec(strings.Split("/usr/local/codeql-home/codeql/codeql database run-queries --ram=14567 --threads=4 /tmp/source_db/go --min-disk-free=1024 -v --expect-discarded-cache --intra-layer-parallelism", " "), skipEntrypoint).
	//	WithExec(strings.Split("/usr/local/codeql-home/codeql/codeql database interpret-results --threads=4 --format=sarif-latest -v --output=../results/go.sarif --no-sarif-add-snippets --print-diagnostics-summary --print-metrics-summary --sarif-add-query-help --sarif-group-rules-by-pack --sarif-add-baseline-file-info --sublanguage-file-coverage --sarif-include-diagnostics --new-analysis-summary /tmp/source_db/go", " "), skipEntrypoint).
	//	WithExec(strings.Split("/usr/local/codeql-home/codeql/codeql database cleanup /tmp/source_db/go --mode=brutal", " "), skipEntrypoint).
	//	Stderr(b.Ctx)
	//if err != nil {
	//	return err
	//}
	//fmt.Println(out)

	return nil
}

func (b *BasicTests) BasicTest() error {
	fmt.Println("Running basic tests...")

	// Go cache volume
	goBuildCache := b.Client.CacheVolume("goBuild")
	goPkgCache := b.Client.CacheVolume("goPkg")

	// Run go-licenses
	fmt.Println("Scanning - Forbidden Licenses (go-licenses)")
	out, err := b.Client.Container().
		From("golang:"+b.Inputs.GoVersion+"-bullseye").
		WithDirectory("/src", b.Src).
		WithMountedCache("/go/pkg", goPkgCache).
		WithMountedCache("/root/.cache/go-build", goBuildCache).
		WithWorkdir("/src").
		WithExec(strings.Split("go install github.com/google/go-licenses@latest", " ")).
		WithExec(strings.Split("go-licenses check ./...", " ")).
		Stderr(b.Ctx)
	if err != nil {
		return err
	}
	fmt.Println(out)

	// Run GitGuardian
	if b.runGitguardian {
		fmt.Println("Scanning - Credentials (GitGuardian)")
		out, err := b.Client.Container().
			From("gitguardian/ggshield:v1.22.0").
			WithDirectory("/src", b.Src).
			WithMountedCache("/go/pkg", goPkgCache).
			WithMountedCache("/root/.cache/go-build", goBuildCache).
			WithWorkdir("/src").
			WithEntrypoint([]string{"/app/docker/actions-secret-entrypoint.sh"}).
			Stderr(b.Ctx)
		if err != nil {
			return err
		}
		fmt.Println(out)
	}

	// Run Snyk
	if b.runSnyk {
		fmt.Println("Scanning - Vulnerabilities (Snyk)")
		out, err := b.Client.Container().
			From("snyk/snyk:golang").
			WithDirectory("/src", b.Src).
			WithMountedCache("/go/pkg", goPkgCache).
			WithMountedCache("/root/.cache/go-build", goBuildCache).
			WithWorkdir("/src").
			Stderr(b.Ctx)
		if err != nil {
			return err
		}
		fmt.Println(out)
	}

	// Run golangci-lint
	fmt.Println("Test go linting")
	out, err = b.Client.Container().
		From("golangci/golangci-lint").
		WithDirectory("/src", b.Src).
		WithMountedCache("/go/pkg", goPkgCache).
		WithMountedCache("/root/.cache/go-build", goBuildCache).
		WithWorkdir("/src").
		WithExec(strings.Split("golangci-lint run --out-format=github-actions --new-from-rev=HEAD~ --timeout 10m", " ")).
		Stderr(b.Ctx)
	// FIXME need to find a way to generate patch like in https://github.com/golangci/golangci-lint-action/blob/master/src/utils/diffUtils.ts#L5
	if err != nil {
		return err
	}
	fmt.Println(out)

	// Test coverage
	// TODO implement

	// Comment results to PR
	// TODO consider continue using peter-evans/create-or-update-comment@v2 (no real need to use dagger)

	return nil
}

func (b *BasicTests) Run() error {
	// check if secrets are set
	b.CheckSecrets()

	if err := b.EnvironmentTest(); err != nil {
		return fmt.Errorf("environment test failed: %w", err)
	}

	if err := b.BasicTest(); err != nil {
		return fmt.Errorf("basic test failed: %w", err)
	}

	return nil
}
