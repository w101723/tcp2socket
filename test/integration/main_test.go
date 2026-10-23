package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

var binPath string

func TestMain(m *testing.M) {
	// Find project root
	wd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get working dir: %v\n", err)
		os.Exit(1)
	}

	// Navigate up to root if we are in test/integration
	rootDir := wd
	for {
		if _, err := os.Stat(filepath.Join(rootDir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(rootDir)
		if parent == rootDir {
			fmt.Fprintf(os.Stderr, "could not find go.mod in any parent directory\n")
			os.Exit(1)
		}
		rootDir = parent
	}

	tempDir, err := os.MkdirTemp("", "tcp2socket_test_*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tempDir)

	binPath = filepath.Join(tempDir, "binary")

	// Build the project once: go build [-race] -o temp/binary .
	buildArgs := []string{"build"}
	if os.Getenv("TCP2SOCKET_TEST_RACE") == "1" {
		buildArgs = append(buildArgs, "-race")
	}
	buildArgs = append(buildArgs, "-o", binPath, ".")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	buildCmd := exec.CommandContext(ctx, "go", buildArgs...)
	buildCmd.Dir = rootDir
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to build binary: %v\n", err)
		os.Exit(1)
	}

	exitCode := m.Run()

	os.RemoveAll(tempDir)
	os.Exit(exitCode)
}
