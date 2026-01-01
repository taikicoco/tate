// Package main provides the entry point for the Tate columnar database.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/taikicoco/tate/internal/executor"
	"github.com/taikicoco/tate/internal/shell"
	"github.com/taikicoco/tate/internal/storage"
)

func main() {
	dataDir := flag.String("data", "", "Data directory (default: ~/.tate)")
	flag.Parse()

	dir, err := resolveDataDir(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	catalog, exec, err := initialize(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	sh := shell.New(catalog, exec, dir)
	if err := sh.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func resolveDataDir(dataDir string) (string, error) {
	if dataDir != "" {
		return dataDir, nil
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}
	return filepath.Join(homeDir, ".tate"), nil
}

func initialize(dataDir string) (*storage.Catalog, *executor.Executor, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	catalog, err := storage.NewCatalog(dataDir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize catalog: %w", err)
	}

	exec := executor.New(catalog, dataDir)
	return catalog, exec, nil
}
