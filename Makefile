.PHONY: build run test clean lint fmt bench

# Build the tate binary
build:
	go build -o bin/tate ./cmd/tate

# Run the tate REPL
run: build
	./bin/tate

# Run tests
test:
	go test -v ./...

# Run benchmarks
bench:
	go test -bench=. -benchmem ./...

# Clean build artifacts and data
clean:
	rm -rf bin/
	rm -rf data/
	rm -f coverage.out coverage.html

# Run linter (requires golangci-lint)
lint:
	golangci-lint run ./...

# Format code
fmt:
	go fmt ./...

# Run all checks before commit
check: fmt lint test
	@echo "All checks passed!"

# Initialize data directory
init-data:
	mkdir -p data/tables
