.PHONY: lint test fmt vet run

lint: vet
	@if command -v golangci-lint >/dev/null 2>&1; then golangci-lint run ./...; fi

test:
	go test -v -race ./...

fmt:
	gofmt -s -w .

vet:
	go vet ./...

run:
	go run cmd/worker/main.go
