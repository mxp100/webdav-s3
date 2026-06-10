.PHONY: deps run tidy

deps: tidy

tidy:
	go mod tidy

run: tidy
	go run ./cmd/webdav-s3
