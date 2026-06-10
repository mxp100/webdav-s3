#!/usr/bin/env bash
set -euo pipefail

# Generate go.sum entries for required modules
go mod tidy

# Run the WebDAV server
go run ./cmd/webdav-s3
