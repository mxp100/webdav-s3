# webdav-s3

High-performance WebDAV server backed directly by S3-compatible object storage.  
Optimized for workloads with many small files (50–300KB). Configuration via environment variables and `.env`.

## Features

- WebDAV over HTTP(S) using Go's `webdav` handler.
- Direct S3/MinIO backend (no local disk).
- In-memory buffered writes for small files to minimize S3 calls.
- Optional Basic Auth.
- .env support and production-ready defaults.
- Tuned HTTP transport for high concurrency.

## Quick start

1. Create a `.env` from example:
   ```
   cp .env.example .env
   ```
2. Edit `.env` and set:
   - `S3_BUCKET`
   - `S3_ACCESS_KEY_ID`
   - `S3_SECRET_ACCESS_KEY`
   - For non-AWS endpoints, set `S3_ENDPOINT` (e.g. `https://play.min.io:9000`).
3. Run:
   ```
   go run .
   ```
4. Mount from a WebDAV client (macOS Finder, Windows, rclone, etc.) at:
   - URL: `http://localhost:8080/`
   - If `BASIC_AUTH_USER` and `BASIC_AUTH_PASS` set, use those credentials.

## Environment variables

Server:
- ADDRESS (default `:8080`)
- WEBDAV_PREFIX (default `/`)
- BASIC_AUTH_USER, BASIC_AUTH_PASS (optional)
- TLS_CERT_FILE, TLS_KEY_FILE (optional for HTTPS)
- SHUTDOWN_TIMEOUT (default `10s`)

S3 / MinIO:
- S3_ENDPOINT (e.g. `s3.amazonaws.com` or `https://minio.example.com:9000`)
- S3_REGION (leave empty to auto-detect; set explicitly only if required)
- S3_BUCKET (required)
- S3_ACCESS_KEY_ID (required)
- S3_SECRET_ACCESS_KEY (required)
- S3_USE_PATH_STYLE (`false` by default; set `true` for path-style)
- S3_SECURE (`true` by default; ignored if endpoint has scheme)

HTTP transport tuning (S3 client):
- S3_MAX_IDLE_CONNS (default `2048`)
- S3_MAX_IDLE_CONNS_PER_HOST (default `1024`)

File system:
- UPLOAD_BUFFER_LIMIT (bytes; default `8388608` / 8 MiB)

## Notes and limitations

- Append writes are not supported (S3 limitation).
- Rename is implemented as copy-then-delete; for directories this iterates and copies all objects beneath the prefix.
- Directory listings infer "folders" from prefixes and optional zero-byte folder markers created by `Mkdir`.
- For very large directories, listing may scan recursively to derive immediate children; adjust client behavior or namespace layout accordingly.

## Build
