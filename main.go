package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/net/webdav"

	"webdav-s3/internal/config"
	"webdav-s3/internal/s3fs"
)

func main() {
	// Load configuration (includes .env if present)
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	// Build HTTP transport for S3 with large connection pools
	s3Transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 60 * time.Second}).DialContext,
		MaxIdleConns:          cfg.S3MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.S3MaxIdleConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	endpoint, secure, err := parseEndpoint(cfg.S3Endpoint, cfg.S3Secure)
	if err != nil {
		log.Fatalf("invalid S3 endpoint: %v", err)
	}

	minioOpts := &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.S3AccessKeyID, cfg.S3SecretAccessKey, ""),
		Secure:       secure,
		Region:       cfg.S3Region,
		BucketLookup: bucketLookupMode(cfg.S3UsePathStyle),
		Transport:    s3Transport,
	}
	mcli, err := minio.New(endpoint, minioOpts)
	if err != nil {
		log.Fatalf("failed to init S3 client: %v", err)
	}
	mcli.SetAppInfo("webdav-s3", "1.0.0")

	// Ensure bucket exists (with region auto-discovery fallback)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	exists, err := mcli.BucketExists(ctx, cfg.S3Bucket)
	if err != nil {
		// Try to discover bucket region and reinitialize client to avoid 301 redirects
		if loc, rerr := mcli.GetBucketLocation(ctx, cfg.S3Bucket); rerr == nil && loc != "" && loc != minioOpts.Region {
			minioOpts.Region = loc
			if m2, e2 := minio.New(endpoint, minioOpts); e2 == nil {
				mcli = m2
				mcli.SetAppInfo("webdav-s3", "1.0.0")
				exists, err = mcli.BucketExists(ctx, cfg.S3Bucket)
			}
		}
	}
	if err != nil {
		log.Fatalf("error checking bucket existence: %v", err)
	}
	if !exists {
		log.Fatalf("bucket %q does not exist", cfg.S3Bucket)
	}

	// Build WebDAV handler
	fs := s3fs.New(mcli, cfg.S3Bucket, s3fs.Options{
		UploadBufferLimit: cfg.UploadBufferLimit,
	})
	ls := webdav.NewMemLS()
	dav := &webdav.Handler{
		Prefix:     cfg.WebDAVPrefix,
		FileSystem: fs,
		LockSystem: ls,
	}

	var handler http.Handler = dav
	if cfg.BasicAuthUser != "" || cfg.BasicAuthPass != "" {
		handler = basicAuth(handler, cfg.BasicAuthUser, cfg.BasicAuthPass)
	}

	srv := &http.Server{
		Addr:              cfg.Address,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       0, // allow long uploads, controlled by reverse proxy if any
		WriteTimeout:      0,
		IdleTimeout:       120 * time.Second,
	}

	// Start server
	errCh := make(chan error, 1)
	go func() {
		log.Printf("WebDAV server listening on %s (prefix=%q, bucket=%q, endpoint=%s, secure=%v, pathStyle=%v)",
			cfg.Address, cfg.WebDAVPrefix, cfg.S3Bucket, endpoint, secure, cfg.S3UsePathStyle)
		if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
			errCh <- srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			errCh <- srv.ListenAndServe()
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		log.Printf("received signal %s, shutting down...", sig)
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	} else {
		log.Printf("server stopped")
	}
}

func basicAuth(next http.Handler, user, pass string) http.Handler {
	// If either is empty, reject unless both provided
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user == "" || pass == "" {
			http.Error(w, "basic auth not configured", http.StatusUnauthorized)
			return
		}
		u, p, ok := r.BasicAuth()
		if !ok || u != user || p != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="restricted"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func parseEndpoint(raw string, fallbackSecure bool) (host string, secure bool, err error) {
	if raw == "" {
		return "", false, errors.New("empty endpoint")
	}
	// Allow raw host:port, http(s)://host[:port]
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		u, e := url.Parse(raw)
		if e != nil {
			return "", false, e
		}
		return u.Host, u.Scheme == "https", nil
	}
	return raw, fallbackSecure, nil
}

func bucketLookupMode(pathStyle bool) minio.BucketLookupType {
	if pathStyle {
		return minio.BucketLookupPath
	}
	return minio.BucketLookupAuto
}
