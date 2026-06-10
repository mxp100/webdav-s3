package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	// Server
	Address         string
	WebDAVPrefix    string
	BasicAuthUser   string
	BasicAuthPass   string
	TLSCertFile     string
	TLSKeyFile      string
	ShutdownTimeout time.Duration

	// S3/MinIO
	S3Endpoint            string
	S3Region              string
	S3Bucket              string
	S3AccessKeyID         string
	S3SecretAccessKey     string
	S3UsePathStyle        bool
	S3Secure              bool
	S3MaxIdleConns        int
	S3MaxIdleConnsPerHost int

	// FS options
	UploadBufferLimit int64
}

func Load() (*Config, error) {
	_ = godotenv.Load() // ignore error if .env doesn't exist

	cfg := &Config{
		Address:               getEnv("ADDRESS", ":8080"),
		WebDAVPrefix:          getEnv("WEBDAV_PREFIX", "/"),
		BasicAuthUser:         os.Getenv("BASIC_AUTH_USER"),
		BasicAuthPass:         os.Getenv("BASIC_AUTH_PASS"),
		TLSCertFile:           os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:            os.Getenv("TLS_KEY_FILE"),
		ShutdownTimeout:       getEnvDuration("SHUTDOWN_TIMEOUT", 10*time.Second),
		S3Endpoint:            getEnv("S3_ENDPOINT", "s3.amazonaws.com"),
		S3Region:              getEnv("S3_REGION", ""),
		S3Bucket:              getEnv("S3_BUCKET", ""),
		S3AccessKeyID:         os.Getenv("S3_ACCESS_KEY_ID"),
		S3SecretAccessKey:     os.Getenv("S3_SECRET_ACCESS_KEY"),
		S3UsePathStyle:        getEnvBool("S3_USE_PATH_STYLE", false),
		S3Secure:              getEnvBool("S3_SECURE", true),
		S3MaxIdleConns:        getEnvInt("S3_MAX_IDLE_CONNS", 2048),
		S3MaxIdleConnsPerHost: getEnvInt("S3_MAX_IDLE_CONNS_PER_HOST", 1024),
		UploadBufferLimit:     getEnvInt64("UPLOAD_BUFFER_LIMIT", 8*1024*1024), // 8MiB
	}

	// Clean prefix to always start with "/" and not end with "/"
	if cfg.WebDAVPrefix == "" {
		cfg.WebDAVPrefix = "/"
	}
	if !strings.HasPrefix(cfg.WebDAVPrefix, "/") {
		cfg.WebDAVPrefix = "/" + cfg.WebDAVPrefix
	}
	if len(cfg.WebDAVPrefix) > 1 && strings.HasSuffix(cfg.WebDAVPrefix, "/") {
		cfg.WebDAVPrefix = strings.TrimSuffix(cfg.WebDAVPrefix, "/")
	}

	return cfg, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "yes", "y", "on":
			return true
		case "0", "false", "no", "n", "off":
			return false
		}
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getEnvInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
