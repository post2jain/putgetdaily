//go:build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	sigv4 "s3load/internal/aws/sigv4"
	"s3load/internal/config"
	"s3load/internal/payload"
	"s3load/internal/s3client"
	"s3load/internal/worker"
)

func TestMinIOPutGet(t *testing.T) {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	access := os.Getenv("MINIO_ACCESS_KEY")
	secret := os.Getenv("MINIO_SECRET_KEY")
	bucket := os.Getenv("MINIO_BUCKET")
	if endpoint == "" || access == "" || secret == "" || bucket == "" {
		t.Skip("MINIO_* environment variables not set")
	}

	cfg := &config.Config{
		Endpoints:     []string{endpoint},
		Bucket:        bucket,
		Operation:     "put",
		Duration:      time.Second,
		Concurrency:   1,
		TargetTPS:     1,
		ObjectSize:    128,
		PartSize:      128,
		AccessKey:     access,
		SecretKey:     secret,
		Region:        "us-east-1",
		KeyPrefix:     "integration",
		PayloadVerify: true,
	}

	store := payload.NewMemoryStore()
	client, err := s3client.New(s3client.Options{
		Endpoints:   cfg.Endpoints,
		Bucket:      cfg.Bucket,
		Concurrency: cfg.Concurrency,
		Timeout:     30 * time.Second,
		Credentials: sigv4.Credentials{AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey},
		Region:      cfg.Region,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer client.Close()

	runner := worker.NewRunner(client, cfg, store, nil)
	ctx := context.Background()
	key := "integration-object"

	if _, err := runner.Process(ctx, worker.WorkItem{Operation: "put", Key: key}); err != nil {
		t.Fatalf("put error: %v", err)
	}
	if _, err := runner.Process(ctx, worker.WorkItem{Operation: "get", Key: key}); err != nil {
		t.Fatalf("get error: %v", err)
	}
}
