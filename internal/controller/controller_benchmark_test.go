package controller

import (
	"context"
	"testing"
	"time"

	sigv4 "s3load/internal/aws/sigv4"
	"s3load/internal/config"
	"s3load/internal/keyspace"
	"s3load/internal/payload"
	"s3load/internal/s3client"
	"s3load/internal/stats"
	"s3load/internal/worker"
)

func BenchmarkControllerScheduling(b *testing.B) {
	mock := newMockS3Server("bench-bucket")
	defer mock.Close()

	cfg := &config.Config{
		Endpoints:     []string{mock.URL()},
		Bucket:        "bench-bucket",
		Operation:     "put",
		Duration:      2 * time.Second,
		Requests:      200,
		Concurrency:   8,
		TargetTPS:     500,
		ObjectSize:    256,
		PartSize:      256,
		AccessKey:     "bench",
		SecretKey:     "bench",
		Region:        "us-east-1",
		KeyPrefix:     "bench",
		PayloadVerify: false,
	}

	store := payload.NewMemoryStore()
	client, err := s3client.New(s3client.Options{
		Endpoints:   cfg.Endpoints,
		Bucket:      cfg.Bucket,
		Concurrency: cfg.Concurrency,
		Timeout:     5 * time.Second,
		Credentials: sigv4.Credentials{AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey},
		Region:      cfg.Region,
	})
	if err != nil {
		b.Fatalf("client: %v", err)
	}
	defer client.Close()

	handling := worker.NewRunner(client, cfg, store, nil)
	keyGen, err := keyspace.New(keyspace.Options{Prefix: cfg.KeyPrefix})
	if err != nil {
		b.Fatalf("keyspace: %v", err)
	}
	limiter := &staticLimiter{tokens: 1000}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collector := stats.NewCollector()
		ctrl := New(cfg, nil, keyGen, handling, collector, limiter, nil)
		ctrl.Run(context.Background())
	}
}
