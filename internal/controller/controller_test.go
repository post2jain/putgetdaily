package controller

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
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

type staticLimiter struct{ tokens int }

func (l *staticLimiter) Take(time.Time) int     { return l.tokens }
func (l *staticLimiter) Observe(actual float64) {}
func (l *staticLimiter) CurrentTarget() float64 { return float64(l.tokens) }

func TestControllerRespectsRequestLimit(t *testing.T) {
	mock := newMockS3Server("bucket")
	defer mock.Close()

	cfg := &config.Config{
		Endpoints:     []string{mock.URL()},
		Bucket:        "bucket",
		Operation:     "put",
		Duration:      5 * time.Second,
		Requests:      5,
		Concurrency:   2,
		TargetTPS:     100,
		ObjectSize:    64,
		PartSize:      64,
		AccessKey:     "ak",
		SecretKey:     "sk",
		Region:        "us-east-1",
		KeyPrefix:     "test",
		MultipartFail: "abort",
		PayloadVerify: true,
	}

	keyGen, err := keyspace.New(keyspace.Options{Prefix: cfg.KeyPrefix})
	if err != nil {
		t.Fatalf("keyspace: %v", err)
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
		t.Fatalf("client: %v", err)
	}
	defer client.Close()

	runner := worker.NewRunner(client, cfg, store, nil)
	collector := stats.NewCollector()
	ctrl := New(cfg, nil, keyGen, runner, collector, &staticLimiter{tokens: 10}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	snapshot := ctrl.Run(ctx)

	if snapshot.TotalRequests != uint64(cfg.Requests) {
		t.Fatalf("expected %d requests, got %d", cfg.Requests, snapshot.TotalRequests)
	}
}

// mock S3 server reused across tests.
type mockS3Server struct {
	srv    *httptest.Server
	bucket string
	mu     sync.RWMutex
	data   map[string][]byte
}

func newMockS3Server(bucket string) *mockS3Server {
	m := &mockS3Server{bucket: bucket, data: make(map[string][]byte)}
	srv := httptest.NewServer(http.HandlerFunc(m.handle))
	m.srv = srv
	return m
}

func (m *mockS3Server) URL() string { return m.srv.URL }

func (m *mockS3Server) Close() { m.srv.Close() }

func (m *mockS3Server) handle(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/"+m.bucket+"/")
	switch {
	case r.Method == http.MethodPut && r.URL.RawQuery == "uploads=":
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<UploadId>mock</UploadId>`))
	case r.Method == http.MethodPut && strings.Contains(r.URL.RawQuery, "uploadId="):
		io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", "etag")
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploadId="):
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete:
		m.mu.Lock()
		delete(m.data, key)
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.data[key] = body
		m.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet:
		m.mu.RLock()
		payload, ok := m.data[key]
		m.mu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if rng := r.Header.Get("Range"); rng != "" {
			parts := strings.Split(strings.TrimPrefix(rng, "bytes="), "-")
			if len(parts) == 2 {
				start, _ := strconv.Atoi(parts[0])
				end, _ := strconv.Atoi(parts[1])
				if start >= 0 && end < len(payload) {
					payload = payload[start : end+1]
				}
			}
		}
		w.WriteHeader(http.StatusOK)
		w.Write(payload)
	case r.Method == http.MethodHead:
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusOK)
	}
}
