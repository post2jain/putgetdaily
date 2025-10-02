package worker

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

	"fmt"

	sigv4 "s3load/internal/aws/sigv4"
	"s3load/internal/config"
	"s3load/internal/payload"
	"s3load/internal/s3client"
)

type mockS3 struct {
	srv    *httptest.Server
	data   map[string][]byte
	mu     sync.RWMutex
	bucket string
}

func newMockS3(bucket string) (*mockS3, error) {
	m := &mockS3{data: make(map[string][]byte), bucket: bucket}
	h := http.NewServeMux()
	h.HandleFunc("/", m.handle)
	var srv *httptest.Server
	var panicErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr = fmt.Errorf("start server: %v", r)
			}
		}()
		srv = httptest.NewServer(h)
	}()
	if panicErr != nil {
		return nil, panicErr
	}
	m.srv = srv
	return m, nil
}

func (m *mockS3) Close() { m.srv.Close() }

func (m *mockS3) URL() string { return m.srv.URL }

func (m *mockS3) handle(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/"+m.bucket+"/")
	switch {
	case r.Method == http.MethodPut && r.URL.RawQuery == "uploads=":
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<UploadId>mock</UploadId>`))
	case r.Method == http.MethodPut && strings.Contains(r.URL.RawQuery, "uploadId="):
		// multipart part upload
		if _, err := io.ReadAll(r.Body); err == nil {
			w.Header().Set("ETag", "etag")
			w.WriteHeader(http.StatusOK)
		}
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
			// simplistic range handling assuming bytes=a-b
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

func TestRunnerPutAndGet(t *testing.T) {
	mock, err := newMockS3("test-bucket")
	if err != nil {
		t.Skipf("mock server unavailable: %v", err)
	}
	defer mock.Close()

	cfg := &config.Config{
		Endpoints:     []string{mock.URL()},
		Bucket:        "test-bucket",
		Operation:     "put",
		Duration:      time.Second,
		Concurrency:   1,
		TargetTPS:     10,
		ObjectSize:    128,
		PartSize:      128,
		Retries:       1,
		RetryBackoff:  10 * time.Millisecond,
		AccessKey:     "test",
		SecretKey:     "secret",
		Region:        "us-east-1",
		KeyPrefix:     "testobject",
		MultipartFail: "abort",
		PayloadVerify: true,
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
		t.Fatalf("s3client.New: %v", err)
	}
	defer client.Close()

	runner := NewRunner(client, cfg, store, nil)

	ctx := context.Background()
	key := "unit-test"
	if _, err := runner.Process(ctx, WorkItem{Operation: "put", Key: key}); err != nil {
		t.Fatalf("put process error: %v", err)
	}
	if _, ok, err := store.Lookup(key); err != nil || !ok {
		t.Fatalf("expected checksum stored; err=%v ok=%v", err, ok)
	}

	if _, err := runner.Process(ctx, WorkItem{Operation: "get", Key: key}); err != nil {
		t.Fatalf("get process error: %v", err)
	}
}
