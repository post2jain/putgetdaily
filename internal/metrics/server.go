package metrics

import (
	"context"
	"net/http"
	"time"

	"s3load/internal/stats"
)

// Server wraps an HTTP server exposing metrics and health endpoints.
type Server struct {
	srv *http.Server
}

// NewServer initialises the HTTP server using the provided collector.
func NewServer(addr string, collector *stats.Collector) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", collector.JSONHandler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return &Server{srv: srv}
}

// Start runs the HTTP server asynchronously.
func (s *Server) Start() {
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Server lifecycle errors are expected when shutting down; ignore.
		}
	}()
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	if s == nil || s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}
