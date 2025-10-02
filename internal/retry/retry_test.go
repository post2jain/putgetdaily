package retry

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"s3load/internal/s3client"
)

func TestShouldRetryStatus(t *testing.T) {
	cases := map[int]bool{
		http.StatusOK:                  false,
		http.StatusRequestTimeout:      true,
		http.StatusTooManyRequests:     true,
		http.StatusInternalServerError: true,
		http.StatusBadRequest:          false,
	}
	for status, want := range cases {
		if got := ShouldRetryStatus(status); got != want {
			t.Fatalf("status %d expected %v got %v", status, want, got)
		}
	}
}

func TestIsRetryable(t *testing.T) {
	if IsRetryable(nil) {
		t.Fatalf("nil error should not be retryable")
	}
	if !IsRetryable(context.DeadlineExceeded) {
		t.Fatalf("deadline exceeded should retry")
	}
	errorCases := []error{
		&s3client.Error{StatusCode: http.StatusInternalServerError},
		errors.New("timeout reading response"),
	}
	for _, err := range errorCases {
		if !IsRetryable(err) {
			t.Fatalf("expected retryable for %v", err)
		}
	}
}
