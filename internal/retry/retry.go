package retry

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"syscall"

	"s3load/internal/s3client"
)

// IsRetryable determines whether an error warrants a retry.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var httpErr *s3client.Error
	if errors.As(err, &httpErr) {
		return ShouldRetryStatus(httpErr.StatusCode)
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() || netErr.Temporary() {
			return true
		}
	}

	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNRESET, syscall.ECONNABORTED, syscall.EPIPE, syscall.ETIMEDOUT:
			return true
		}
	}

	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "timeout") || strings.Contains(lower, "temporarily unavailable") {
		return true
	}

	return false
}

// ShouldRetryStatus reports whether an HTTP status code should be retried.
func ShouldRetryStatus(status int) bool {
	if status == 0 {
		return true
	}
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return true
	case http.StatusConflict:
		return true
	}
	if status >= 500 && status <= 599 {
		return true
	}
	return false
}
