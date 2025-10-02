package s3client

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"s3load/internal/aws/sigv4"
)

// Client issues S3 requests across one or more endpoints.
type Client struct {
	endpoints    []*endpointClient
	selector     atomic.Uint64
	creds        sigv4.Credentials
	region       string
	limiter      *connLimiter
	reapInterval time.Duration
	reaperStop   chan struct{}
	reaperDone   sync.WaitGroup
	closeOnce    sync.Once
}

type endpointClient struct {
	base   *url.URL
	http   *http.Client
	style  AddressingStyle
	bucket string
}

// Error represents an error response from S3.
type Error struct {
	StatusCode int
	Body       string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("unexpected status %d: %s", e.StatusCode, e.Body)
}

// AddressingStyle selects path vs virtual host addressing.
type AddressingStyle int

const (
	PathStyle AddressingStyle = iota
	VirtualHostStyle
)

// Options configure the S3 client.
type Options struct {
	Endpoints    []string
	Bucket       string
	Concurrency  int
	Timeout      time.Duration
	ConnLifetime time.Duration
	Credentials  sigv4.Credentials
	Region       string
	Addressing   AddressingStyle
	ReapInterval time.Duration
}

// New creates a new client.
func New(opts Options) (*Client, error) {
	if len(opts.Endpoints) == 0 {
		return nil, fmt.Errorf("at least one endpoint required")
	}
	limiter := newConnLimiter(opts.Concurrency)
	endpoints := make([]*endpointClient, 0, len(opts.Endpoints))
	dialer := &net.Dialer{}
	for _, raw := range opts.Endpoints {
		parsed, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("parse endpoint %q: %w", raw, err)
		}
		transport := &http.Transport{
			MaxConnsPerHost:     0,
			MaxIdleConnsPerHost: 0,
			MaxIdleConns:        0,
			IdleConnTimeout:     opts.ConnLifetime,
			ForceAttemptHTTP2:   true,
		}
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			if err := limiter.acquire(ctx); err != nil {
				return nil, err
			}
			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				limiter.release()
				return nil, err
			}
			return limiter.wrapConn(conn), nil
		}
		client := &http.Client{
			Transport: transport,
			Timeout:   opts.Timeout,
		}
		endpoints = append(endpoints, &endpointClient{
			base:   parsed,
			http:   client,
			style:  opts.Addressing,
			bucket: opts.Bucket,
		})
	}

	client := &Client{
		endpoints:    endpoints,
		creds:        opts.Credentials,
		region:       opts.Region,
		limiter:      limiter,
		reapInterval: opts.ReapInterval,
	}
	client.SetConcurrency(opts.Concurrency)
	client.startReaper()
	return client, nil
}

// Request describes an S3 HTTP request to execute.
type Request struct {
	Method        string
	Key           string
	Query         url.Values
	Headers       http.Header
	Body          io.ReadSeeker
	ExpectStatus  int
	PayloadSHA256 string
	ContentLength int64
}

// Do executes the request against the next endpoint.
func (c *Client) Do(ctx context.Context, req Request) (*http.Response, error) {
	endpoint := c.nextEndpoint()

	fullURL, err := c.buildURL(endpoint, req.Key, req.Query)
	if err != nil {
		return nil, err
	}

	var bodyReader io.Reader
	if req.Body != nil {
		if _, err := req.Body.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind body: %w", err)
		}
		bodyReader = io.NopCloser(req.Body)
	}

	httpReq, err := http.NewRequestWithContext(ctx, req.Method, fullURL, bodyReader)
	if err != nil {
		return nil, err
	}

	// Apply headers
	for name, values := range req.Headers {
		for _, v := range values {
			httpReq.Header.Add(name, v)
		}
	}

	switch {
	case req.ContentLength >= 0:
		httpReq.ContentLength = req.ContentLength
	case req.Body != nil:
		if seeker, ok := req.Body.(interface{ Size() int64 }); ok {
			httpReq.ContentLength = seeker.Size()
		} else if sizer, ok := req.Body.(interface{ Len() int }); ok {
			httpReq.ContentLength = int64(sizer.Len())
		}
	}

	hostHeader := endpoint.base.Host
	if endpoint.style == VirtualHostStyle {
		hostHeader = bucketHost(endpoint.base.Host, endpoint.bucket)
	}
	httpReq.Header.Set("Host", hostHeader)

	if err := sigv4.SignRequest(httpReq, c.region, "s3", c.creds, req.PayloadSHA256, time.Now()); err != nil {
		return nil, err
	}

	resp, err := endpoint.http.Do(httpReq)
	if err != nil {
		return nil, err
	}

	if req.ExpectStatus != 0 && resp.StatusCode != req.ExpectStatus {
		var snippet string
		if resp.Body != nil {
			preview, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			snippet = string(preview)
			resp.Body.Close()
		}
		return nil, &Error{StatusCode: resp.StatusCode, Body: snippet}
	}
	return resp, nil
}

func (c *Client) startReaper() {
	if c.reapInterval <= 0 {
		return
	}
	c.reaperStop = make(chan struct{})
	c.reaperDone.Add(1)
	go func() {
		defer c.reaperDone.Done()
		ticker := time.NewTicker(c.reapInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, endpoint := range c.endpoints {
					if transport, ok := endpoint.http.Transport.(*http.Transport); ok {
						transport.CloseIdleConnections()
					}
				}
			case <-c.reaperStop:
				return
			}
		}
	}()
}

// OpenConnections returns the current number of open connections tracked by this client.
func (c *Client) OpenConnections() int {
	if c.limiter == nil {
		return 0
	}
	return c.limiter.currentConnections()
}

// SetConcurrency updates the connection limiter and HTTP transport limits.
func (c *Client) SetConcurrency(limit int) {
	if c.limiter != nil {
		c.limiter.setLimit(limit)
	}

	perEndpoint := 0
	if limit > 0 && len(c.endpoints) > 0 {
		perEndpoint = limit / len(c.endpoints)
		if perEndpoint == 0 {
			perEndpoint = 1
		}
	}

	for _, endpoint := range c.endpoints {
		transport, ok := endpoint.http.Transport.(*http.Transport)
		if !ok {
			continue
		}
		if limit <= 0 {
			transport.MaxConnsPerHost = 0
			transport.MaxIdleConnsPerHost = 0
			transport.MaxIdleConns = 0
			continue
		}
		transport.MaxConnsPerHost = perEndpoint
		transport.MaxIdleConnsPerHost = perEndpoint
		transport.MaxIdleConns = limit
	}
}

// Close stops background maintenance and releases idle connections.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		if c.reaperStop != nil {
			close(c.reaperStop)
			c.reaperDone.Wait()
		}
		for _, endpoint := range c.endpoints {
			if transport, ok := endpoint.http.Transport.(*http.Transport); ok {
				transport.CloseIdleConnections()
			}
		}
	})
}

func (c *Client) buildURL(endpoint *endpointClient, key string, query url.Values) (string, error) {
	base := *endpoint.base
	path := strings.TrimPrefix(base.Path, "/")
	if endpoint.style == VirtualHostStyle {
		// Bucket goes into host
		base.Host = bucketHost(base.Host, endpoint.bucket)
		if key != "" {
			base.Path = joinPath(path, key)
		} else {
			base.Path = joinPath(path, "")
		}
	} else {
		base.Path = joinPath(path, endpoint.bucket, key)
	}
	if query != nil {
		base.RawQuery = query.Encode()
	}
	return base.String(), nil
}

func (c *Client) nextEndpoint() *endpointClient {
	idx := c.selector.Add(1) - 1
	return c.endpoints[idx%uint64(len(c.endpoints))]
}

func joinPath(elements ...string) string {
	parts := make([]string, 0, len(elements))
	for _, el := range elements {
		el = strings.Trim(el, "/")
		if el == "" {
			continue
		}
		parts = append(parts, el)
	}
	return "/" + strings.Join(parts, "/")
}

func bucketHost(host, bucket string) string {
	if bucket == "" {
		return host
	}
	return bucket + "." + host
}
