package worker

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"log/slog"

	"s3load/internal/config"
	"s3load/internal/payload"
	"s3load/internal/s3client"
)

// Result captures an operation's outcome metrics.
type Result struct {
	BytesSent     int64
	BytesReceived int64
	Success       bool
}

// WorkItem represents a scheduled operation for a specific key.
type WorkItem struct {
	Operation string
	Key       string
	Aux       map[string]string
}

// Runner executes S3 operations.
type Runner struct {
	client        *s3client.Client
	cfg           *config.Config
	store         payload.Store
	logger        *slog.Logger
	mpMu          sync.Mutex
	sessions      map[string]*multipartSession
	lifecycleOnce sync.Once
	lifecycleData []byte
	lifecycleErr  error
}

// NewRunner constructs a Runner.
func NewRunner(client *s3client.Client, cfg *config.Config, store payload.Store, logger *slog.Logger) *Runner {
	r := &Runner{client: client, cfg: cfg, store: store, logger: logger}
	if cfg.MultipartFail == "resume" {
		r.sessions = make(map[string]*multipartSession)
	}
	return r
}

// Process executes the configured operation against the provided key.
func (r *Runner) Process(ctx context.Context, item WorkItem) (Result, error) {
	op := item.Operation
	if op == "" {
		op = r.cfg.Operation
	}
	switch op {
	case "put":
		return r.doPut(ctx, item.Key)
	case "get":
		return r.doGet(ctx, item.Key, "")
	case "multipartget":
		return r.doMultipartGet(ctx, item.Key)
	case "delete":
		return r.doDelete(ctx, item.Key)
	case "head":
		return r.doHead(ctx, item.Key)
	case "options":
		return r.doOptions(ctx, item.Key)
	case "randget":
		// Use half size window by default
		size := r.cfg.ObjectSize
		if record, ok, err := r.store.Lookup(item.Key); err == nil {
			if ok && record.Size > 0 {
				size = record.Size
			}
		} else {
			return Result{}, err
		}
		if size <= 0 {
			size = 1024
		}
		rng := size / 2
		if rng <= 0 {
			rng = 1024
		}
		start := rand.Int63n(rng)
		length := minInt64(1024, size-start)
		header := fmt.Sprintf("bytes=%d-%d", start, start+length-1)
		return r.doGet(ctx, item.Key, header)
	case "puttagging":
		return r.doPutTagging(ctx, item.Key)
	case "updatemeta":
		return r.doUpdateMeta(ctx, item.Key)
	case "restore":
		return r.doRestore(ctx, item.Key)
	case "multipartput", "multipart":
		return r.doMultipartPut(ctx, item.Key)
	case "copy":
		return r.doCopy(ctx, item.Key)
	case "objectacl":
		return r.doPutObjectACL(ctx, item)
	case "bucketacl":
		return r.doPutBucketACL(ctx, item)
	case "checklifecycle":
		return r.doCheckLifecycle(ctx, item)
	case "bucketversioning":
		return r.doBucketVersioning(ctx, item)
	case "listversions":
		return r.doListVersions(ctx, item)
	case "setretention":
		return r.doSetRetention(ctx, item)
	case "setlegalhold":
		return r.doSetLegalHold(ctx, item)
	default:
		return Result{}, fmt.Errorf("unsupported operation %q", op)
	}
}

func (r *Runner) doPut(ctx context.Context, key string) (Result, error) {
	reader := payload.NewReader(key, r.cfg.ObjectSize)
	shaSum, md5Sum, crc32c := payload.Checksums(key, r.cfg.ObjectSize)

	headers := http.Header{}
	headers.Set("Content-Type", "application/octet-stream")
	r.applyWriteHeaders(headers)
	if len(md5Sum) > 0 {
		headers.Set("Content-MD5", base64.StdEncoding.EncodeToString(md5Sum))
	}

	req := s3client.Request{
		Method:        http.MethodPut,
		Key:           key,
		Headers:       headers,
		Body:          reader,
		ExpectStatus:  http.StatusOK,
		PayloadSHA256: hex.EncodeToString(shaSum),
		ContentLength: r.cfg.ObjectSize,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if err := r.store.Remember(key, payload.Record{SHA256: shaSum, MD5: md5Sum, CRC32C: crc32c, Size: r.cfg.ObjectSize}); err != nil {
		r.warn("remember checksum", key, err)
	}
	return Result{BytesSent: r.cfg.ObjectSize, Success: true}, nil
}

func (r *Runner) doMultipartPut(ctx context.Context, key string) (Result, error) {
	if r.cfg.ObjectSize == 0 || r.cfg.ObjectSize <= r.cfg.PartSize {
		return r.doPut(ctx, key)
	}

	partSize := r.cfg.PartSize
	if partSize <= 0 {
		partSize = 5 * 1024 * 1024
	}
	totalSize := r.cfg.ObjectSize
	totalParts := int((totalSize + partSize - 1) / partSize)
	shaSum, md5Sum, crc32c := payload.Checksums(key, totalSize)

	var (
		session       *multipartSession
		parts         []completePart
		uploadID      string
		startPart     = 1
		uploadedBytes int64
	)

	if r.cfg.MultipartFail == "resume" {
		if existing := r.getSession(key); existing != nil {
			if existing.TotalParts == totalParts && existing.PartSize == partSize && existing.TotalSize == totalSize {
				session = existing
				uploadID = existing.UploadID
				parts = append(parts, existing.Parts...)
				startPart = existing.NextPart
				if startPart < 1 {
					startPart = 1
				}
				if existing.UploadedBytes > 0 {
					uploadedBytes = existing.UploadedBytes
				}
			} else {
				r.clearSession(key)
			}
		}
	}

	if uploadID == "" {
		initHeaders := http.Header{}
		r.applyWriteHeaders(initHeaders)

		initReq := s3client.Request{
			Method:       http.MethodPost,
			Key:          key,
			Query:        url.Values{"uploads": {""}},
			ExpectStatus: http.StatusOK,
			Headers:      initHeaders,
		}
		initResp, err := r.client.Do(ctx, initReq)
		if err != nil {
			return Result{}, err
		}
		uploadID, err = parseInitiateResponse(initResp.Body)
		if initResp.Body != nil {
			initResp.Body.Close()
		}
		if err != nil {
			return Result{}, err
		}
		if r.cfg.MultipartFail == "resume" {
			session = &multipartSession{
				UploadID:      uploadID,
				Parts:         make([]completePart, 0, totalParts),
				NextPart:      1,
				TotalParts:    totalParts,
				PartSize:      partSize,
				TotalSize:     totalSize,
				UploadedBytes: 0,
			}
			r.saveSession(key, session)
		}
	}

	if parts == nil {
		parts = make([]completePart, 0, totalParts)
	}
	if session != nil && len(parts) == len(session.Parts) {
		parts = append([]completePart(nil), session.Parts...)
	}
	if session != nil && session.NextPart > startPart {
		startPart = session.NextPart
	}
	if startPart > totalParts+1 {
		startPart = totalParts + 1
	}

	for partNum := startPart; partNum <= totalParts; partNum++ {
		offset := int64(partNum-1) * partSize
		remaining := totalSize - offset
		currentSize := partSize
		if remaining < partSize {
			currentSize = remaining
		}

		reader := payload.NewReader(key, currentSize)
		headers := http.Header{}
		headers.Set("Content-Type", "application/octet-stream")

		partReq := s3client.Request{
			Method:        http.MethodPut,
			Key:           key,
			Query:         url.Values{"partNumber": {strconv.Itoa(partNum)}, "uploadId": {uploadID}},
			Headers:       headers,
			Body:          reader,
			ExpectStatus:  http.StatusOK,
			ContentLength: currentSize,
		}

		resp, err := r.client.Do(ctx, partReq)
		if err != nil {
			r.onMultipartFailure(ctx, key, uploadID, session)
			return Result{}, err
		}
		etag := resp.Header.Get("ETag")
		if resp.Body != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		if etag == "" {
			r.onMultipartFailure(ctx, key, uploadID, session)
			return Result{}, errors.New("multipart upload missing ETag")
		}
		part := completePart{PartNumber: partNum, ETag: etag}
		parts = append(parts, part)
		uploadedBytes += currentSize
		if session != nil {
			session.Parts = append(session.Parts, part)
			session.NextPart = partNum + 1
			session.UploadedBytes = uploadedBytes
			r.saveSession(key, session)
		}
	}

	completeXML, err := buildCompleteXML(parts)
	if err != nil {
		r.onMultipartFailure(ctx, key, uploadID, session)
		return Result{}, err
	}
	completeDigest := sha256.Sum256(completeXML)
	payloadHash := hex.EncodeToString(completeDigest[:])

	headers := http.Header{}
	headers.Set("Content-Type", "application/xml")

	completeReq := s3client.Request{
		Method:        http.MethodPost,
		Key:           key,
		Query:         url.Values{"uploadId": {uploadID}},
		Headers:       headers,
		Body:          bytes.NewReader(completeXML),
		ExpectStatus:  http.StatusOK,
		ContentLength: int64(len(completeXML)),
		PayloadSHA256: payloadHash,
	}

	resp, err := r.client.Do(ctx, completeReq)
	if err != nil {
		r.onMultipartFailure(ctx, key, uploadID, session)
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	if err := r.store.Remember(key, payload.Record{SHA256: shaSum, MD5: md5Sum, CRC32C: crc32c, Size: totalSize}); err != nil {
		r.warn("remember checksum", key, err)
	}
	if session != nil {
		r.clearSession(key)
	}
	return Result{BytesSent: uploadedBytes, Success: true}, nil
}

func (r *Runner) doGet(ctx context.Context, key string, rangeHeader string) (Result, error) {
	headers := http.Header{}
	expect := http.StatusOK
	if rangeHeader != "" {
		headers.Set("Range", rangeHeader)
		expect = http.StatusPartialContent
	}
	req := s3client.Request{
		Method:       http.MethodGet,
		Key:          key,
		Headers:      headers,
		ExpectStatus: expect,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()

	shaHasher := sha256.New()
	mdHasher := md5.New()
	crcHasher := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	bytesRead, err := io.Copy(io.MultiWriter(shaHasher, mdHasher, crcHasher), resp.Body)
	if err != nil {
		return Result{BytesReceived: bytesRead}, err
	}

	if r.cfg.PayloadVerify && rangeHeader == "" {
		record, ok, err := r.store.Lookup(key)
		if err != nil {
			return Result{BytesReceived: bytesRead}, err
		}
		if ok {
			crcVal := crcHasher.Sum32()
			if err := compareChecksums(key, record, shaHasher.Sum(nil), mdHasher.Sum(nil), &crcVal); err != nil {
				return Result{BytesReceived: bytesRead}, err
			}
		}
	}
	return Result{BytesReceived: bytesRead, Success: true}, nil
}

func (r *Runner) doMultipartGet(ctx context.Context, key string) (Result, error) {
	record, ok, err := r.store.Lookup(key)
	if err != nil {
		return Result{}, err
	}
	if !ok || record.Size <= 0 {
		return r.doGet(ctx, key, "")
	}
	partSize := r.cfg.PartSize
	if partSize <= 0 {
		partSize = 5 * 1024 * 1024
	}
	if record.Size <= partSize {
		return r.doGet(ctx, key, "")
	}

	shaHasher := sha256.New()
	mdHasher := md5.New()
	crcHasher := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	var total int64
	for offset := int64(0); offset < record.Size; offset += partSize {
		end := offset + partSize - 1
		if end >= record.Size {
			end = record.Size - 1
		}
		headers := http.Header{}
		headers.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))
		req := s3client.Request{
			Method:       http.MethodGet,
			Key:          key,
			Headers:      headers,
			ExpectStatus: http.StatusPartialContent,
		}
		resp, err := r.client.Do(ctx, req)
		if err != nil {
			return Result{BytesReceived: total}, err
		}
		data, err := io.ReadAll(resp.Body)
		if resp.Body != nil {
			resp.Body.Close()
		}
		if err != nil {
			return Result{BytesReceived: total}, err
		}
		total += int64(len(data))
		shaHasher.Write(data)
		mdHasher.Write(data)
		crcHasher.Write(data)
	}

	if r.cfg.PayloadVerify {
		crcVal := crcHasher.Sum32()
		if err := compareChecksums(key, record, shaHasher.Sum(nil), mdHasher.Sum(nil), &crcVal); err != nil {
			return Result{BytesReceived: total}, err
		}
	}
	return Result{BytesReceived: total, Success: true}, nil
}

func (r *Runner) doDelete(ctx context.Context, key string) (Result, error) {
	req := s3client.Request{
		Method:       http.MethodDelete,
		Key:          key,
		ExpectStatus: http.StatusNoContent,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if err := r.store.Forget(key); err != nil {
		r.warn("forget checksum", key, err)
	}
	return Result{Success: true}, nil
}

func (r *Runner) doHead(ctx context.Context, key string) (Result, error) {
	req := s3client.Request{
		Method:       http.MethodHead,
		Key:          key,
		ExpectStatus: http.StatusOK,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		resp.Body.Close()
	}
	return Result{Success: true}, nil
}

func (r *Runner) doOptions(ctx context.Context, key string) (Result, error) {
	req := s3client.Request{
		Method:       http.MethodOptions,
		Key:          key,
		ExpectStatus: http.StatusOK,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return Result{Success: true}, nil
}

func (r *Runner) doPutTagging(ctx context.Context, key string) (Result, error) {
	headers := http.Header{}
	headers.Set("Content-Type", "application/xml")
	bodyStr := r.taggingXML()
	body := strings.NewReader(bodyStr)
	size := int64(body.Len())
	digest := sha256.Sum256([]byte(bodyStr))
	req := s3client.Request{
		Method:        http.MethodPut,
		Key:           key,
		Headers:       headers,
		Query:         url.Values{"tagging": {""}},
		ExpectStatus:  http.StatusOK,
		Body:          readerFromString(body),
		ContentLength: size,
		PayloadSHA256: hex.EncodeToString(digest[:]),
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return Result{BytesSent: size, Success: true}, nil
}

func (r *Runner) doUpdateMeta(ctx context.Context, key string) (Result, error) {
	headers := http.Header{}
	headers.Set("x-amz-metadata-directive", "REPLACE")
	headers.Set("x-amz-copy-source", fmt.Sprintf("/%s/%s", r.cfg.Bucket, key))
	applyMetadata(headers, r.cfg.Metadata)
	if r.cfg.Tagging != "" {
		headers.Set("x-amz-tagging-directive", "REPLACE")
		headers.Set("x-amz-tagging", r.cfg.Tagging)
	}
	req := s3client.Request{
		Method:       http.MethodPut,
		Key:          key,
		Headers:      headers,
		ExpectStatus: http.StatusOK,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return Result{Success: true}, nil
}

func (r *Runner) doRestore(ctx context.Context, key string) (Result, error) {
	headers := http.Header{}
	headers.Set("Content-Type", "application/xml")
	bodyStr := `<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Days>1</Days><GlacierJobParameters><Tier>Standard</Tier></GlacierJobParameters></RestoreRequest>`
	payload := strings.NewReader(bodyStr)
	size := int64(payload.Len())
	digest := sha256.Sum256([]byte(bodyStr))
	req := s3client.Request{
		Method:        http.MethodPost,
		Key:           key,
		Headers:       headers,
		Query:         url.Values{"restore": {""}},
		ExpectStatus:  http.StatusAccepted,
		Body:          readerFromString(payload),
		ContentLength: size,
		PayloadSHA256: hex.EncodeToString(digest[:]),
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return Result{BytesSent: size, Success: true}, nil
}

func (r *Runner) doCopy(ctx context.Context, key string) (Result, error) {
	headers := http.Header{}
	headers.Set("x-amz-copy-source", fmt.Sprintf("/%s/%s", r.cfg.Bucket, key))
	if len(r.cfg.Metadata) > 0 {
		headers.Set("x-amz-metadata-directive", "REPLACE")
		applyMetadata(headers, r.cfg.Metadata)
	}
	if r.cfg.Tagging != "" {
		headers.Set("x-amz-tagging-directive", "REPLACE")
		headers.Set("x-amz-tagging", r.cfg.Tagging)
	}
	req := s3client.Request{
		Method:       http.MethodPut,
		Key:          key,
		Headers:      headers,
		ExpectStatus: http.StatusOK,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return Result{Success: true}, nil
}

func (r *Runner) doPutObjectACL(ctx context.Context, item WorkItem) (Result, error) {
	acl := firstNonEmpty(auxValue(item.Aux, "acl"), r.cfg.ACL)
	if acl == "" {
		return Result{}, errors.New("acl not configured")
	}
	if item.Key == "" {
		return Result{}, errors.New("object key required for objectacl")
	}
	headers := http.Header{}
	headers.Set("x-amz-acl", acl)
	req := s3client.Request{
		Method:       http.MethodPut,
		Key:          item.Key,
		Headers:      headers,
		Query:        url.Values{"acl": {""}},
		ExpectStatus: http.StatusOK,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return Result{Success: true}, nil
}

func (r *Runner) doPutBucketACL(ctx context.Context, item WorkItem) (Result, error) {
	acl := firstNonEmpty(auxValue(item.Aux, "bucket_acl"), r.cfg.BucketACL)
	if acl == "" {
		return Result{}, errors.New("bucket-acl not configured")
	}
	headers := http.Header{}
	headers.Set("x-amz-acl", acl)
	req := s3client.Request{
		Method:       http.MethodPut,
		Headers:      headers,
		Query:        url.Values{"acl": {""}},
		ExpectStatus: http.StatusOK,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return Result{Success: true}, nil
}

func (r *Runner) doCheckLifecycle(ctx context.Context, item WorkItem) (Result, error) {
	if r.cfg.LifecycleFile == "" {
		return Result{}, errors.New("lifecycle-file not configured")
	}
	expected, err := r.loadLifecycleExpected()
	if err != nil {
		return Result{}, err
	}
	req := s3client.Request{
		Method:       http.MethodGet,
		Query:        url.Values{"lifecycle": {""}},
		ExpectStatus: http.StatusOK,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{}, err
	}
	trimmed := bytes.TrimSpace(data)
	if !bytes.Equal(trimmed, expected) {
		return Result{}, fmt.Errorf("lifecycle mismatch: got %q", string(trimmed))
	}
	return Result{BytesReceived: int64(len(data)), Success: true}, nil
}

func (r *Runner) loadLifecycleExpected() ([]byte, error) {
	r.lifecycleOnce.Do(func() {
		data, err := os.ReadFile(r.cfg.LifecycleFile)
		if err != nil {
			r.lifecycleErr = err
			return
		}
		r.lifecycleData = bytes.TrimSpace(data)
	})
	return r.lifecycleData, r.lifecycleErr
}

func (r *Runner) doBucketVersioning(ctx context.Context, item WorkItem) (Result, error) {
	state := strings.ToLower(firstNonEmpty(auxValue(item.Aux, "versioning_state"), r.cfg.VersioningState))
	if state == "" {
		return Result{}, errors.New("versioning-state not configured")
	}
	var status string
	switch state {
	case "enabled":
		status = "Enabled"
	case "suspended":
		status = "Suspended"
	default:
		return Result{}, fmt.Errorf("unsupported versioning-state %q", state)
	}
	body := fmt.Sprintf(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>%s</Status></VersioningConfiguration>`, status)
	buf := []byte(body)
	headers := http.Header{}
	headers.Set("Content-Type", "application/xml")
	hash := sha256.Sum256(buf)
	req := s3client.Request{
		Method:        http.MethodPut,
		Headers:       headers,
		Query:         url.Values{"versioning": {""}},
		Body:          bytes.NewReader(buf),
		ContentLength: int64(len(buf)),
		PayloadSHA256: hex.EncodeToString(hash[:]),
		ExpectStatus:  http.StatusOK,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return Result{BytesSent: int64(len(buf)), Success: true}, nil
}

func (r *Runner) doListVersions(ctx context.Context, item WorkItem) (Result, error) {
	query := url.Values{"versions": {""}}
	if item.Key != "" {
		query.Set("prefix", item.Key)
	}
	req := s3client.Request{
		Method:       http.MethodGet,
		Query:        query,
		ExpectStatus: http.StatusOK,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{}, err
	}
	if err := r.verifyListVersions(data); err != nil {
		return Result{BytesReceived: int64(len(data))}, err
	}
	return Result{BytesReceived: int64(len(data)), Success: true}, nil
}

func (r *Runner) doSetRetention(ctx context.Context, item WorkItem) (Result, error) {
	mode := strings.ToLower(firstNonEmpty(auxValue(item.Aux, "retention_mode"), r.cfg.RetentionMode))
	if mode == "" {
		return Result{}, errors.New("retention-mode not configured")
	}
	var modeUpper string
	switch mode {
	case "governance":
		modeUpper = "GOVERNANCE"
	case "compliance":
		modeUpper = "COMPLIANCE"
	default:
		return Result{}, fmt.Errorf("unsupported retention-mode %q", mode)
	}
	duration := r.cfg.RetentionDuration
	if v := auxValue(item.Aux, "retention_duration"); v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return Result{}, fmt.Errorf("parse retention_duration: %w", err)
		}
		duration = parsed
	}
	if duration <= 0 {
		return Result{}, errors.New("retention-duration must be greater than zero")
	}
	retainUntil := time.Now().Add(duration).UTC().Format(time.RFC3339)
	body := fmt.Sprintf(`{"Mode":"%s","RetainUntilDate":"%s"}`, modeUpper, retainUntil)
	buf := []byte(body)
	headers := http.Header{}
	headers.Set("Content-Type", "application/json")
	if r.cfg.BypassGovernance || strings.EqualFold(auxValue(item.Aux, "bypass_governance"), "true") {
		headers.Set("x-amz-bypass-governance-retention", "true")
	}
	hash := sha256.Sum256(buf)
	req := s3client.Request{
		Method:        http.MethodPut,
		Key:           item.Key,
		Headers:       headers,
		Query:         url.Values{"retention": {""}},
		Body:          bytes.NewReader(buf),
		ContentLength: int64(len(buf)),
		PayloadSHA256: hex.EncodeToString(hash[:]),
		ExpectStatus:  http.StatusOK,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return Result{BytesSent: int64(len(buf)), Success: true}, nil
}

func (r *Runner) doSetLegalHold(ctx context.Context, item WorkItem) (Result, error) {
	status := strings.ToLower(firstNonEmpty(auxValue(item.Aux, "legal_hold_status"), r.cfg.LegalHoldStatus))
	if status == "" {
		return Result{}, errors.New("legal-hold-status not configured")
	}
	var xmlStatus string
	switch status {
	case "on":
		xmlStatus = "ON"
	case "off":
		xmlStatus = "OFF"
	default:
		return Result{}, fmt.Errorf("unsupported legal-hold-status %q", status)
	}
	body := fmt.Sprintf(`<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>%s</Status></LegalHold>`, xmlStatus)
	buf := []byte(body)
	headers := http.Header{}
	headers.Set("Content-Type", "application/xml")
	hash := sha256.Sum256(buf)
	req := s3client.Request{
		Method:        http.MethodPut,
		Key:           item.Key,
		Headers:       headers,
		Query:         url.Values{"legal-hold": {""}},
		Body:          bytes.NewReader(buf),
		ContentLength: int64(len(buf)),
		PayloadSHA256: hex.EncodeToString(hash[:]),
		ExpectStatus:  http.StatusOK,
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return Result{BytesSent: int64(len(buf)), Success: true}, nil
}

func (r *Runner) verifyListVersions(data []byte) error {
	type versionEntry struct {
		Key            string `xml:"Key"`
		ETag           string `xml:"ETag"`
		ChecksumCRC32  string `xml:"ChecksumCRC32"`
		ChecksumCRC32C string `xml:"ChecksumCRC32C"`
	}
	type listVersionsResult struct {
		Versions []versionEntry `xml:"Version"`
	}
	var result listVersionsResult
	if err := xml.Unmarshal(data, &result); err != nil {
		return fmt.Errorf("parse list versions response: %w", err)
	}
	for _, v := range result.Versions {
		key := strings.TrimSpace(v.Key)
		if key == "" {
			continue
		}
		record, ok, err := r.store.Lookup(key)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		var mdActual []byte
		tag := strings.Trim(strings.TrimSpace(v.ETag), "\"")
		if len(tag) == 32 && !strings.Contains(tag, "-") {
			if decoded, err := hex.DecodeString(tag); err == nil {
				mdActual = decoded
			}
		}
		var crcPtr *uint32
		checksum := strings.TrimSpace(v.ChecksumCRC32C)
		if checksum == "" {
			checksum = strings.TrimSpace(v.ChecksumCRC32)
		}
		if checksum != "" {
			if decoded, err := base64.StdEncoding.DecodeString(checksum); err == nil {
				var val uint32
				switch len(decoded) {
				case 4:
					val = binary.BigEndian.Uint32(decoded)
				case 8:
					val = binary.BigEndian.Uint32(decoded[:4])
				}
				if val != 0 {
					crcPtr = &val
				}
			}
		}
		if err := compareChecksums(key, record, nil, mdActual, crcPtr); err != nil {
			return err
		}
	}
	return nil
}

func auxValue(values map[string]string, key string) string {
	if values == nil {
		return ""
	}
	return values[key]
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func compareChecksums(key string, record payload.Record, shaActual, mdActual []byte, crcActual *uint32) error {
	var mismatches []string
	if len(record.SHA256) > 0 && shaActual != nil && !bytes.Equal(record.SHA256, shaActual) {
		mismatches = append(mismatches, fmt.Sprintf("sha256 expected %s got %s", hex.EncodeToString(record.SHA256), hex.EncodeToString(shaActual)))
	}
	if len(record.MD5) > 0 && mdActual != nil && !bytes.Equal(record.MD5, mdActual) {
		mismatches = append(mismatches, fmt.Sprintf("md5 expected %s got %s", hex.EncodeToString(record.MD5), hex.EncodeToString(mdActual)))
	}
	if record.CRC32C != 0 && crcActual != nil && record.CRC32C != *crcActual {
		mismatches = append(mismatches, fmt.Sprintf("crc32c expected %08x got %08x", record.CRC32C, *crcActual))
	}
	if len(mismatches) > 0 {
		return fmt.Errorf("checksum mismatch for %s: %s", key, strings.Join(mismatches, "; "))
	}
	return nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// OpenConnections reports the number of open HTTP connections.
func (r *Runner) OpenConnections() int {
	return r.client.OpenConnections()
}

// SetConcurrency adjusts the underlying client's connection ceiling.
func (r *Runner) SetConcurrency(limit int) {
	if r.client == nil {
		return
	}
	r.client.SetConcurrency(limit)
}

func (r *Runner) onMultipartFailure(ctx context.Context, key, uploadID string, session *multipartSession) {
	if r.cfg.MultipartFail == "resume" && session != nil {
		r.saveSession(key, session)
		return
	}
	r.abortMultipart(ctx, key, uploadID)
	r.clearSession(key)
}

func (r *Runner) warn(msg, key string, err error) {
	if err == nil || r.logger == nil {
		return
	}
	r.logger.Warn(msg, slog.String("key", key), slog.String("error", err.Error()))
}

func (r *Runner) getSession(key string) *multipartSession {
	if r.sessions == nil {
		return nil
	}
	r.mpMu.Lock()
	defer r.mpMu.Unlock()
	sess, ok := r.sessions[key]
	if !ok {
		return nil
	}
	clone := *sess
	if len(sess.Parts) > 0 {
		clone.Parts = make([]completePart, len(sess.Parts))
		copy(clone.Parts, sess.Parts)
	}
	return &clone
}

func (r *Runner) saveSession(key string, sess *multipartSession) {
	if r.sessions == nil {
		return
	}
	clone := *sess
	if len(sess.Parts) > 0 {
		clone.Parts = make([]completePart, len(sess.Parts))
		copy(clone.Parts, sess.Parts)
	}
	r.mpMu.Lock()
	r.sessions[key] = &clone
	r.mpMu.Unlock()
}

func (r *Runner) clearSession(key string) {
	if r.sessions == nil {
		return
	}
	r.mpMu.Lock()
	delete(r.sessions, key)
	r.mpMu.Unlock()
}

// readerFromString wraps *strings.Reader to satisfy io.ReadSeeker.
func readerFromString(r *strings.Reader) io.ReadSeeker {
	return r
}

func (r *Runner) applyWriteHeaders(headers http.Header) {
	applyMetadata(headers, r.cfg.Metadata)
	if r.cfg.Tagging != "" {
		headers.Set("x-amz-tagging", r.cfg.Tagging)
	}
}

type initiateMultipartUploadResult struct {
	UploadID string `xml:"UploadId"`
}

type completeMultipartUpload struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []completePart `xml:"Part"`
}

type completePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type multipartSession struct {
	UploadID      string
	Parts         []completePart
	NextPart      int
	TotalParts    int
	PartSize      int64
	TotalSize     int64
	UploadedBytes int64
}

func parseInitiateResponse(body io.Reader) (string, error) {
	if body == nil {
		return "", errors.New("initiate multipart upload returned empty body")
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("read initiate multipart response: %w", err)
	}
	var parsed initiateMultipartUploadResult
	if err := xml.Unmarshal(data, &parsed); err != nil {
		return "", fmt.Errorf("parse initiate multipart response: %w", err)
	}
	if parsed.UploadID == "" {
		return "", errors.New("initiate multipart upload missing UploadId")
	}
	return parsed.UploadID, nil
}

func buildCompleteXML(parts []completePart) ([]byte, error) {
	if len(parts) == 0 {
		return nil, errors.New("multipart upload requires at least one part")
	}
	payload := completeMultipartUpload{Parts: parts}
	out, err := xml.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal complete multipart payload: %w", err)
	}
	return out, nil
}

func (r *Runner) abortMultipart(ctx context.Context, key, uploadID string) {
	if uploadID == "" {
		return
	}
	req := s3client.Request{
		Method: http.MethodDelete,
		Key:    key,
		Query:  url.Values{"uploadId": {uploadID}},
	}
	resp, err := r.client.Do(ctx, req)
	if err != nil {
		return
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func applyMetadata(headers http.Header, metadata map[string]string) {
	if len(metadata) == 0 {
		return
	}
	for key, value := range metadata {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		headers.Set("x-amz-meta-"+strings.ToLower(trimmed), value)
	}
}

func (r *Runner) taggingXML() string {
	if len(r.cfg.TaggingMap) == 0 {
		return `<Tagging><TagSet><Tag><Key>env</Key><Value>test</Value></Tag></TagSet></Tagging>`
	}
	return buildTaggingXML(r.cfg.TaggingMap)
}

func buildTaggingXML(tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("<Tagging><TagSet>")
	for _, k := range keys {
		b.WriteString("<Tag><Key>")
		b.WriteString(k)
		b.WriteString("</Key><Value>")
		b.WriteString(tags[k])
		b.WriteString("</Value></Tag>")
	}
	b.WriteString("</TagSet></Tagging>")
	return b.String()
}
