package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Credentials represents AWS credentials for SigV4.
type Credentials struct {
	AccessKey    string
	SecretKey    string
	SessionToken string
}

// SignRequest applies AWS SigV4 signing to the request.
func SignRequest(req *http.Request, region, service string, creds Credentials, payloadHash string, now time.Time) error {
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	amzDate := now.UTC().Format("20060102T150405Z")
	dateStamp := now.UTC().Format("20060102")

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if creds.SessionToken != "" {
		req.Header.Set("x-amz-security-token", creds.SessionToken)
	}
	req.Header.Set("host", canonicalHost(req.URL))

	canonicalReq, signedHeaders := buildCanonicalRequest(req, payloadHash)
	hashedCanonical := sha256HashHex(canonicalReq)

	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		hashedCanonical,
	}, "\n")

	signingKey := deriveSigningKey(creds.SecretKey, dateStamp, region, service)
	signature := hmacSHA256Hex(signingKey, stringToSign)

	authorization := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		creds.AccessKey,
		credentialScope,
		signedHeaders,
		signature,
	)
	req.Header.Set("Authorization", authorization)
	return nil
}

func buildCanonicalRequest(req *http.Request, payloadHash string) (string, string) {
	canonicalURI := canonicalURI(req.URL)
	canonicalQuery := canonicalQuery(req.URL)

	headerNames := make([]string, 0, len(req.Header))
	headers := make(map[string][]string)
	for name, values := range req.Header {
		lower := strings.ToLower(name)
		headerNames = append(headerNames, lower)
		headers[lower] = values
	}
	sort.Strings(headerNames)
	builder := strings.Builder{}
	signedHeaders := strings.Builder{}
	var prior string
	for _, name := range headerNames {
		if name == prior {
			continue
		}
		prior = name
		values := headers[name]
		trimmed := make([]string, len(values))
		for i, v := range values {
			trimmed[i] = strings.TrimSpace(v)
		}
		sort.Strings(trimmed)
		builder.WriteString(name)
		builder.WriteString(":")
		builder.WriteString(strings.Join(trimmed, ","))
		builder.WriteString("\n")
		if signedHeaders.Len() > 0 {
			signedHeaders.WriteString(";")
		}
		signedHeaders.WriteString(name)
	}

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQuery,
		builder.String(),
		signedHeaders.String(),
		payloadHash,
	}, "\n")

	return canonicalRequest, signedHeaders.String()
}

func canonicalURI(u *url.URL) string {
	if u.Path == "" {
		return "/"
	}
	// Ensure each segment is escaped individually.
	segments := strings.Split(u.EscapedPath(), "/")
	for i, segment := range segments {
		if segment == "" {
			continue
		}
		unescaped, err := url.PathUnescape(segment)
		if err != nil {
			continue
		}
		segments[i] = url.PathEscape(unescaped)
	}
	return strings.Join(segments, "/")
}

func canonicalQuery(u *url.URL) string {
	if u.RawQuery == "" {
		return ""
	}
	values, _ := url.ParseQuery(u.RawQuery)
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		vals := values[key]
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, url.QueryEscape(key)+"="+url.QueryEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

func canonicalHost(u *url.URL) string {
	return strings.ToLower(u.Host)
}

func sha256HashHex(data string) string {
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:])
}

func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return kSigning
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func hmacSHA256Hex(key []byte, data string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}
