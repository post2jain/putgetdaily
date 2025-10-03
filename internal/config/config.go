package config

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const envPrefix = "S3LOAD_"

// Config captures the runtime configuration of the load generator.
type Config struct {
	Endpoints         []string          `json:"endpoints"`
	Bucket            string            `json:"bucket"`
	Operation         string            `json:"operation"`
	ACL               string            `json:"acl"`
	BucketACL         string            `json:"bucket_acl"`
	Duration          time.Duration     `json:"duration"`
	Requests          int               `json:"requests"`
	Concurrency       int               `json:"concurrency"`
	MaxConcurrency    int               `json:"max_concurrency"`
	TargetTPS         float64           `json:"tps"`
	ObjectSize        int64             `json:"size"`
	PartSize          int64             `json:"partsize"`
	Retries           int               `json:"retries"`
	RetryBackoff      time.Duration     `json:"retry_backoff"`
	RetryJitterPct    float64           `json:"retry_jitter"`
	AccessKey         string            `json:"access_key"`
	SecretKey         string            `json:"secret_key"`
	Region            string            `json:"region"`
	ObjectListFile    string            `json:"object_list_file"`
	LifecycleFile     string            `json:"lifecycle_file"`
	WorkloadFile      string            `json:"workload_file"`
	WorkloadTemplate  string            `json:"workload_template"`
	ListTemplates     bool              `json:"list_templates"`
	Describe          bool              `json:"describe"`
	DryRun            bool              `json:"dry_run"`
	KeyStrategy       string            `json:"key_strategy"`
	Prefixes          []string          `json:"prefixes"`
	Metadata          map[string]string `json:"metadata"`
	Tagging           string            `json:"tagging"`
	TaggingMap        map[string]string `json:"-"`
	KeyPrefix         string            `json:"prefix"`
	AddressingStyle   string            `json:"addressing_style"`
	HTTPTimeout       time.Duration     `json:"http_timeout"`
	ConnLifetime      time.Duration     `json:"conn_lifetime"`
	ConnReapInterval  time.Duration     `json:"conn_reap_interval"`
	ShutdownGrace     time.Duration     `json:"shutdown_grace"`
	MetricsAddr       string            `json:"metrics_addr"`
	JSONReportPath    string            `json:"json_report"`
	LogLevel          string            `json:"log_level"`
	PayloadVerify     bool              `json:"verify_payload"`
	RateAlgorithm     string            `json:"rate_algorithm"`
	PIDKp             float64           `json:"pid_kp"`
	PIDKi             float64           `json:"pid_ki"`
	PIDKd             float64           `json:"pid_kd"`
	ZipfS             float64           `json:"zipf_s"`
	ZipfV             float64           `json:"zipf_v"`
	ZipfIMax          uint64            `json:"zipf_imax"`
	ChecksumStore     string            `json:"checksum_store"`
	ChecksumPath      string            `json:"checksum_path"`
	VersioningState   string            `json:"versioning_state"`
	RetentionMode     string            `json:"retention_mode"`
	RetentionDuration time.Duration     `json:"retention_duration"`
	LegalHoldStatus   string            `json:"legal_hold_status"`
	BypassGovernance  bool              `json:"bypass_governance"`
	MaxErrorRate      float64           `json:"max_error_rate"`
	MaxLatencyP99     time.Duration     `json:"max_latency_p99"`
	MultipartFail     string            `json:"multipart_on_error"`
}

// defaultConfig returns a Config populated with default values.
func defaultConfig() *Config {
	return &Config{
		Endpoints:        []string{"https://127.0.0.1:18082"},
		Bucket:           "test",
		Operation:        "put",
		Duration:         60 * time.Second,
		Requests:         0,
		Concurrency:      1,
		MaxConcurrency:   0,
		TargetTPS:        1.0,
		ObjectSize:       32 * 1024,
		PartSize:         5 * 1024 * 1024,
		Retries:          0,
		RetryBackoff:     100 * time.Millisecond,
		RetryJitterPct:   0.2,
		HTTPTimeout:      30 * time.Second,
		ConnLifetime:     0,
		ConnReapInterval: 30 * time.Second,
		ShutdownGrace:    10 * time.Second,
		Metadata:         map[string]string{},
		TaggingMap:       map[string]string{},
		KeyPrefix:        "testobject",
		KeyStrategy:      "sequential",
		AddressingStyle:  "path",
		RateAlgorithm:    "token-bucket",
		PIDKp:            0.4,
		PIDKi:            0.05,
		PIDKd:            0.1,
		ZipfS:            1.01,
		ZipfV:            1.0,
		ZipfIMax:         0,
		PayloadVerify:    true,
		ChecksumStore:    "memory",
		MultipartFail:    "abort",
		LogLevel:         "info",
	}
}

// Parse interprets CLI arguments, config files, and environment overrides into a Config.
func Parse(args []string) (*Config, error) {
	cfg := defaultConfig()

	configPath := findConfigPath(args)
	if configPath != "" {
		if err := loadConfigFile(configPath, cfg); err != nil {
			return nil, err
		}
	}

	if err := applyEnvOverrides(cfg); err != nil {
		return nil, err
	}

	fs := flag.NewFlagSet("s3load", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: s3load [flags]\n\nFlags:\n")
		fs.PrintDefaults()
	}

	endpointsFlag := fs.String("endpoint", strings.Join(cfg.Endpoints, ","), "Comma-separated list of S3-compatible endpoints")
	bucketFlag := fs.String("bucket", cfg.Bucket, "Target bucket name")
	operationFlag := fs.String("operation", cfg.Operation, "Operation to execute (put, multipartput, multipartget, get, copy, puttagging, updatemeta, randget, delete, options, head, restore)")
	durationSeconds := int(cfg.Duration / time.Second)
	durationFlag := fs.Int("duration", durationSeconds, "Test duration in seconds")
	requestsFlag := fs.Int("requests", cfg.Requests, "Total number of requests to perform (mutually exclusive with duration)")
	concurrencyFlag := fs.Int("concurrency", cfg.Concurrency, "Initial concurrent requests (starting connection cap)")
	maxConcurrencyFlag := fs.Int("max-concurrency", cfg.MaxConcurrency, "Optional ceiling for dynamic concurrency growth (0 disables)")
	tpsFlag := fs.Float64("tps", cfg.TargetTPS, "Target transactions per second")
	sizeFlag := fs.Int64("size", cfg.ObjectSize, "Object size in bytes for write workloads")
	partSizeFlag := fs.Int64("partsize", cfg.PartSize, "Multipart upload part size in bytes")
	retriesFlag := fs.Int("retries", cfg.Retries, "Number of retry attempts per request")
	retryBackoffFlag := fs.Duration("retry-backoff", cfg.RetryBackoff, "Initial backoff between retries")
	retryJitterFlag := fs.Float64("retry-jitter", cfg.RetryJitterPct, "Retry backoff jitter percentage (0-1 range)")
	accessKeyFlag := fs.String("access-key", cfg.AccessKey, "AWS access key ID")
	secretKeyFlag := fs.String("secret-key", cfg.SecretKey, "AWS secret access key")
	regionFlag := fs.String("region", cfg.Region, "AWS region")
	objectListFlag := fs.String("object-list-file", cfg.ObjectListFile, "Path to newline-separated list of object keys")
	aclFlag := fs.String("acl", cfg.ACL, "Canned ACL for object ACL operations (e.g. private, public-read)")
	bucketACLFlag := fs.String("bucket-acl", cfg.BucketACL, "Canned ACL for bucket ACL operations")
	lifecycleFlag := fs.String("lifecycle-file", cfg.LifecycleFile, "Path to expected lifecycle configuration used by checklifecycle operation")
	workloadFileFlag := fs.String("workload-file", cfg.WorkloadFile, "Path to JSON file describing weighted operations")
	workloadTemplateFlag := fs.String("workload-template", cfg.WorkloadTemplate, "Built-in workload template name")
	listTemplatesFlag := fs.Bool("list-templates", cfg.ListTemplates, "List built-in workload templates and exit")
	describeFlag := fs.Bool("describe", cfg.Describe, "Describe effective configuration and exit")
	dryRunFlag := fs.Bool("dry-run", cfg.DryRun, "Validate configuration without execution")
	keyStrategyFlag := fs.String("key-strategy", cfg.KeyStrategy, "Key selection strategy: sequential, random, zipf")
	prefixesFlag := fs.String("prefixes", strings.Join(cfg.Prefixes, ","), "Comma separated list of key prefixes for synthetic keys")
	metadataFlag := fs.String("metadata", encodeKeyValuePairs(cfg.Metadata), "Object metadata to attach to PUTs (key1=value1&key2=value2)")
	taggingFlag := fs.String("tagging", cfg.Tagging, "Object tagging string to include on PUTs")
	prefixFlag := fs.String("prefix", cfg.KeyPrefix, "Key prefix for generated object names")
	addressingFlag := fs.String("addressing-style", cfg.AddressingStyle, "S3 addressing style: virtual or path")
	httpTimeoutFlag := fs.Duration("http-timeout", cfg.HTTPTimeout, "HTTP client timeout")
	connLifetimeFlag := fs.Duration("conn-lifetime", cfg.ConnLifetime, "Maximum wall-clock lifetime for persistent connections (0 keeps connections indefinitely)")
	connReapIntervalFlag := fs.Duration("conn-reap-interval", cfg.ConnReapInterval, "Interval for closing idle connections; 0 disables")
	shutdownGraceFlag := fs.Duration("shutdown-grace", cfg.ShutdownGrace, "Grace period to drain in-flight operations on shutdown; 0 waits indefinitely")
	metricsAddrFlag := fs.String("metrics-addr", cfg.MetricsAddr, "Address for HTTP metrics/health endpoint (e.g. :9090)")
	jsonReportFlag := fs.String("json-report", cfg.JSONReportPath, "Optional path to write JSON run report")
	logLevelFlag := fs.String("log-level", cfg.LogLevel, "Log level: debug, info, warn, error")
	verifyFlag := fs.Bool("verify-payload", cfg.PayloadVerify, "Enable payload integrity verification for read operations")
	rateAlgorithmFlag := fs.String("rate-algorithm", cfg.RateAlgorithm, "Rate control algorithm: token-bucket or pid")
	pidKpFlag := fs.Float64("pid-kp", cfg.PIDKp, "PID controller proportional gain")
	pidKiFlag := fs.Float64("pid-ki", cfg.PIDKi, "PID controller integral gain")
	pidKdFlag := fs.Float64("pid-kd", cfg.PIDKd, "PID controller derivative gain")
	zipfSFlag := fs.Float64("zipf-s", cfg.ZipfS, "Zipf distribution s parameter (skew)")
	zipfVFlag := fs.Float64("zipf-v", cfg.ZipfV, "Zipf distribution v parameter")
	zipfIMaxFlag := fs.Uint64("zipf-imax", cfg.ZipfIMax, "Zipf distribution maximum key index (0 derives from dataset)")
	checksumStoreFlag := fs.String("checksum-store", cfg.ChecksumStore, "Checksum store backend: memory or filesystem")
	checksumPathFlag := fs.String("checksum-path", cfg.ChecksumPath, "Directory for filesystem checksum store")
	versioningFlag := fs.String("versioning-state", cfg.VersioningState, "Bucket versioning state: enabled or suspended")
	retentionModeFlag := fs.String("retention-mode", cfg.RetentionMode, "Object lock retention mode: governance or compliance")
	retentionDurationFlag := fs.Duration("retention-duration", cfg.RetentionDuration, "Retention duration to add to current time when setting object retention")
	legalHoldFlag := fs.String("legal-hold-status", cfg.LegalHoldStatus, "Legal hold status: on or off")
	bypassGovernanceFlag := fs.Bool("bypass-governance", cfg.BypassGovernance, "Set x-amz-bypass-governance-retention header when updating retention")
	maxErrorRateFlag := fs.Float64("max-error-rate", cfg.MaxErrorRate, "Maximum acceptable error rate (0-1); negative disables")
	maxLatencyP99Flag := fs.Duration("max-latency-p99", cfg.MaxLatencyP99, "Maximum acceptable p99 latency (0 disables)")
	multipartFailFlag := fs.String("multipart-on-error", cfg.MultipartFail, "Behavior when multipart upload fails: abort or resume")
	configFlag := fs.String("config", configPath, "Path to optional JSON configuration file")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if *configFlag != "" && *configFlag != configPath {
		// allow specifying --config after defaults to override pre-scan path
		if err := loadConfigFile(*configFlag, cfg); err != nil {
			return nil, err
		}
		if err := applyEnvOverrides(cfg); err != nil {
			return nil, err
		}
	}

	setFlags := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		setFlags[f.Name] = true
	})

	if setFlags["endpoint"] {
		cfg.Endpoints = splitAndTrim(*endpointsFlag)
	}
	if setFlags["bucket"] {
		cfg.Bucket = *bucketFlag
	}
	if setFlags["operation"] {
		cfg.Operation = strings.ToLower(*operationFlag)
	}
	if setFlags["duration"] {
		cfg.Duration = time.Duration(*durationFlag) * time.Second
	}
	if setFlags["requests"] {
		cfg.Requests = *requestsFlag
	}
	if setFlags["concurrency"] {
		cfg.Concurrency = *concurrencyFlag
	}
	if setFlags["max-concurrency"] {
		cfg.MaxConcurrency = *maxConcurrencyFlag
	}
	if setFlags["tps"] {
		cfg.TargetTPS = *tpsFlag
	}
	if setFlags["size"] {
		cfg.ObjectSize = *sizeFlag
	}
	if setFlags["partsize"] {
		cfg.PartSize = *partSizeFlag
	}
	if setFlags["retries"] {
		cfg.Retries = *retriesFlag
	}
	if setFlags["retry-backoff"] {
		cfg.RetryBackoff = *retryBackoffFlag
	}
	if setFlags["retry-jitter"] {
		cfg.RetryJitterPct = *retryJitterFlag
	}
	if setFlags["access-key"] {
		cfg.AccessKey = *accessKeyFlag
	}
	if setFlags["secret-key"] {
		cfg.SecretKey = *secretKeyFlag
	}
	if setFlags["region"] {
		cfg.Region = *regionFlag
	}
	if setFlags["object-list-file"] {
		cfg.ObjectListFile = *objectListFlag
	}
	if setFlags["acl"] {
		cfg.ACL = *aclFlag
	}
	if setFlags["bucket-acl"] {
		cfg.BucketACL = *bucketACLFlag
	}
	if setFlags["lifecycle-file"] {
		cfg.LifecycleFile = *lifecycleFlag
	}
	if setFlags["workload-file"] {
		cfg.WorkloadFile = *workloadFileFlag
	}
	if setFlags["workload-template"] {
		cfg.WorkloadTemplate = *workloadTemplateFlag
	}
	if setFlags["list-templates"] {
		cfg.ListTemplates = *listTemplatesFlag
	}
	if setFlags["describe"] {
		cfg.Describe = *describeFlag
	}
	if setFlags["dry-run"] {
		cfg.DryRun = *dryRunFlag
	}
	if setFlags["key-strategy"] {
		cfg.KeyStrategy = strings.ToLower(*keyStrategyFlag)
	}
	if setFlags["prefixes"] {
		cfg.Prefixes = splitAndTrim(*prefixesFlag)
	}
	if setFlags["metadata"] {
		if m, err := parseKeyValuePairs(*metadataFlag); err != nil {
			return nil, err
		} else {
			cfg.Metadata = m
		}
	}
	if setFlags["tagging"] {
		cfg.Tagging = *taggingFlag
		if tagMap, err := parseKeyValuePairs(cfg.Tagging); err != nil {
			return nil, err
		} else {
			cfg.TaggingMap = tagMap
		}
	} else if cfg.Tagging != "" && len(cfg.TaggingMap) == 0 {
		tagMap, err := parseKeyValuePairs(cfg.Tagging)
		if err != nil {
			return nil, err
		}
		cfg.TaggingMap = tagMap
	}
	if setFlags["prefix"] {
		cfg.KeyPrefix = *prefixFlag
	}
	if setFlags["addressing-style"] {
		cfg.AddressingStyle = strings.ToLower(*addressingFlag)
	}
	if setFlags["http-timeout"] {
		cfg.HTTPTimeout = *httpTimeoutFlag
	}
	if setFlags["conn-lifetime"] {
		cfg.ConnLifetime = *connLifetimeFlag
	}
	if setFlags["conn-reap-interval"] {
		cfg.ConnReapInterval = *connReapIntervalFlag
	}
	if setFlags["shutdown-grace"] {
		cfg.ShutdownGrace = *shutdownGraceFlag
	}
	if setFlags["metrics-addr"] {
		cfg.MetricsAddr = *metricsAddrFlag
	}
	if setFlags["json-report"] {
		cfg.JSONReportPath = *jsonReportFlag
	}
	if setFlags["log-level"] {
		cfg.LogLevel = strings.ToLower(*logLevelFlag)
	}
	if setFlags["verify-payload"] {
		cfg.PayloadVerify = *verifyFlag
	}
	if setFlags["rate-algorithm"] {
		cfg.RateAlgorithm = strings.ToLower(*rateAlgorithmFlag)
	}
	if setFlags["pid-kp"] {
		cfg.PIDKp = *pidKpFlag
	}
	if setFlags["pid-ki"] {
		cfg.PIDKi = *pidKiFlag
	}
	if setFlags["pid-kd"] {
		cfg.PIDKd = *pidKdFlag
	}
	if setFlags["zipf-s"] {
		cfg.ZipfS = *zipfSFlag
	}
	if setFlags["zipf-v"] {
		cfg.ZipfV = *zipfVFlag
	}
	if setFlags["zipf-imax"] {
		cfg.ZipfIMax = *zipfIMaxFlag
	}
	if setFlags["checksum-store"] {
		cfg.ChecksumStore = strings.ToLower(*checksumStoreFlag)
	}
	if setFlags["checksum-path"] {
		cfg.ChecksumPath = *checksumPathFlag
	}
	if setFlags["versioning-state"] {
		cfg.VersioningState = strings.ToLower(*versioningFlag)
	}
	if setFlags["retention-mode"] {
		cfg.RetentionMode = strings.ToLower(*retentionModeFlag)
	}
	if setFlags["retention-duration"] {
		cfg.RetentionDuration = *retentionDurationFlag
	}
	if setFlags["legal-hold-status"] {
		cfg.LegalHoldStatus = strings.ToLower(*legalHoldFlag)
	}
	if setFlags["bypass-governance"] {
		cfg.BypassGovernance = *bypassGovernanceFlag
	}
	if setFlags["max-error-rate"] {
		cfg.MaxErrorRate = *maxErrorRateFlag
	}
	if setFlags["max-latency-p99"] {
		cfg.MaxLatencyP99 = *maxLatencyP99Flag
	}
	if setFlags["multipart-on-error"] {
		cfg.MultipartFail = strings.ToLower(*multipartFailFlag)
	}

	if cfg.Metadata == nil {
		cfg.Metadata = map[string]string{}
	}
	if cfg.Tagging != "" && len(cfg.TaggingMap) == 0 {
		tagMap, err := parseKeyValuePairs(cfg.Tagging)
		if err != nil {
			return nil, err
		}
		cfg.TaggingMap = tagMap
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Snapshot returns a map representation of the configuration for describing purposes.
func (c *Config) Snapshot() map[string]any {
	metaCopy := make(map[string]string, len(c.Metadata))
	for k, v := range c.Metadata {
		metaCopy[k] = v
	}
	return map[string]any{
		"endpoints":          c.Endpoints,
		"bucket":             c.Bucket,
		"operation":          c.Operation,
		"acl":                c.ACL,
		"bucket_acl":         c.BucketACL,
		"duration":           c.Duration.String(),
		"requests":           c.Requests,
		"dry_run":            c.DryRun,
		"concurrency":        c.Concurrency,
		"max_concurrency":    c.MaxConcurrency,
		"tps":                c.TargetTPS,
		"size":               c.ObjectSize,
		"partsize":           c.PartSize,
		"retries":            c.Retries,
		"retry_backoff":      c.RetryBackoff.String(),
		"retry_jitter":       c.RetryJitterPct,
		"access_key":         c.AccessKey,
		"secret_key":         maskSecret(c.SecretKey),
		"region":             c.Region,
		"object_list_file":   c.ObjectListFile,
		"lifecycle_file":     c.LifecycleFile,
		"workload_file":      c.WorkloadFile,
		"workload_template":  c.WorkloadTemplate,
		"key_strategy":       c.KeyStrategy,
		"prefixes":           c.Prefixes,
		"metadata":           metaCopy,
		"tagging":            c.Tagging,
		"prefix":             c.KeyPrefix,
		"addressing_style":   c.AddressingStyle,
		"http_timeout":       c.HTTPTimeout.String(),
		"conn_lifetime":      c.ConnLifetime.String(),
		"conn_reap_interval": c.ConnReapInterval.String(),
		"shutdown_grace":     c.ShutdownGrace.String(),
		"metrics_addr":       c.MetricsAddr,
		"json_report":        c.JSONReportPath,
		"log_level":          c.LogLevel,
		"verify_payload":     c.PayloadVerify,
		"rate_algorithm":     c.RateAlgorithm,
		"pid_kp":             c.PIDKp,
		"pid_ki":             c.PIDKi,
		"pid_kd":             c.PIDKd,
		"zipf_s":             c.ZipfS,
		"zipf_v":             c.ZipfV,
		"zipf_imax":          c.ZipfIMax,
		"checksum_store":     c.ChecksumStore,
		"checksum_path":      c.ChecksumPath,
		"versioning_state":   c.VersioningState,
		"retention_mode":     c.RetentionMode,
		"retention_duration": c.RetentionDuration.String(),
		"legal_hold_status":  c.LegalHoldStatus,
		"bypass_governance":  c.BypassGovernance,
		"max_error_rate":     c.MaxErrorRate,
		"max_latency_p99":    c.MaxLatencyP99.String(),
		"multipart_on_error": c.MultipartFail,
	}
}

func maskSecret(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= 4 {
		return "****"
	}
	return value[:2] + strings.Repeat("*", len(value)-4) + value[len(value)-2:]
}

func findConfigPath(args []string) string {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--config" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--config=") {
			return strings.TrimPrefix(arg, "--config=")
		}
	}
	return ""
}

func loadConfigFile(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("parse config file: %w", err)
	}
	if err := applyMap(cfg, raw); err != nil {
		return err
	}
	return nil
}

func applyMap(cfg *Config, values map[string]any) error {
	for key, value := range values {
		if err := applyConfigValue(cfg, strings.ToLower(key), value); err != nil {
			return fmt.Errorf("config key %q: %w", key, err)
		}
	}
	return nil
}

func applyEnvOverrides(cfg *Config) error {
	mapping := envKeyMapping()
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, envPrefix) {
			continue
		}
		parts := strings.SplitN(env[len(envPrefix):], "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToUpper(parts[0])
		if mapped, ok := mapping[key]; ok {
			if err := applyConfigValue(cfg, mapped, parts[1]); err != nil {
				return fmt.Errorf("environment %s%s: %w", envPrefix, parts[0], err)
			}
		}
	}
	return nil
}

func envKeyMapping() map[string]string {
	return map[string]string{
		"BUCKET":             "bucket",
		"ENDPOINT":           "endpoints",
		"ENDPOINTS":          "endpoints",
		"OPERATION":          "operation",
		"DURATION":           "duration",
		"REQUESTS":           "requests",
		"CONCURRENCY":        "concurrency",
		"MAX_CONCURRENCY":    "max_concurrency",
		"TPS":                "tps",
		"SIZE":               "size",
		"PARTSIZE":           "partsize",
		"RETRIES":            "retries",
		"RETRY_BACKOFF":      "retry_backoff",
		"RETRY_JITTER":       "retry_jitter",
		"ACCESS_KEY":         "access_key",
		"SECRET_KEY":         "secret_key",
		"REGION":             "region",
		"OBJECT_LIST_FILE":   "object_list_file",
		"LIFECYCLE_FILE":     "lifecycle_file",
		"WORKLOAD_FILE":      "workload_file",
		"WORKLOAD_TEMPLATE":  "workload_template",
		"LIST_TEMPLATES":     "list_templates",
		"DESCRIBE":           "describe",
		"DRY_RUN":            "dry-run",
		"KEY_STRATEGY":       "key_strategy",
		"PREFIXES":           "prefixes",
		"METADATA":           "metadata",
		"TAGGING":            "tagging",
		"PREFIX":             "prefix",
		"KEY_PREFIX":         "prefix",
		"ADDRESSING_STYLE":   "addressing_style",
		"HTTP_TIMEOUT":       "http_timeout",
		"CONN_LIFETIME":      "conn_lifetime",
		"CONN_REAP_INTERVAL": "conn_reap_interval",
		"SHUTDOWN_GRACE":     "shutdown_grace",
		"METRICS_ADDR":       "metrics_addr",
		"JSON_REPORT":        "json_report",
		"LOG_LEVEL":          "log_level",
		"VERIFY_PAYLOAD":     "verify_payload",
		"RATE_ALGORITHM":     "rate_algorithm",
		"PID_KP":             "pid_kp",
		"PID_KI":             "pid_ki",
		"PID_KD":             "pid_kd",
		"ZIPF_S":             "zipf_s",
		"ZIPF_V":             "zipf_v",
		"ZIPF_IMAX":          "zipf_imax",
		"CHECKSUM_STORE":     "checksum_store",
		"CHECKSUM_PATH":      "checksum_path",
		"ACL":                "acl",
		"BUCKET_ACL":         "bucket_acl",
		"VERSIONING_STATE":   "versioning_state",
		"RETENTION_MODE":     "retention_mode",
		"RETENTION_DURATION": "retention_duration",
		"LEGAL_HOLD_STATUS":  "legal_hold_status",
		"BYPASS_GOVERNANCE":  "bypass_governance",
		"MAX_ERROR_RATE":     "max_error_rate",
		"MAX_LATENCY_P99":    "max_latency_p99",
		"MULTIPART_ON_ERROR": "multipart_on_error",
	}
}

func applyConfigValue(cfg *Config, key string, value any) error {
	switch key {
	case "endpoints":
		switch v := value.(type) {
		case []any:
			endpoints := make([]string, 0, len(v))
			for _, item := range v {
				endpoints = append(endpoints, fmt.Sprint(item))
			}
			cfg.Endpoints = endpoints
		default:
			cfg.Endpoints = splitAndTrim(fmt.Sprint(v))
		}
	case "bucket":
		cfg.Bucket = fmt.Sprint(value)
	case "operation":
		cfg.Operation = strings.ToLower(fmt.Sprint(value))
	case "duration":
		if err := setDuration(&cfg.Duration, value); err != nil {
			return err
		}
	case "requests":
		if err := setInt(&cfg.Requests, value); err != nil {
			return err
		}
	case "concurrency":
		if err := setInt(&cfg.Concurrency, value); err != nil {
			return err
		}
	case "max_concurrency":
		if err := setInt(&cfg.MaxConcurrency, value); err != nil {
			return err
		}
	case "tps":
		if err := setFloat(&cfg.TargetTPS, value); err != nil {
			return err
		}
	case "size":
		if err := setInt64(&cfg.ObjectSize, value); err != nil {
			return err
		}
	case "partsize":
		if err := setInt64(&cfg.PartSize, value); err != nil {
			return err
		}
	case "retries":
		if err := setInt(&cfg.Retries, value); err != nil {
			return err
		}
	case "retry_backoff":
		if err := setDuration(&cfg.RetryBackoff, value); err != nil {
			return err
		}
	case "retry_jitter":
		if err := setFloat(&cfg.RetryJitterPct, value); err != nil {
			return err
		}
	case "access_key":
		cfg.AccessKey = fmt.Sprint(value)
	case "secret_key":
		cfg.SecretKey = fmt.Sprint(value)
	case "region":
		cfg.Region = fmt.Sprint(value)
	case "object_list_file":
		cfg.ObjectListFile = fmt.Sprint(value)
	case "lifecycle_file":
		cfg.LifecycleFile = fmt.Sprint(value)
	case "workload_file":
		cfg.WorkloadFile = fmt.Sprint(value)
	case "workload_template":
		cfg.WorkloadTemplate = fmt.Sprint(value)
	case "list_templates":
		if err := setBool(&cfg.ListTemplates, value); err != nil {
			return err
		}
	case "describe":
		if err := setBool(&cfg.Describe, value); err != nil {
			return err
		}
	case "dry_run":
		if err := setBool(&cfg.DryRun, value); err != nil {
			return err
		}
	case "key_strategy":
		cfg.KeyStrategy = strings.ToLower(fmt.Sprint(value))
	case "prefixes":
		switch v := value.(type) {
		case []any:
			prefixes := make([]string, 0, len(v))
			for _, item := range v {
				prefixes = append(prefixes, fmt.Sprint(item))
			}
			cfg.Prefixes = prefixes
		default:
			cfg.Prefixes = splitAndTrim(fmt.Sprint(v))
		}
	case "metadata":
		switch v := value.(type) {
		case map[string]any:
			meta := make(map[string]string, len(v))
			for k, val := range v {
				meta[k] = fmt.Sprint(val)
			}
			cfg.Metadata = meta
		default:
			meta, err := parseKeyValuePairs(fmt.Sprint(v))
			if err != nil {
				return err
			}
			cfg.Metadata = meta
		}
	case "tagging":
		cfg.Tagging = fmt.Sprint(value)
		tagMap, err := parseKeyValuePairs(cfg.Tagging)
		if err != nil {
			return err
		}
		cfg.TaggingMap = tagMap
	case "prefix":
		cfg.KeyPrefix = fmt.Sprint(value)
	case "addressing_style":
		cfg.AddressingStyle = strings.ToLower(fmt.Sprint(value))
	case "http_timeout":
		if err := setDuration(&cfg.HTTPTimeout, value); err != nil {
			return err
		}
	case "conn_lifetime":
		if err := setDuration(&cfg.ConnLifetime, value); err != nil {
			return err
		}
	case "conn_reap_interval":
		if err := setDuration(&cfg.ConnReapInterval, value); err != nil {
			return err
		}
	case "shutdown_grace":
		if err := setDuration(&cfg.ShutdownGrace, value); err != nil {
			return err
		}
	case "metrics_addr":
		cfg.MetricsAddr = fmt.Sprint(value)
	case "json_report":
		cfg.JSONReportPath = fmt.Sprint(value)
	case "log_level":
		cfg.LogLevel = strings.ToLower(fmt.Sprint(value))
	case "verify_payload":
		if err := setBool(&cfg.PayloadVerify, value); err != nil {
			return err
		}
	case "rate_algorithm":
		cfg.RateAlgorithm = strings.ToLower(fmt.Sprint(value))
	case "pid_kp":
		if err := setFloat(&cfg.PIDKp, value); err != nil {
			return err
		}
	case "pid_ki":
		if err := setFloat(&cfg.PIDKi, value); err != nil {
			return err
		}
	case "pid_kd":
		if err := setFloat(&cfg.PIDKd, value); err != nil {
			return err
		}
	case "zipf_s":
		if err := setFloat(&cfg.ZipfS, value); err != nil {
			return err
		}
	case "zipf_v":
		if err := setFloat(&cfg.ZipfV, value); err != nil {
			return err
		}
	case "zipf_imax":
		if err := setUint64(&cfg.ZipfIMax, value); err != nil {
			return err
		}
	case "checksum_store":
		cfg.ChecksumStore = strings.ToLower(fmt.Sprint(value))
	case "checksum_path":
		cfg.ChecksumPath = fmt.Sprint(value)
	case "acl":
		cfg.ACL = fmt.Sprint(value)
	case "bucket_acl":
		cfg.BucketACL = fmt.Sprint(value)
	case "versioning_state":
		cfg.VersioningState = strings.ToLower(fmt.Sprint(value))
	case "retention_mode":
		cfg.RetentionMode = strings.ToLower(fmt.Sprint(value))
	case "retention_duration":
		if err := setDuration(&cfg.RetentionDuration, value); err != nil {
			return err
		}
	case "legal_hold_status":
		cfg.LegalHoldStatus = strings.ToLower(fmt.Sprint(value))
	case "bypass_governance":
		if err := setBool(&cfg.BypassGovernance, value); err != nil {
			return err
		}
	case "max_error_rate":
		if err := setFloat(&cfg.MaxErrorRate, value); err != nil {
			return err
		}
	case "max_latency_p99":
		if err := setDuration(&cfg.MaxLatencyP99, value); err != nil {
			return err
		}
	case "multipart_on_error":
		cfg.MultipartFail = strings.ToLower(fmt.Sprint(value))
	default:
		// unknown keys ignored
	}
	return nil
}

func setDuration(target *time.Duration, value any) error {
	switch v := value.(type) {
	case time.Duration:
		*target = v
	case float64:
		// treat as seconds
		*target = time.Duration(v * float64(time.Second))
	default:
		str := strings.TrimSpace(fmt.Sprint(v))
		if str == "" {
			*target = 0
			return nil
		}
		d, err := time.ParseDuration(str)
		if err != nil {
			return fmt.Errorf("parse duration %q: %w", str, err)
		}
		*target = d
	}
	return nil
}

func setInt(target *int, value any) error {
	switch v := value.(type) {
	case float64:
		*target = int(v)
	default:
		str := strings.TrimSpace(fmt.Sprint(v))
		if str == "" {
			*target = 0
			return nil
		}
		i, err := strconv.Atoi(str)
		if err != nil {
			return fmt.Errorf("parse int %q: %w", str, err)
		}
		*target = i
	}
	return nil
}

func setInt64(target *int64, value any) error {
	switch v := value.(type) {
	case float64:
		*target = int64(v)
	default:
		str := strings.TrimSpace(fmt.Sprint(v))
		if str == "" {
			*target = 0
			return nil
		}
		i, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return fmt.Errorf("parse int64 %q: %w", str, err)
		}
		*target = i
	}
	return nil
}

func setUint64(target *uint64, value any) error {
	switch v := value.(type) {
	case float64:
		*target = uint64(v)
	default:
		str := strings.TrimSpace(fmt.Sprint(v))
		if str == "" {
			*target = 0
			return nil
		}
		i, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return fmt.Errorf("parse uint64 %q: %w", str, err)
		}
		*target = i
	}
	return nil
}

func setFloat(target *float64, value any) error {
	switch v := value.(type) {
	case float64:
		*target = v
	default:
		str := strings.TrimSpace(fmt.Sprint(v))
		if str == "" {
			*target = 0
			return nil
		}
		f, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return fmt.Errorf("parse float %q: %w", str, err)
		}
		*target = f
	}
	return nil
}

func setBool(target *bool, value any) error {
	switch v := value.(type) {
	case bool:
		*target = v
	default:
		str := strings.TrimSpace(fmt.Sprint(v))
		if str == "" {
			*target = false
			return nil
		}
		b, err := strconv.ParseBool(str)
		if err != nil {
			return fmt.Errorf("parse bool %q: %w", str, err)
		}
		*target = b
	}
	return nil
}

func (c *Config) validate() error {
	if len(c.Endpoints) == 0 {
		return errors.New("at least one endpoint must be provided")
	}
	for i, ep := range c.Endpoints {
		if strings.TrimSpace(ep) == "" {
			return fmt.Errorf("endpoint %d is empty", i)
		}
	}
	if c.Bucket == "" {
		return errors.New("bucket must be specified")
	}
	if c.Duration <= 0 && c.Requests <= 0 {
		return errors.New("either duration or requests must be specified")
	}
	if c.Duration > 0 && c.Requests > 0 {
		return errors.New("duration and requests cannot be used together")
	}
	if c.Concurrency <= 0 {
		return errors.New("concurrency must be greater than zero")
	}
	if c.TargetTPS <= 0 {
		return errors.New("tps must be greater than zero")
	}
	if c.MaxConcurrency < 0 {
		return errors.New("max-concurrency cannot be negative")
	}
	if c.MaxConcurrency > 0 && c.MaxConcurrency < c.Concurrency {
		return fmt.Errorf("max-concurrency %d must be greater than or equal to concurrency %d", c.MaxConcurrency, c.Concurrency)
	}
	if c.ObjectSize < 0 {
		return errors.New("size must be non-negative")
	}
	if c.PartSize <= 0 {
		return errors.New("partsize must be greater than zero")
	}
	if c.RetryBackoff < 0 {
		return errors.New("retry-backoff must be non-negative")
	}
	if c.RetryJitterPct < 0 || c.RetryJitterPct > 1 {
		return errors.New("retry-jitter must be in [0,1]")
	}
	switch c.AddressingStyle {
	case "virtual", "path":
	default:
		return fmt.Errorf("unsupported addressing-style %q", c.AddressingStyle)
	}
	switch c.KeyStrategy {
	case "sequential", "random", "zipf":
	default:
		return fmt.Errorf("unknown key-strategy %q", c.KeyStrategy)
	}
	switch c.RateAlgorithm {
	case "token-bucket", "pid":
	default:
		return fmt.Errorf("unknown rate-algorithm %q", c.RateAlgorithm)
	}
	if c.RateAlgorithm == "pid" {
		if c.PIDKp < 0 || c.PIDKi < 0 || c.PIDKd < 0 {
			return errors.New("pid gains must be non-negative")
		}
	}
	switch c.ChecksumStore {
	case "memory", "filesystem":
	default:
		return fmt.Errorf("unsupported checksum-store %q", c.ChecksumStore)
	}
	switch c.MultipartFail {
	case "abort", "resume":
	default:
		return fmt.Errorf("unsupported multipart-on-error %q", c.MultipartFail)
	}
	if c.ACL != "" {
		c.ACL = strings.ToLower(c.ACL)
	}
	if c.BucketACL != "" {
		c.BucketACL = strings.ToLower(c.BucketACL)
	}
	if c.LifecycleFile != "" {
		if _, err := os.Stat(c.LifecycleFile); err != nil {
			return fmt.Errorf("lifecycle-file: %w", err)
		}
	}
	switch c.VersioningState {
	case "", "enabled", "suspended":
	default:
		return fmt.Errorf("unsupported versioning-state %q", c.VersioningState)
	}
	switch c.RetentionMode {
	case "", "governance", "compliance":
	default:
		return fmt.Errorf("unsupported retention-mode %q", c.RetentionMode)
	}
	if c.RetentionMode != "" {
		if c.RetentionDuration <= 0 {
			return errors.New("retention-duration must be greater than zero when retention-mode is set")
		}
	}
	if c.RetentionDuration < 0 {
		return errors.New("retention-duration cannot be negative")
	}
	switch c.LegalHoldStatus {
	case "", "on", "off":
	default:
		return fmt.Errorf("unsupported legal-hold-status %q", c.LegalHoldStatus)
	}
	if c.ChecksumStore == "filesystem" && c.ChecksumPath == "" {
		return errors.New("checksum-path is required for filesystem store")
	}
	if c.MaxErrorRate >= 0 && c.MaxErrorRate > 1 {
		return errors.New("max-error-rate must be between 0 and 1")
	}
	if c.MaxLatencyP99 < 0 {
		return errors.New("max-latency-p99 cannot be negative")
	}
	if c.ConnReapInterval < 0 {
		return errors.New("conn-reap-interval cannot be negative")
	}
	if c.ShutdownGrace < 0 {
		return errors.New("shutdown-grace cannot be negative")
	}
	if c.KeyStrategy == "zipf" {
		if c.ZipfS <= 1.0 {
			return errors.New("zipf-s must be greater than 1.0 to converge")
		}
		if c.ZipfIMax == 0 && c.ObjectListFile == "" {
			c.ZipfIMax = 1 << 20
		}
	}
	if len(c.Endpoints) > 1 {
		if c.Concurrency%len(c.Endpoints) != 0 {
			return fmt.Errorf("concurrency %d must be a multiple of number of endpoints %d", c.Concurrency, len(c.Endpoints))
		}
		if c.MaxConcurrency > 0 && c.MaxConcurrency%len(c.Endpoints) != 0 {
			return fmt.Errorf("max-concurrency %d must be a multiple of number of endpoints %d", c.MaxConcurrency, len(c.Endpoints))
		}
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return errors.New("access-key and secret-key are required")
	}
	if c.WorkloadFile != "" && c.WorkloadTemplate != "" {
		return errors.New("workload-file and workload-template cannot be used together")
	}
	return nil
}

func splitAndTrim(value string) []string {
	if value == "" {
		return nil
	}
	raw := strings.Split(value, ",")
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func encodeKeyValuePairs(values map[string]string) string {
	if len(values) == 0 {
		return ""
	}
	pairs := make([]string, 0, len(values))
	for k, v := range values {
		pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(pairs, "&")
}

func parseKeyValuePairs(value string) (map[string]string, error) {
	result := make(map[string]string)
	if value == "" {
		return result, nil
	}
	pairs := strings.Split(value, "&")
	for _, pair := range pairs {
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("metadata pair %q must be key=value", pair)
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		if key == "" {
			return nil, fmt.Errorf("metadata key in %q is empty", pair)
		}
		result[key] = val
	}
	return result, nil
}
