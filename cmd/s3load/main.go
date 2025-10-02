package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	sigv4 "s3load/internal/aws/sigv4"
	"s3load/internal/config"
	"s3load/internal/controller"
	"s3load/internal/keyspace"
	"s3load/internal/logging"
	"s3load/internal/metrics"
	"s3load/internal/payload"
	"s3load/internal/rate"
	"s3load/internal/s3client"
	"s3load/internal/stats"
	"s3load/internal/worker"
	"s3load/internal/workload"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(2)
	}

	if cfg.ListTemplates {
		for _, name := range workload.BuiltInTemplates() {
			fmt.Println(name)
		}
		return
	}

	if cfg.Describe {
		desc := cfg.Snapshot()
		data, err := json.MarshalIndent(desc, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "describe error: %v\n", err)
			os.Exit(2)
		}
		fmt.Println(string(data))
		return
	}

	logger := logging.New(cfg.LogLevel)

	collector := stats.NewCollector()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	keyGen, err := keyspace.New(keyspace.Options{
		ObjectListFile: cfg.ObjectListFile,
		Prefix:         cfg.KeyPrefix,
		Prefixes:       cfg.Prefixes,
		RandomStart:    false,
		Strategy:       cfg.KeyStrategy,
		ZipfS:          cfg.ZipfS,
		ZipfV:          cfg.ZipfV,
		ZipfIMax:       cfg.ZipfIMax,
	})
	if err != nil {
		logger.Error("failed to build key generator", "error", err)
		os.Exit(2)
	}

	var (
		checksumStore payload.Store
		cleanups      []func()
	)
	switch cfg.ChecksumStore {
	case "filesystem":
		checksumStore, err = payload.NewFileStore(cfg.ChecksumPath)
		if err != nil {
			logger.Error("failed to initialise checksum store", "error", err)
			os.Exit(2)
		}
	default:
		checksumStore = payload.NewMemoryStore()
	}
	cleanups = append(cleanups, func() {
		checksumStore.Close()
	})
	runCleanups := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	if cfg.DryRun {
		desc := cfg.Snapshot()
		data, err := json.MarshalIndent(desc, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "dry-run describe error: %v\n", err)
			runCleanups()
			os.Exit(2)
		}
		fmt.Println(string(data))
		runCleanups()
		return
	}

	var workloadSelector *workload.Selector
	if cfg.WorkloadTemplate != "" {
		templateData, err := workload.LoadTemplate(cfg.WorkloadTemplate)
		if err != nil {
			logger.Error("failed to load workload template", "template", cfg.WorkloadTemplate, "error", err)
			runCleanups()
			os.Exit(2)
		}
		workloadSelector, err = workload.FromJSON(templateData)
		if err != nil {
			logger.Error("failed to parse workload template", "template", cfg.WorkloadTemplate, "error", err)
			runCleanups()
			os.Exit(2)
		}
	} else if cfg.WorkloadFile != "" {
		workloadSelector, err = workload.Load(cfg.WorkloadFile)
		if err != nil {
			logger.Error("failed to load workload file", "error", err)
			runCleanups()
			os.Exit(2)
		}
	}

	var limiter rate.Limiter
	switch cfg.RateAlgorithm {
	case "pid":
		limiter = rate.NewPIDLimiter(cfg.TargetTPS, cfg.PIDKp, cfg.PIDKi, cfg.PIDKd)
	default:
		limiter = rate.NewTokenBucket(cfg.TargetTPS)
	}

	addressing := s3client.PathStyle
	if cfg.AddressingStyle == "virtual" {
		addressing = s3client.VirtualHostStyle
	}

	client, err := s3client.New(s3client.Options{
		Endpoints:    cfg.Endpoints,
		Bucket:       cfg.Bucket,
		Concurrency:  cfg.Concurrency,
		Timeout:      cfg.HTTPTimeout,
		ConnLifetime: cfg.ConnLifetime,
		Credentials: sigv4.Credentials{
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
		},
		Region:       cfg.Region,
		Addressing:   addressing,
		ReapInterval: cfg.ConnReapInterval,
	})
	if err != nil {
		logger.Error("failed to create S3 client", "error", err)
		runCleanups()
		os.Exit(2)
	}
	cleanups = append(cleanups, func() {
		client.Close()
	})

	runner := worker.NewRunner(client, cfg, checksumStore, logger)
	ctrl := controller.New(cfg, logger, keyGen, runner, collector, limiter, workloadSelector)

	var metricsServer *metrics.Server
	if cfg.MetricsAddr != "" {
		metricsServer = metrics.NewServer(cfg.MetricsAddr, collector)
		metricsServer.Start()
		cleanups = append(cleanups, func() {
			metricsServer.Stop(context.Background())
		})
	}

	start := time.Now()
	snapshot := ctrl.Run(ctx)
	end := time.Now()

	logger.Info("run finished", "total_requests", snapshot.TotalRequests, "success", snapshot.SuccessRequests, "failed", snapshot.FailedRequests)

	errorRate := 0.0
	if snapshot.TotalRequests > 0 {
		errorRate = float64(snapshot.FailedRequests) / float64(snapshot.TotalRequests)
	}
	p99 := snapshot.Percentiles["p99"]
	pass := true
	var violations []string
	if cfg.MaxErrorRate >= 0 && errorRate > cfg.MaxErrorRate {
		pass = false
		violations = append(violations, fmt.Sprintf("error-rate %.4f > %.4f", errorRate, cfg.MaxErrorRate))
	}
	if cfg.MaxLatencyP99 > 0 && p99 > cfg.MaxLatencyP99 {
		pass = false
		violations = append(violations, fmt.Sprintf("p99 %s > %s", p99, cfg.MaxLatencyP99))
	}

	summary := map[string]any{
		"pass":            pass,
		"total_requests":  snapshot.TotalRequests,
		"success":         snapshot.SuccessRequests,
		"failed":          snapshot.FailedRequests,
		"error_rate":      errorRate,
		"p99_latency":     p99,
		"actual_tps":      snapshot.ActualTPS,
		"target_tps":      snapshot.TargetTPS,
		"violations":      violations,
		"max_error_rate":  cfg.MaxErrorRate,
		"max_latency_p99": cfg.MaxLatencyP99,
	}

	if pass {
		logger.Info("success criteria met", "error_rate", errorRate, "p99", p99, "actual_tps", snapshot.ActualTPS)
	} else {
		logger.Warn("success criteria violated", "violations", violations, "error_rate", errorRate, "p99", p99, "actual_tps", snapshot.ActualTPS)
	}

	report := map[string]any{
		"config": map[string]any{
			"bucket":       cfg.Bucket,
			"operation":    cfg.Operation,
			"duration_sec": cfg.Duration.Seconds(),
			"tps":          cfg.TargetTPS,
			"concurrency":  cfg.Concurrency,
			"size":         cfg.ObjectSize,
			"endpoints":    cfg.Endpoints,
		},
		"run": map[string]any{
			"started_at":  start.Format(time.RFC3339Nano),
			"finished_at": end.Format(time.RFC3339Nano),
		},
		"metrics": snapshot,
	}
	report["summary"] = summary

	if err := emitReport(cfg.JSONReportPath, report); err != nil {
		logger.Error("failed to write report", "error", err)
	}

	if !pass {
		runCleanups()
		os.Exit(1)
	}

	runCleanups()
}

func emitReport(path string, report any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if path == "" {
		return encoder.Encode(report)
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	fileEncoder := json.NewEncoder(file)
	fileEncoder.SetIndent("", "  ")
	return fileEncoder.Encode(report)
}
