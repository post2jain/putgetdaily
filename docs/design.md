# S3 Load Tool Architecture

## Overview
The tool generates and measures S3 workloads against any S3-compatible endpoint. It drives the desired transactions per second (TPS) while respecting maximum concurrency (total open TCP sockets) and request duration. The binary exposes structured JSON logging, JSON run reports, and an HTTP metrics/health server.

## Executable layout
- `cmd/s3load/main.go`: Entrypoint; initializes logging, parses CLI flags, loads object lists, wires dependencies, and starts the controller.
- `internal/config`: Flag parsing, validation, and configuration struct. CLI-only inputs for v1.
- `internal/logging`: Structured JSON logger factory (Zap).
- `internal/metrics`: Prometheus registry, collectors, and HTTP server for metrics & liveness.
- `internal/keyspace`: Key generation strategies and object list loader with round-robin iteration.
- `internal/payload`: Payload generators and integrity verification helpers.
- `internal/s3client`: Thin wrapper around AWS SDK v2 S3 client + custom HTTP transport (SigV4 auth).
- `internal/worker`: Per-operation execution logic, retries/backoff, payload validation.
- `internal/controller`: Dynamic concurrency/TPS orchestrator, connection lifetime management, run lifecycle, JSON result export.
- `internal/stats`: Aggregated counters, latency histograms, run summary marshaling.
- `internal/util`: Shared helpers (time, math, pool).

## Core workflows
1. **Configuration**: Parse CLI flags (operation, bucket, duration, tps, concurrency, size, retries, backoff, endpoint, credentials, object list file, metrics addr, conn lifetime, log detail). Validate combinations (e.g., duration vs requests, concurrency > 0, endpoint format).
2. **Initialization**:
   - Create logger (Zap JSON) and metrics registry.
   - Load object list file if provided; fall back to deterministic key generator.
   - Build HTTP transport honoring `concurrency` as `MaxConnsPerHost`/`MaxIdleConns`, with TTL-managed connections (custom `TimedConn`).
   - Instantiate AWS SDK v2 S3 client with SigV4 credentials (access key/secret via flags or env) and optional region.
3. **Controller**:
   - Compute request budget (`tps * duration`).
   - Maintain rolling TPS measurement (per second buckets).
   - Launch worker goroutines up to `concurrency`; scheduler decides when to issue new requests.
   - Scenario handling: if prior second requests still pending, open new ones up to `concurrency` limit; reuse completed connections otherwise.
   - Ensure total live sockets ≤ max concurrency via custom dialer tracking.
   - Respect `--conn-lifetime`: connection reaper closes aged idles.
4. **Workers**:
   - Receive operations to perform (put/multipartput/get/etc).
   - Generate/lookup keys, build payloads (size parameter), optional integrity tag (hash stored alongside key map; for reads validate against stored hash).
   - Use retry strategy (max attempts, backoff policy) on transient failures.
   - Emit per-request metrics: latency, result codes, bytes transferred.
5. **Metrics & Reporting**:
   - Prometheus counters/gauges/histograms for TPS, latency, errors, concurrency.
   - Health endpoint exposing readiness + basic stats.
   - Structured JSON logs for events and per-run summary.
   - At completion, emit JSON report (to stdout or optional file) with aggregated metrics, percentiles, error breakdown.

## Concurrency & TPS control
- `TargetTPS` controller maintains per-second scheduling windows.
- Base concurrency comes from `--concurrency`; the controller now expands the cap when backlog persists, enabling burst growth without manual tuning.
- Optional `--max-concurrency` clamps the dynamic ceiling to protect clusters from runaway connection counts.
- Connection accounting lives in the dialer; limits can be raised at runtime while keeping visibility into open sockets.

## Payload integrity
- For write operations, generate deterministic payload (e.g., repeating pattern hashed). Maintain in-memory map of key → checksum (bounded or persistent?). For large runs, optionally stream checksum metadata to disk to avoid growth.
- For reads, recompute checksum after body download and compare.

## Extensibility considerations
- Operation handlers implemented via interface to ease future operations.
- Controller designed to plug in additional rate algorithms (PID-style, token bucket, etc.).
- Metrics module exposing hooks for custom collectors; logging centralised for consistent structure.

## Testing strategy
- Unit tests for config validation, key generation, payload hashing, retry logic.
- Integration test harness using localstack/minio (future network access). For now, introduce interface for S3 client to allow mocking.
- Deterministic tests for TPS scheduler with fake clock.
