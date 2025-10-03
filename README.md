# s3load

s3load (a.k.a. putgetdaily) is a high-throughput load generator for S3-compatible
object stores. It focuses on reproducible PUT/GET stress tests, configurable key
generation, and detailed post-run metrics so you can spot regressions in daily
checks or benchmarking pipelines.

---

## Features

- **Dynamic concurrency** – start with `--concurrency` and let the scheduler grow
  up to `--max-concurrency` when a backlog builds (covering burst scenarios where
  responses are delayed).
- **Flexible operations** – drive PUT, GET, multipart upload/download, delete,
  HEAD, copy, metadata updates, tagging, and ranged reads from a single binary.
- **Weighted workloads** – mix operations with `--workload-file` or
  `--workload-template`; switch to deterministic key lists via
  `--object-list-file`.
- **Deterministic payloads** – optional checksum store (in-memory or filesystem)
  now tracks SHA-256, MD5, and CRC32C so GET, LIST, and multipart reads can
  automatically verify payload integrity without external dependencies.
- **Rate control** – token-bucket (default) or PID modes target a steady TPS
  while adapting to observed throughput.
- **Observability** – Prometheus-style metrics endpoint, per-second summaries,
  JSON run reports, and in-process logging with structured context.

---

## Prerequisites

- Go 1.25 or newer.
- Network access to your S3-compatible endpoint.
- Credentials provided either via environment (`S3LOAD_ACCESS_KEY`,
  `S3LOAD_SECRET_KEY`, `S3LOAD_REGION`) or CLI flags.

---

## Installation

```bash
# Clone the repo (if you have not already)
git clone https://github.com/<your-org>/putgetdaily.git
cd putgetdaily

# Build a local binary
go build -o bin/s3load ./cmd/s3load

# Or install into your GOPATH/bin
go install ./cmd/s3load

# You can also run directly without installing
go run ./cmd/s3load --help
```

---

## Quick Start

The example below issues 1,000 PUTs per second against `my-bucket`, allowing the
controller to scale up to 2,000 concurrent connections if responses stall.

```bash
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
AWS_REGION=us-east-1 \
go run ./cmd/s3load \
  --endpoint https://s3.amazonaws.com \
  --bucket my-bucket \
  --operation put \
  --size 1048576 \
  --concurrency 1000 \
  --max-concurrency 2000 \
  --tps 1000 \
  --duration 60 \
    --json-report run.json
```

Use `--requests <N>` instead of `--duration` to issue a fixed number of calls.

---

## Core Flags & Environment Variables

s3load reads configuration from (in priority order) CLI flags, environment
variables prefixed with `S3LOAD_`, and optional JSON config files. Key flags:

| Flag | Environment | Description |
| ---- | ----------- | ----------- |
| `--endpoint` | `S3LOAD_ENDPOINT` | Comma-separated list of S3 endpoints. |
| `--bucket` | `S3LOAD_BUCKET` | Target bucket name. |
| `--operation` | `S3LOAD_OPERATION` | Operation to run (`put`, `get`, `multipart`, `delete`, etc.). |
| `--duration` / `--requests` | `S3LOAD_DURATION`, `S3LOAD_REQUESTS` | Run length (seconds) or fixed request budget. |
| `--concurrency` | `S3LOAD_CONCURRENCY` | Initial concurrent requests / connection cap. |
| `--max-concurrency` | `S3LOAD_MAX_CONCURRENCY` | Optional ceiling for dynamic concurrency growth (`0` disables). |
| `--tps` | `S3LOAD_TPS` | Target transactions per second. |
| `--size` | `S3LOAD_SIZE` | Object size in bytes for PUT workloads. |
| `--object-list-file` | `S3LOAD_OBJECT_LIST_FILE` | Newline-separated list of keys to reuse. |
| `--lifecycle-file` | `S3LOAD_LIFECYCLE_FILE` | Expected lifecycle XML used by `checklifecycle`. |
| `--workload-file` | `S3LOAD_WORKLOAD_FILE` | JSON definition of weighted operations. |
| `--workload-template` | `S3LOAD_WORKLOAD_TEMPLATE` | Built-in workload name (`--list-templates` to view choices). |
| `--metrics-addr` | `S3LOAD_METRICS_ADDR` | Expose Prometheus metrics on the given address (e.g. `:9090`). |
| `--json-report` | `S3LOAD_JSON_REPORT` | Write the final report to a file in addition to stdout. |
| `--rate-algorithm` | `S3LOAD_RATE_ALGORITHM` | `token-bucket` (default) or `pid` for adaptive control. |
| `--conn-lifetime` | `S3LOAD_CONN_LIFETIME` | Maximum wall-clock lifetime for each TCP connection; closes sockets after the duration even if they stay busy. |
| `--acl` | `S3LOAD_ACL` | Default canned ACL for `objectacl`. |
| `--bucket-acl` | `S3LOAD_BUCKET_ACL` | Default canned ACL for `bucketacl`. |
| `--versioning-state` | `S3LOAD_VERSIONING_STATE` | Bucket versioning state (`enabled` or `suspended`). |
| `--retention-mode` | `S3LOAD_RETENTION_MODE` | Object lock retention mode (`governance` or `compliance`). |
| `--retention-duration` | `S3LOAD_RETENTION_DURATION` | Duration added to now when setting retention (e.g. `720h`). |
| `--legal-hold-status` | `S3LOAD_LEGAL_HOLD_STATUS` | Legal hold status (`on` or `off`). |
| `--bypass-governance` | `S3LOAD_BYPASS_GOVERNANCE` | Add `x-amz-bypass-governance-retention` when setting retention. |

See `go run ./cmd/s3load --help` for the full flag list, including retry
configuration, keyspace strategies (`sequential`, `random`, `zipf`), and
multipart tuning.

---

## Workloads & Keyspace

- **Synthetic keys** – generated from `--prefix`/`--prefixes`, using sequential,
  random, or Zipf distributions.
- **Object lists** – supply existing keys via `--object-list-file` (see
  `objects.txt` for an example format).
- **Weighted operations** – define mixes like 70% GET / 30% PUT with
  `--workload-file` JSON. Built-in templates can be listed via
  `--list-templates` and include:
  - `smoke` (simple PUT-only sanity check)
  - `put-get` (balanced PUT/GET sampler)
  - `hot-read` (heavy on GETs, small percentage of random range reads)
  - `cold-read` (simulates long-tail reads with Zipf key selection)
  - `overwrite-heavy` (overwrites and metadata/tag updates on existing keys)
  Load one with `--workload-template <name>`.
- **Compliance operations** – trigger canned operations such as `objectacl`,
  `bucketacl`, `checklifecycle`, `bucketversioning`, `listversions`,
  `setretention`, and `setlegalhold` to validate governance workflows. Combine
  them in a workload mix or drive them directly with `--operation`.

---

## Dynamic Concurrency Behaviour

Each scheduling tick compares the active inflight operations with the tokens
granted by the rate limiter:

- If there is backlog (Scenario 1) the controller raises the current limit (up to
  `--max-concurrency`) and instructs the S3 client to open more connections so new
  requests launch immediately.
- If partial responses arrive (Scenario 2) completed workers are re-used first;
  new connections only open when needed, keeping total sockets bounded by the
  latest limit.

This adaptive behaviour ensures you can sustain your target TPS even when the
service under test is slow to respond, without permanently overprovisioning the
initial concurrency.

---

## Metrics & Reporting

- Logs: structured JSON (level, key, operation, latency, etc.).
- Metrics: enable with `--metrics-addr` to scrape Prometheus counters, rates,
  backpressure indicators, and open connection counts.
- Reports: stdout always prints a structured JSON summary; add
  `--json-report <path>` to archive it.
- Success criteria: set `--max-error-rate` and/or `--max-latency-p99` so the run
  exits non-zero when SLAs are violated.

---

## Development

```bash
# Lint/format (Go will tidy imports for you)
gofmt -w ./internal ./cmd ./integration

# Run unit tests
go test ./...
```

The repo uses standard Go tooling—no extra build steps required. See
`docs/design.md` for architectural notes.

---

## License

Add your preferred license information here.
