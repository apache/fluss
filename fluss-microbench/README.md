# Fluss Microbench

YAML-driven performance testing framework for Fluss, with support for multiple workload types, resource sampling, multi-format report generation, and baseline comparison. Also includes JMH micro-benchmarks for low-level component testing.

## Quick Start

```bash
# List available presets
./fluss-microbench/fluss-microbench.sh list

# Run the log-append preset (auto-builds on first run)
./fluss-microbench/fluss-microbench.sh run --scenario-file log-append

# Run a custom YAML scenario
./fluss-microbench/fluss-microbench.sh run --scenario-file my-scenario.yaml

# Validate a configuration
./fluss-microbench/fluss-microbench.sh validate --scenario-file my-scenario.yaml
```

All workload parameters (records, threads, warmup, duration) are defined in the scenario YAML and cannot be overridden via CLI. This ensures every report is statistically reliable.

### Manual Build (Optional)

The launcher script auto-detects and builds when needed. To build manually:

```bash
mvn install -pl fluss-microbench -am -DskipTests -Drat.skip=true
```

## Built-in Preset Scenarios

| Preset | Description | Table Type | Focus |
|--------|-------------|------------|-------|
| `log-append` | Log table append writes + concurrent scan | Log | Write throughput, scan performance |
| `kv-upsert-get` | KV table upsert + point lookup | PK | RocksDB write/compaction |
| `kv-agg-mixed` | SUM/MAX/MIN aggregation mixed read/write | Aggregation | Aggregation compute, read/write contention |
| `kv-agg-listagg` | LISTAGG string concatenation | Aggregation | Memory growth, GC pressure |
| `kv-agg-rbm32` | RoaringBitmap OR aggregation | Aggregation | Bitmap expansion, compaction |

## YAML Configuration

```yaml
meta:
  name: my-scenario
  description: "Scenario description"

cluster:
  tablet-servers: 1                # Number of TabletServers
  jvm-args: ["-Xmx2g", "-Xms2g"]  # Server JVM arguments
  config:                          # Fluss server config overrides
    kv.rocksdb.block-cache-size: "256mb"

client:
  config:                          # Fluss client config overrides
    client.writer.buffer.memory-size: "256mb"
    client.writer.batch-size: "2mb"

table:
  name: my_table
  columns:
    - name: id
      type: BIGINT
    - name: value
      type: STRING
    - name: score
      type: DOUBLE
      agg: SUM                     # Aggregation function (requires merge-engine: AGGREGATION)
  primary-key: [id]                # Primary key (optional; omit for Log table)
  merge-engine: AGGREGATION        # Merge engine (optional)
  buckets: 3                       # Number of buckets

data:
  seed: 42                         # Random seed (optional, for reproducibility)
  generators:                      # Per-column data generators
    id:
      type: sequential
      start: 0
      end: 1000000
    value:
      type: random-string
      length: 64
    score:
      type: random-double
      min: 0.0
      max: 100.0

workload:                          # Workload phases (executed in order)
  - phase: write
    warmup: 10000                  # Warmup records (excluded from stats)
    records: 5000000               # Total records (>= 100,000)
    threads: 8
  - phase: lookup
    duration: 5m                   # Duration (>= 1min)
    threads: 8
    key-range: [0, 1000000]        # Lookup key range
  - phase: mixed
    duration: 3m
    threads: 8
    mix:                           # Mix ratios (percentages, must sum to 100)
      write: 50
      lookup: 50

sampling:
  interval: 1s                     # Sampling interval
  nmt: true                        # Enable Native Memory Tracking
  nmtInterval: 5s                  # NMT sampling interval (independent of main sampling)
  jfr: true                        # Enable JFR recording

report:
  formats: [json, csv, html]       # Report formats

thresholds:                        # Performance thresholds (optional, for regression detection)
  write-tps:
    min: 100000
  p99-ms:
    max: 1000
  heap-mb:
    max: 4096
  rss-peak-mb:
    max: 8000
```

## Workload Types

| Type | Description | Required Config |
|------|-------------|-----------------|
| `write` | Write (upsert for PK tables, append for Log tables) | `records` or `duration` |
| `lookup` | Point lookup (requires PK table) | `duration`, `key-range` |
| `prefix-lookup` | Prefix lookup (requires PK table) | `duration`, `key-prefix-length` |
| `scan` | Full scan (Log table) | `duration` |
| `mixed` | Mixed workload | `duration`, `mix` percentages |

## Data Generators

| Type | Description | Key Parameters |
|------|-------------|----------------|
| `sequential` | Sequential integers | `start`, `end` |
| `random-int` | Random integers | `min`, `max` |
| `random-long` | Random longs | `min`, `max` |
| `random-float` | Random floats | `min`, `max` |
| `random-double` | Random doubles | `min`, `max` |
| `random-boolean` | Random booleans | `true-ratio` |
| `random-string` | Random strings | `length` or `{min, max}` |
| `random-bytes` | Random byte arrays | `length` |
| `timestamp-now` | Current timestamp | — |
| `enum` | Enumerated values | `values: [a, b, c]` |
| `roaring-bitmap-32` | 32-bit RoaringBitmap | `size`, `range`, `overlap` |
| `roaring-bitmap-64` | 64-bit RoaringBitmap | `size`, `range`, `overlap` |

## CLI Subcommands

```bash
# Run a benchmark
./fluss-microbench/fluss-microbench.sh run --scenario-file <file|preset> [options]

# Validate a YAML config
./fluss-microbench/fluss-microbench.sh validate --scenario-file <file|preset>

# Compare two reports
./fluss-microbench/fluss-microbench.sh diff --baseline <ref1> --target <ref2>

# Baseline management
./fluss-microbench/fluss-microbench.sh baseline save --name <name> --report-dir <dir>
./fluss-microbench/fluss-microbench.sh baseline list
./fluss-microbench/fluss-microbench.sh baseline delete --name <name>

# List presets
./fluss-microbench/fluss-microbench.sh list

# Generate a dataset file
./fluss-microbench/fluss-microbench.sh generate --scenario-file <file> --output <path>

# Clean output
./fluss-microbench/fluss-microbench.sh clean
./fluss-microbench/fluss-microbench.sh clean --runs          # Clean runs only
./fluss-microbench/fluss-microbench.sh clean --before 7d     # Clean items older than 7 days
```

## Output Directory Layout

All output is stored under a single root directory (default: `.microbench/` in the project root). Each run creates a timestamped subdirectory under `runs/{scenario}/`:

```
.microbench/
  runs/
    log-append/
      20260407-143022/          # Timestamped run directory
        summary.json            # Structured report
        timeseries.csv          # Time-series sampling data
        report.html             # Visual report (if html format enabled)
        config-snapshot.yaml    # Full YAML config snapshot
        run.log                 # Combined stderr log
        logs/
          server-0.log          # Forked server stdout/stderr
      latest -> 20260407-143022 # Symlink to most recent run
      previous -> 20260407-120000 # Symlink to prior run
  baselines/
    v1.0/                       # Named baseline snapshots
      summary.json
      timeseries.csv
      config-snapshot.yaml
  datasets/                     # Pre-generated dataset files
```

### Comparing Runs

```bash
# Auto-diff against the previous run of the same scenario
./fluss-microbench/fluss-microbench.sh run --scenario-file log-append --diff-previous

# Diff against a named baseline after a run
./fluss-microbench/fluss-microbench.sh run --scenario-file log-append --diff-baseline v1.0

# Standalone diff between any two refs (baseline name, run path, or summary.json)
./fluss-microbench/fluss-microbench.sh diff --baseline v1.0 --target log-append/latest
```

### MICROBENCH_ROOT

The output root defaults to `$PROJECT_ROOT/.microbench`. Override it via:

- **Environment variable**: `export MICROBENCH_ROOT=/tmp/my-bench-output`
- **System property**: `-Dmicrobench.root=/tmp/my-bench-output`

The launcher script (`fluss-microbench.sh`) reads `MICROBENCH_ROOT` and passes it as `-Dmicrobench.root`.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                     PerfRunner (CLI)                     │
│  run | validate | diff | baseline | list | generate     │
└──────────────────────┬──────────────────────────────────┘
                       │
              ┌────────▼────────┐
              │ PerfConfig      │  YAML parsing + validation
              │ ScenarioConfig  │  Scenario config model
              │ ScenarioValidator│ Semantic validation
              └────────┬────────┘
                       │
              ┌────────▼────────┐
              │ YamlDrivenEngine│  Core orchestrator
              │                 │  Cluster → Table → Execute → Sample → Report
              └──┬────┬────┬──┘
                 │    │    │
    ┌────────────┘    │    └────────────┐
    ▼                 ▼                 ▼
┌──────────┐  ┌──────────────┐  ┌──────────────┐
│ Cluster  │  │   Executors  │  │  Sampling    │
│          │  │              │  │              │
│ Forked   │  │ Write        │  │ OSHI (CPU/   │
│ Cluster  │  │ Lookup       │  │   RSS/Disk)  │
│ Manager  │  │ PrefixLookup │  │ JVM MXBean   │
│          │  │ Scan         │  │ NMT (jcmd)   │
│ Cluster  │  │ Mixed        │  │ JFR          │
│ Bootstrap│  │              │  │ FlussMetric  │
│ (child   │  │ ExecutorUtils│  │   Sampler    │
│  process)│  └──────┬───────┘  └──────────────┘
└──────────┘         │
              ┌──────▼───────┐
              │    Stats     │
              │              │
              │ Latency      │  Reservoir Sampling + CAS
              │  Recorder    │  (p50/p95/p99/p999/max)
              │ Throughput   │  AtomicLong counters
              │  Counter     │  (ops/s, bytes/s)
              └──────┬───────┘
                     │
              ┌──────▼───────┐
              │   Report     │
              │              │
              │ PerfReport   │  Data model (Builder)
              │ ReportWriter │  JSON / CSV / HTML
              │ ReportDiffer │  Baseline comparison
              │ MetricClassifier│ Metric grouping
              └──────────────┘
```

### Execution Modes

- **Forked** (default): Forks a separate child JVM for the Fluss cluster; the client runs in the parent process. Supports NMT/JFR sampling.
- **Embedded**: Cluster and client run in the same JVM; only client metrics are sampled.
- **External**: Connects to an existing cluster via `--bootstrap-servers`.

### Async Write Model

Writes use the same async pipeline model as the Flink Connector:
- `write(row)` returns a `CompletableFuture` (non-blocking)
- `whenComplete()` callback records latency and throughput
- Backpressure propagates naturally through the `RecordAccumulator` buffer
- Throughput degrades gracefully during write stalls

### Async Lookup Model

Lookups use the same async pipeline model as Flink's `AsyncLookupFunction`:
- `Semaphore(256)` limits concurrent in-flight requests
- `whenComplete()` callback records latency
- Lookup batches are eagerly filled to avoid batch-timeout idle waits

## JMH Benchmarks

This module also contains JMH micro-benchmarks for low-level Fluss components (under `src/test/`):

- `ArrowReadableChannelBenchmark` — Compares Arrow `ReadableByteChannel` implementations (ByteBuffer vs InputStream)
- `ArrowWritableChannelBenchmark` — Compares Arrow `WritableByteChannel` implementations (MemorySegment vs OutputStream)
- `LogScannerBenchmark` — Measures log fetching throughput via Netty

Run JMH benchmarks via their `main()` methods in an IDE, or through Maven Surefire.

## Notes

- All workload phases require `records` >= 100,000 or `duration` >= 1 minute
- Server temp data directories are automatically cleaned up after tests (via `FlussClusterExtension.close()`)
- Child process crashes are handled via shutdown hooks
- Config values are passed using semicolon-separated pairs (`key=value;key=value`), supporting commas within values
