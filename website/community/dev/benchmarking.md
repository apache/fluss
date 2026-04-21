---
sidebar_position: 3
---

# Benchmarking

Fluss includes a YAML-driven performance testing framework (`fluss-microbench`) for measuring write throughput, lookup latency, scan performance, and resource consumption. It also contains JMH micro-benchmarks for low-level component testing.

## Quick Start

```bash
# List available preset scenarios
./fluss-microbench/fluss-microbench.sh list

# Run a preset (auto-builds on first run)
./fluss-microbench/fluss-microbench.sh run --scenario-file log-append

# Validate a YAML config without running
./fluss-microbench/fluss-microbench.sh validate --scenario-file my-scenario.yaml
```

All workload parameters (records, threads, warmup, duration) are defined in the scenario YAML and cannot be overridden via CLI. This ensures every report is statistically reliable.

## Built-in Presets

| Preset | Description | Table Type |
|--------|-------------|------------|
| `log-append` | Log table append writes + concurrent scan | Log |
| `kv-upsert-get` | KV table upsert + point lookup | PK |
| `kv-agg-mixed` | SUM/MAX/MIN aggregation mixed read/write | Aggregation |
| `kv-agg-listagg` | LISTAGG string concatenation | Aggregation |
| `kv-agg-rbm32` | RoaringBitmap OR aggregation | Aggregation |

## Comparing Results

Each run is stored in a timestamped directory under `.microbench/runs/{scenario}/{timestamp}/`. Symlinks `latest` and `previous` track the two most recent runs.

```bash
# Auto-diff against the previous run of the same scenario
./fluss-microbench/fluss-microbench.sh run --scenario-file log-append --diff-previous

# Diff against a named baseline
./fluss-microbench/fluss-microbench.sh run --scenario-file log-append --diff-baseline v1.0

# Standalone diff between any two refs
./fluss-microbench/fluss-microbench.sh diff --baseline v1.0 --target log-append/latest

# Save a baseline from a run directory
./fluss-microbench/fluss-microbench.sh baseline save --name v1.0 --report-dir .microbench/runs/log-append/latest

# List and delete baselines
./fluss-microbench/fluss-microbench.sh baseline list
./fluss-microbench/fluss-microbench.sh baseline delete --name v1.0
```

## Custom Scenarios

Create a YAML file with the following structure:

```yaml
meta:
  name: my-scenario
  description: "Custom benchmark scenario"

cluster:
  tablet-servers: 1
  jvm-args: ["-Xmx2g", "-Xms2g"]
  config:
    kv.rocksdb.block-cache-size: "256mb"

table:
  name: my_table
  columns:
    - name: id
      type: BIGINT
    - name: value
      type: STRING
    - name: score
      type: DOUBLE
      agg: SUM
  primary-key: [id]
  merge-engine: AGGREGATION
  buckets: 3

data:
  seed: 42
  generators:
    id: { type: sequential, start: 0, end: 1000000 }
    value: { type: random-string, length: 64 }
    score: { type: random-double, min: 0.0, max: 100.0 }

workload:
  - phase: write
    warmup: 10000
    records: 5000000
    threads: 8
  - phase: lookup
    duration: 5m
    threads: 8
    key-range: [0, 1000000]

sampling:
  interval: 1s
  nmt: true

report:
  formats: [json, csv, html]

thresholds:
  write-tps: { min: 100000 }
  p99-ms: { max: 1000 }
  heap-mb: { max: 4096 }
```

Run it with:

```bash
./fluss-microbench/fluss-microbench.sh run --scenario-file my-scenario.yaml
```

## JMH Micro-benchmarks

JMH benchmarks for low-level Fluss components live under `fluss-microbench/src/test/`:

- `ArrowReadableChannelBenchmark` -- Arrow `ReadableByteChannel` implementations
- `ArrowWritableChannelBenchmark` -- Arrow `WritableByteChannel` implementations
- `LogScannerBenchmark` -- Log fetching throughput via Netty

Run them via their `main()` methods in an IDE, or through Maven Surefire.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MICROBENCH_ROOT` | Root directory for all benchmark output | `$PROJECT_ROOT/.microbench` |

The launcher script reads `MICROBENCH_ROOT` and passes it as `-Dmicrobench.root` to the JVM. You can also set the system property directly when running via Maven or an IDE.
