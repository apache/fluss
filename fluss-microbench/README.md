# Fluss Microbench

This module runs repeatable Fluss workloads from YAML scenarios against an independently started cluster. It executes phases in order and writes a JSON result with throughput and latency for each phase.

## Run

```bash
./fluss-microbench/fluss-microbench.sh list
./fluss-microbench/fluss-microbench.sh validate --scenario-file kv-upsert-get
./fluss-dist/target/fluss-*-bin/fluss-*/bin/local-cluster.sh start
./fluss-microbench/fluss-microbench.sh run --scenario-file kv-upsert-get --bootstrap-servers localhost:9123
./fluss-microbench/fluss-microbench.sh run --scenario-file my-scenario.yaml --bootstrap-servers localhost:9123
./fluss-dist/target/fluss-*-bin/fluss-*/bin/local-cluster.sh stop
```

Build the distribution with `./mvnw -pl fluss-dist -am package -DskipTests` before starting the local cluster. The launcher builds the benchmark module on first use. Results are written to `.microbench/runs/<scenario>/<timestamp>/summary.json`; `config-snapshot.yaml` records the input. Set `MICROBENCH_ROOT` to change the output root. Workload parameters are read from YAML.

The client writer buffer defaults to 512 MB and must exceed 256 MB when set in YAML. It uses JVM heap. The launcher defaults to a 2 GB heap and a 1 GB direct-memory limit; set `MICROBENCH_HEAP_SIZE` and `MICROBENCH_DIRECT_MEMORY_SIZE` to size them for the workload and host. Allow additional process memory for JVM metadata, threads, and other native allocations.

The bundled KV scenarios use 128 buckets and at least 32 threads in every phase that writes. They set the Fluss client limits `client.writer.max-inflight-requests-per-bucket: 5` and `client.lookup.max-inflight-requests: 128`. The writer limit applies per bucket and requires idempotent writes, which are enabled by default. The lookup limit applies to unacknowledged lookup requests in the shared client; lookup operations may be batched into requests.

Bundled scenarios: `kv-upsert-get`, `kv-agg-mixed`, and `kv-agg-rbm32`.

## Scenario

```yaml
meta:
  name: kv-example
client:
  config:
    client.writer.buffer.memory-size: "512mb"
    client.writer.batch-size: "2mb"
    client.writer.max-inflight-requests-per-bucket: "5"
    client.lookup.max-inflight-requests: "128"
table:
  name: bench_table
  columns:
    - name: id
      type: BIGINT
    - name: value
      type: STRING
  primary-key: [id]
  buckets: 128
data:
  seed: 42
  generators:
    id: {type: sequential, start: 0, end: 1000000}
    value: {type: random-string, length: 64}
workload:
  - phase: write
    warmup: 10000
    records: 500000
    threads: 32
  - phase: lookup
    duration: 1m
    threads: 4
    key-range: [0, 1000000]
  - phase: mixed
    duration: 1m
    threads: 32
    mix: {write: 50, lookup: 50}
```

Phases support `write`, `lookup`, `scan`, and `mixed`. A phase name may add a suffix, such as `write-bulk`. The `scan` phase accepts `from-offset` and a `filter` map. Aggregation columns use `agg` and `merge-engine: AGGREGATION`; see the bundled aggregation scenarios. Data generators include sequential values, random primitive values, strings, bytes, enums, timestamps, and RoaringBitmap values.
