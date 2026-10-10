# Benchmarking Fluss

The `fluss-microbench` module runs YAML scenarios against an independently started Fluss cluster. Each phase records throughput and latency in a JSON report.

```bash
./fluss-microbench/fluss-microbench.sh list
./fluss-microbench/fluss-microbench.sh validate --scenario-file kv-upsert-get
./fluss-microbench/fluss-microbench.sh run --scenario-file kv-upsert-get --bootstrap-servers localhost:9123
./fluss-microbench/fluss-microbench.sh run --scenario-file my-scenario.yaml --bootstrap-servers localhost:9123
```

Start a Fluss cluster first, for example with the distribution's `bin/local-cluster.sh start`, and stop it with `bin/local-cluster.sh stop` after the run. The launcher builds the module when needed. It writes `summary.json` and `config-snapshot.yaml` under `.microbench/runs/<scenario>/<timestamp>/`. Set `MICROBENCH_ROOT` to use another output directory.

The bundled scenarios cover KV upserts and lookups, mixed workloads, and bitmap aggregation. See `fluss-microbench/README.md` and the YAML files in `fluss-microbench/src/main/resources/presets/` for the scenario schema.
