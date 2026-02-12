---
sidebar_label: Connector Options
title: Spark Connector Options
sidebar_position: 7
---

# Spark Connector Options

This page lists all the available options for the Fluss Spark connector.

## Read Options

The following Spark configurations can be used to control read behavior for both batch and streaming reads. These options are set using `SET` in Spark SQL or via `spark.conf.set()` in Spark applications. All options are prefixed with `spark.sql.fluss.`.

| Option | Default | Description |
|--------|---------|-------------|
| `spark.sql.fluss.scan.startup.mode` | `full` | The startup mode when reading a Fluss table. Supported values: <ul><li>`full` (default): For primary key tables, reads the full snapshot and merges with log changes. For log tables, reads from the earliest offset.</li><li>`earliest`: Reads from the earliest log/changelog offset.</li><li>`latest`: Reads from the latest log/changelog offset.</li><li>`timestamp`: Reads from a specified timestamp (requires `scan.startup.timestamp`).</li></ul>**Note:** For Structured Streaming read, only `latest` mode is currently supported. |
| `spark.sql.fluss.read.optimized` | `false` | If `true`, Spark will only read data from the data lake snapshot or KV snapshot, without merging log changes. This can improve read performance but may return stale data for primary key tables. |
| `spark.sql.fluss.scan.poll.timeout` | `10000ms` | The timeout for the log scanner to poll records. |
