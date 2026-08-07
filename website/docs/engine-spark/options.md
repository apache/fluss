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
| `spark.sql.fluss.scan.startup.mode` | `full` | The startup mode when reading a Fluss table. Supported values: <ul><li>`full` (default): For primary key tables, reads the full snapshot and merges with log changes. For log tables, reads from the earliest offset.</li><li>`earliest`: Reads from the earliest log/changelog offset.</li><li>`latest`: Reads from the latest log/changelog offset.</li></ul>**Note:** This option only affects Structured Streaming reads, and only `latest` mode is currently supported there. Batch reads ignore it: a plain batch read is always the full table, and a time-range batch read is requested per query (see below). |
| `spark.sql.fluss.read.optimized` | `false` | If `true`, Spark will only read data from the data lake snapshot or KV snapshot, without merging log changes. This can improve read performance but may return stale data for primary key tables. |
| `spark.sql.fluss.scan.poll.timeout` | `10000ms` | The timeout for the log scanner to poll records. |

## Per-Query Read Options

The following options configure a single read and are **not** read from session configuration, so a time window can never leak into later reads. In SQL they are set by the `fluss_incremental_between_timestamp(...)` table-valued function; in the DataFrame API by `spark.read.option(...)` (without the `spark.sql.fluss.` prefix). See [Reads](reads.md#time-range-batch-read) for the full semantics.

| Option | Default | Description |
|--------|---------|-------------|
| `scan.incremental.start.timestamp` | (none) | Enables an incremental (time-range) batch read and sets the **inclusive** lower bound of the window. Accepts epoch milliseconds (e.g. `1678883047356`) or a `yyyy-MM-dd HH:mm:ss` datetime (e.g. `2023-12-09 23:09:12`) interpreted in the Spark session time zone (`spark.sql.session.timeZone`). Batch read only; it has no effect on streaming reads. If the timestamp predates the data still retained by Fluss (bounded by `table.log.ttl`), behavior is controlled by `scan.incremental.timestamp.out-of-range`. |
| `scan.incremental.end.timestamp` | `latest` | The **exclusive** upper bound of an incremental batch read, producing a left-closed right-open `[start, end)` window. `latest` (default) stops at the latest committed data captured at planning time; otherwise the same value format as `scan.incremental.start.timestamp`. Only honored when `scan.incremental.start.timestamp` is set. A timestamp in the future is rejected by the server (`InvalidTimestampException`). |
| `scan.incremental.timestamp.out-of-range` | `error` | Behavior when `scan.incremental.start.timestamp` precedes the earliest data still retained by Fluss (bounded by `table.log.ttl`). <ul><li>`error` (default): fail fast so a truncated window is never returned silently.</li><li>`adjust`: clamp the start to the earliest retained offset and read from there.</li></ul> |
