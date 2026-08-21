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
| `scan.incremental.start.timestamp` | (none) | Enables an incremental (time-range) batch read and sets the **inclusive** lower bound of the window. Accepts epoch milliseconds (e.g. `1678883047356`) or a `yyyy-MM-dd HH:mm:ss` datetime (e.g. `2023-12-09 23:09:12`) interpreted in the Spark session time zone (`spark.sql.session.timeZone`). A blank or unparseable value fails fast instead of falling back to a full-table read. Batch read only; it has no effect on streaming reads. Only the data Fluss still retains (bounded by `table.log.ttl`) is returned, so a timestamp predating it simply yields fewer rows. |
| `scan.incremental.end.timestamp` | (none) | The **exclusive** upper bound of an incremental batch read, producing a left-closed right-open `[start, end)` window. Same value format as `scan.incremental.start.timestamp`; when unset the read runs up to the latest committed data. The table-valued function always writes a concrete timestamp, pinning the bound when the statement is analyzed. Setting it without `scan.incremental.start.timestamp` fails fast, as does a window whose start is not strictly before its end. |

Both bounds are compared against the record commit timestamp while reading, so the window is exact even for data already tiered to remote storage, where resolving a timestamp to a log offset is only as accurate as the server-side time index.
