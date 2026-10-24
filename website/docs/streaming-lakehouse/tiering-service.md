---
title: "Tiering Service"
sidebar_position: 2
---

# Tiering Service

The Tiering Service continuously compacts real-time data from Fluss into the configured lake format (Paimon, Iceberg, Hudi, or Lance) for cost-efficient long-term storage and analytics.

## Overview

The Tiering Service is implemented as an Apache Flink job that:
- Reads records from Fluss tables with lakehouse storage enabled
- Writes data to the configured data lake format (Paimon, Iceberg, Hudi, or Lance)
- Maintains exactly-once semantics between Fluss and the data lake
- Operates incrementally, syncing only missing data segments

A Spark implementation is also available. See [Spark Execution Model](#spark-execution-model) and [Spark deployment](../install-deploy/deploying-streaming-lakehouse.md#spark).

:::warning
Never change the schema of a lake table managed by the Tiering Service through an external engine. The Tiering Service requires the lake table schema to stay in sync with the Fluss table schema, and a diverging schema makes the tiering job fail and restart in a loop until the schemas match again. For lake formats that support schema evolution through Fluss (Paimon and Iceberg), add columns with `ALTER TABLE` on the Fluss table instead. See [Schema Evolution](datalake-formats/paimon.md#schema-evolution) for the symptoms and the recovery steps with Paimon.
:::

For an in-depth look at the Tiering Service internals, see the blog series:

- [Tiering Service Deep Dive — Part 1](https://fluss.apache.org/blog/fluss-tiering-service-deep-dive-part1/)
- [Tiering Service Deep Dive — Part 2](https://fluss.apache.org/blog/fluss-tiering-service-deep-dive-part2/)
- [Tiering Service Deep Dive — Part 3](https://fluss.apache.org/blog/fluss-tiering-service-deep-dive-part3/)

For deployment instructions, see [Deploying Lakestream](../install-deploy/deploying-streaming-lakehouse.md).

## Architecture

### Flink Execution Model

The Tiering Service consists of three Flink operators:

| Operator | Description |
|----------|-------------|
| **TieringSource** | Reads records from Fluss and writes to the data lake via LakeWriter |
| **TieringCommitter** | Commits batches and advances offsets in both Fluss and lake |
| **No-Op Sink** | Dummy sink required by Flink's topology |

#### How It Works

1. **Enumerator** sends heartbeat to Fluss CoordinatorService, receives table metadata
2. **SplitGenerator** calculates data delta between Fluss offsets and lake snapshot offsets
3. **Splits** are created for missing data ranges and assigned to readers
4. **Readers** fetch records from Fluss tablet servers, write via LakeWriter
5. **Committer** performs two-phase commit: first to lake, then updates Fluss coordinator
6. Cycle repeats based on `table.datalake.freshness` interval

#### Split Types

| Table Type | Split Type | Description |
|------------|------------|-------------|
| Log Table (append-only) | TieringLogSplit | Defines starting and stopping offsets for a contiguous range |
| Primary Key Table | TieringSnapshotSplit | References snapshot ID and log offset for CDC replay |

### Spark Execution Model

The Spark application runs a long-lived driver loop that processes one table tiering round at a time. For each round, the driver generates splits and submits an RDD with one partition per split. Executor tasks read the assigned data ranges and write through the lake connector. The driver collects their write results, commits to the lake and Fluss, and then requests the next table.

Background heartbeats keep the assignment alive while splits are generated, tasks run, and results are committed. This uses Spark batch jobs, with no Structured Streaming query or streaming checkpoint directory.

A round completes all its splits before committing. A large initial snapshot or log backlog can therefore delay other tables assigned to the same application. `table.datalake.freshness` is a scheduling target, not a time limit on a Spark round or a guarantee of maximum end-to-end lag.

## Configuration Options

### Flink Options {#tiering-service-options}

The tiering job is a standalone Flink job, and its arguments fall into three groups:

- `--fluss.*` — Fluss client configuration, e.g. `--fluss.bootstrap.servers localhost:9123`
- `--datalake.<format>.*` — lake catalog/storage configuration; Fluss strips the `datalake.<format>.` prefix before passing the remaining keys to the lake connector
- `--lake.tiering.*` — tiering job-level configuration, e.g. `--lake.tiering.auto-expire-snapshot true`

For example:

```shell
${FLINK_HOME}/bin/flink run \
    -Dparallelism.default=3 \
    /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/paimon \
    --lake.tiering.auto-expire-snapshot true
```

The following `--lake.tiering.*` options are set when starting the tiering job:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `lake.tiering.auto-expire-snapshot` | Boolean | false | Auto-trigger snapshot expiration on commit |
| `lake.tiering.partition.mark-done.enabled` | Boolean | false | Enable [partition mark-done](#partition-mark-done) using the actual lake table's options |
| `lake.tiering.io.tmp.dirs` | String | Flink temporary directories | Local directories used for temporary I/O files. If not configured, a `fluss` child directory under each Flink temporary directory is used. Separate multiple directories with commas or the system path separator |

### Spark Options

The Spark entry point accepts the same `--fluss.*`, `--datalake.*`, and `--lake.tiering.*` argument groups shown above. Set `--datalake.format` and the catalog/storage options to match the Fluss cluster; the application does not inherit `server.yaml`. The `fluss.` and `datalake.<format>.` prefixes are stripped, while `lake.tiering.` is retained.

The `lake.tiering.auto-expire-snapshot` and `lake.tiering.partition.mark-done.enabled` options have the same defaults as above. For Spark, `lake.tiering.io.tmp.dirs` is unset by default; the Paimon writer then uses the executor JVM's `java.io.tmpdir`. Explicit paths must be writable on every executor.

Pass Spark runtime options such as `--master`, `--deploy-mode`, `--executor-cores`, and `--conf` before the JAR. Pass service arguments after the JAR as `--key value` or `--key=value`; quote duration values containing spaces.

The following service options are read from the Fluss configuration, so the CLI requires the **`--fluss.` prefix**. For example, use `--fluss.tiering.heartbeat.interval "10 s"`.

| Option in Fluss configuration | Application argument | Default | Description |
|-------------------------------|----------------------|---------|-------------|
| `tiering.poll.table.interval` | `--fluss.tiering.poll.table.interval` | 30 s | Delay before requesting another table when none is available, or after a failed assignment-request heartbeat |
| `tiering.heartbeat.interval` | `--fluss.tiering.heartbeat.interval` | 10 s | Background heartbeat interval during an active tiering round |
| `tiering.poll.timeout` | `--fluss.tiering.poll.timeout` | 10 s | Timeout for polling Fluss records on executors; does not limit the duration of a round |
| `tiering.heartbeat.max.failures` | `--fluss.tiering.heartbeat.max.failures` | 10 | Consecutive heartbeat failures tolerated before the service fails; reaching this count triggers failure |

All four values must be positive. A successful heartbeat resets the failure count.

### Table-Level Options

The following `table.datalake.*` options are configured per table when creating or altering tables, not on the tiering job:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `table.datalake.enabled` | Boolean | false | Enable lakehouse storage for this table |
| `table.datalake.freshness` | Duration | 3min | Maximum lag between Fluss and lake data |
| `table.datalake.auto-compaction` | Boolean | false | Auto-trigger compaction in the data lake |
| `table.datalake.auto-expire-snapshot` | Boolean | false | Auto-expire snapshots in the data lake |

## Partition Mark-Done

The Tiering Service supports marking idle partitions of Paimon tables done. Enable the job-level option `--lake.tiering.partition.mark-done.enabled true` and configure `partition.idle-time-to-done` on the Paimon table. Runtime checks and actions use the **actual Paimon table options**. Fluss `paimon.*` properties can pass these options to Paimon when creating a table; retained Fluss properties do not override the lake table at runtime.

Only `partition.mark-done-action.mode=process-time` is supported. Other modes, including `watermark`, log a warning and disable mark-done for the table. A partition becomes eligible when both its last update and its end time are older than the configured idle duration. The default action creates a `_SUCCESS` file. Actions can be retried after failures, so custom actions must be idempotent. Late data starts tracking the partition again.

Configure `partition.time-interval` on every Paimon table using mark-done, including Fluss auto-partitioned tables. Partition end times use Paimon's timestamp extraction rules and the JVM time zone; Fluss auto-partition rules are not used. If no timestamp pattern or formatter is configured, Paimon uses its defaults.

For a `dt` partition such as `20260928`, configure the Paimon table:

```sql
ALTER TABLE events SET (
  'partition.idle-time-to-done' = '10 min',
  'partition.timestamp-pattern' = '$dt',
  'partition.timestamp-formatter' = 'yyyyMMdd',
  'partition.time-interval' = '1 d'
);
```

## Scaling

### Flink

The Tiering Service is stateless and can be scaled by adjusting Flink parallelism:

```shell
${FLINK_HOME}/bin/flink run \
    -Dparallelism.default=6 \
    /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    ...
```

Multiple tiering service jobs can run simultaneously. They are coordinated by the Fluss cluster to ensure exactly-once semantics and automatic load balancing.

### Spark

Increase executor resources to run more split tasks concurrently. The number of RDD partitions is explicitly set to the number of generated splits, so `spark.default.parallelism` and `spark.sql.shuffle.partitions` do not set tiering parallelism. Actual concurrency is bounded by the split count and available executor task slots.

Each Spark application processes one table at a time. Run multiple applications to tier different tables concurrently.

## Failure Recovery

### Spark

Spark tiering recovers progress from committed lake snapshots and Fluss offsets, without a streaming checkpoint directory. Spark retries failed tasks according to its task retry settings. If a table round fails, the service reports the failure to the Fluss coordinator for rescheduling. A rejected assignment or coordinator epoch change cancels the active round. Reaching `tiering.heartbeat.max.failures` fails the application; configure application restart through your cluster manager or resubmit it. The driver loop does not restart a failed application itself.

## Monitoring

### Flink

Key metrics for monitoring the Tiering Service are available through Flink's metrics system. See [Monitoring Metrics](../maintenance/observability/monitor-metrics.md#tiering-service-metrics) for details on lakehouse tiering metrics.

### Spark

Use the Spark Web UI for jobs, stages, task failures, and executor resource usage. Driver logs report assignments, heartbeats, and round failures; executor logs report completed splits. Spark does not expose the Flink source-reader `fluss.tieringService` metrics.

Monitor Fluss [Coordinator Server metrics](../maintenance/observability/monitor-metrics.md#coordinator-server), including pending/running tables, `tierLag`, `tierDuration`, and `failuresTotal`, for tiering progress.
