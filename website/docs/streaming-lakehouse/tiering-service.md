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

:::warning
Never change the schema of a lake table managed by the Tiering Service through an external engine. The Tiering Service requires the lake table schema to stay in sync with the Fluss table schema, and a diverging schema makes the tiering job fail and restart in a loop until the schemas match again. For lake formats that support schema evolution through Fluss (Paimon and Iceberg), add columns with `ALTER TABLE` on the Fluss table instead. See [Schema Evolution](datalake-formats/paimon.md#schema-evolution) for the symptoms and the recovery steps with Paimon.
:::

For an in-depth look at the Tiering Service internals, see the blog series:

- [Tiering Service Deep Dive — Part 1](https://fluss.apache.org/blog/fluss-tiering-service-deep-dive-part1/)
- [Tiering Service Deep Dive — Part 2](https://fluss.apache.org/blog/fluss-tiering-service-deep-dive-part2/)
- [Tiering Service Deep Dive — Part 3](https://fluss.apache.org/blog/fluss-tiering-service-deep-dive-part3/)

For deployment instructions, see [Deploying Lakestream](../install-deploy/deploying-streaming-lakehouse.md).

## Architecture

The Tiering Service consists of three Flink operators:

| Operator | Description |
|----------|-------------|
| **TieringSource** | Reads records from Fluss and writes to the data lake via LakeWriter |
| **TieringCommitter** | Commits batches and advances offsets in both Fluss and lake |
| **No-Op Sink** | Dummy sink required by Flink's topology |

### How It Works

1. **Enumerator** sends heartbeat to Fluss CoordinatorService, receives table metadata
2. **SplitGenerator** calculates data delta between Fluss offsets and lake snapshot offsets
3. **Splits** are created for missing data ranges and assigned to readers
4. **Readers** fetch records from Fluss tablet servers, write via LakeWriter
5. **Committer** performs two-phase commit: first to lake, then updates Fluss coordinator
6. Cycle repeats based on `table.datalake.freshness` interval

### Split Types

| Table Type | Split Type | Description |
|------------|------------|-------------|
| Log Table (append-only) | TieringLogSplit | Defines starting and stopping offsets for a contiguous range |
| Primary Key Table | TieringSnapshotSplit | References snapshot ID and log offset for CDC replay |

## Configuration Options

### Tiering Service Options

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
| `lake.tiering.commit-thread-num` | Integer | 2 | Number of threads used to commit tables concurrently. Must be positive |

### Table-Level Options

The following `table.datalake.*` options are configured per table when creating or altering tables, not on the tiering job:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `table.datalake.enabled` | Boolean | false | Enable lakehouse storage for this table |
| `table.datalake.freshness` | Duration | 3min | Maximum lag between Fluss and lake data |
| `table.datalake.auto-compaction` | Boolean | false | Auto-trigger compaction in the data lake |
| `table.datalake.auto-expire-snapshot` | Boolean | false | Auto-expire snapshots in the data lake |

## Embedded Flink Runtime Deployment

Instead of submitting the tiering job to a Flink cluster with `flink run`, the same tiering job can
run on Flink's embedded runtime inside a single JVM process, so no separate Flink cluster needs to
be provisioned. The entrypoint for this mode is `EmbeddedFlussLakeTiering`:

```shell
java -cp "${FLINK_HOME}/lib/*" \
    org.apache.fluss.flink.tiering.EmbeddedFlussLakeTiering \
    --config.file /path/to/tiering.yaml
```

The tiering job is unbounded and completely stateless: progress is persisted through lake snapshots
tracked by Fluss, so no Flink checkpoint storage is needed. On a terminal job failure the process
exits with a non-zero code, and a container runtime or process supervisor simply starts it again.

### Configuration File and Arguments

Configuration is read from a YAML config file, using the same key-value syntax as Fluss's
`server.yaml`, and from `--key value` command line arguments, which take precedence over file
entries. Values are written verbatim into `tiering.yaml` and parsed like `server.yaml`: a value
containing `#` is truncated at the comment marker (inherited `server.yaml` parser behavior), so
avoid `#` in configuration values. The tiering service parameters are the same as the Flink job
arguments above: `fluss.*`,
`datalake.format`, `datalake.<format>.*`, and `lake.tiering.*` (without the `--` prefix in the
file). All remaining keys configure the embedded Flink runtime:

```yaml
# tiering service parameters
fluss.bootstrap.servers: localhost:9123
datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon
fluss.tiering.poll.table.interval: 30 s

# embedded Flink runtime parameters
parallelism.default: 4
taskmanager.numberOfTaskSlots: 4
pipeline.name: fluss-lake-tiering
```

The parallelism must stay within the number of slots the embedded runtime offers, so configure
`taskmanager.numberOfTaskSlots` alongside `parallelism.default`; startup fails fast when it does
not. If `parallelism.default` is unset, the embedded runtime runs the tiering source at the
machine's CPU core count (Flink's default local parallelism). When `taskmanager.numberOfTaskSlots`
is pinned below that, the service fails fast at startup. Set both `parallelism.default` and
`taskmanager.numberOfTaskSlots` explicitly to control the parallelism and slot budget.

### Docker Image and Helm Chart

The `docker/fluss-tiering` directory packages this mode into a self-contained image: the official
Flink distribution carries the runtime, and the Fluss tiering jars plus the Paimon, Iceberg, and
Lance lake plugins live in its `lib` directory. The image bundles the Hadoop
`flink-shaded-hadoop-2-uber` jar, which suits the Paimon default but not Iceberg with the Glue or
Hadoop catalog, where it causes `NoSuchMethodError: LogicalTypes.timestampNanos()`. For those
setups, swap it for the `hadoop-client-api` and `hadoop-client-runtime` jars as described in the
[AWS Glue catalog documentation](./datalake-catalogs/glue.md).

Build it from the repository root; the Fluss artifacts must already be in the local Maven
repository (the version under development is not published to Maven Central), so build the
required modules first:

```shell
# Build the Fluss jars first: the tiering module plus the Flink 1.20 connector and the
# lake format artifacts that prepare_build.sh resolves from the local Maven repository.
./mvnw clean install -DskipTests \
    -pl fluss-flink/fluss-flink-tiering,fluss-flink/fluss-flink-1.20,fluss-lake/fluss-lake-paimon,fluss-lake/fluss-lake-iceberg,fluss-lake/fluss-lake-lance \
    -am
docker/fluss-tiering/build.sh
```

Configuration is provided by mounting a file at `/opt/fluss/conf/tiering.yaml` (or setting
`TIERING_CONFIG_FILE`), or by passing `key: value` pairs through the `FLUSS_TIERING_PROPERTIES`
environment variable; values may reference environment variables as `${VAR}`. A `${VAR}` reference
to a variable that is not set aborts startup with exit code 1 and an error message naming the
variable.

Unless `JAVA_TOOL_OPTIONS` is set, the image starts the JVM with
`-XX:MaxRAMPercentage=60.0 -XX:MaxDirectMemorySize=512m`, sizing the heap from the container's
memory limit; setting `JAVA_TOOL_OPTIONS` replaces these defaults entirely, so an override must
configure both the heap and the direct memory.

The Helm chart can deploy the tiering service alongside the Fluss cluster as a stateless Deployment
that defaults to tiering the release's own cluster:

```shell
helm install fluss helm/ \
    --set tiering.enabled=true \
    --set tiering.image.repository=<registry>/fluss-tiering \
    --set tiering.image.tag=<tag>
```

See the [module README](https://github.com/apache/fluss/blob/main/fluss-flink/fluss-flink-tiering/src/README.md)
and the chart's `tiering` values block for the full configuration surface.

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

The Tiering Service is stateless and can be scaled by adjusting Flink parallelism:

```shell
${FLINK_HOME}/bin/flink run \
    -Dparallelism.default=6 \
    /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    ...
```

Multiple tiering service jobs can run simultaneously. They are coordinated by the Fluss cluster to ensure exactly-once semantics and automatic load balancing.

## Monitoring

Key metrics for monitoring the Tiering Service are available through Flink's metrics system. See [Monitoring Metrics](../maintenance/observability/monitor-metrics.md#tiering-service-metrics) for details on lakehouse tiering metrics.
