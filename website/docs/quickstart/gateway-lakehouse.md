---
title: Ingesting HTTP Events into a Real-Time Lakehouse
sidebar_position: 4
---

This guide shows how an application can send JSON events to Fluss through
the Gateway REST API and immediately read them back — without setting up
a message queue, a Flink ingestion pipeline, or a separate Paimon writer.
The same table serves both real-time queries and historical lakehouse
analytics, with Paimon tiering handled automatically in the background.

The example tracks orders flowing through a simple order management system.
The application writes one HTTP request per batch, no Kafka producer, no
Flink ingestion job, no separate Paimon writer. Fluss provides immediate
query visibility in the hot tier, and the reusable Lakehouse Tiering Service
handles writing to Paimon continuously in the background.

In this quickstart, you will:

- spin up Fluss, Fluss Gateway, Flink, Paimon, and RustFS with Docker Compose;
- create a lake-enabled table through the Gateway REST API;
- write JSON order records over HTTP;
- read the records back immediately through Flink SQL before any tiering occurs;
- confirm that the rows are continuously tiered into Paimon.

:::caution Developer Preview - Fluss 1.0 (unreleased)
Fluss Gateway is a preview feature in Fluss 1.0, which has not yet been
released. This quickstart uses its unauthenticated `trust` mode and is
intended only for local development. See
[Fluss Gateway security](/docs/gateway/index.md#security) before planning
a production deployment.

No pre-built Docker Hub image is available yet – this guide requires
building the Gateway image from source (see
[Build the Gateway image](#build-the-gateway-image) below). The Gateway
does not yet support reading records; this guide reads data back through
Flink SQL.
:::

## Environment Setup

### Prerequisites

Before proceeding with this guide, ensure that [Docker](https://docs.docker.com/engine/install/)
and the [Docker Compose plugin](https://docs.docker.com/compose/install/linux/) are
installed on your machine.

:::note
We encourage you to use a recent version of Docker and [Compose v2](https://docs.docker.com/compose/releases/migrate/)
(however, Compose v1 might work with a few adaptions).
:::

### Build the Gateway image

:::note Fluss v1.0 developer preview 
This guide covers a feature shipping in the upcoming Fluss 1.0 release.
A pre-built Docker Hub image will be available at GA - until then,
building it locally takes one extra step: run the script below from the
root of your [Fluss source repository](https://github.com/apache/fluss)
:::
Run this from the root of your Fluss source checkout:
```shell
docker/fluss-gateway/build.sh
```

This compiles the Gateway inside a `rust:1.88-bookworm` builder container
(no local Rust toolchain needed) and produces a local image tagged
`fluss-gateway:dev`, which the Compose file below references directly. The
first build may take several minutes.

### Starting required components

1. Create a working directory for this guide.

```shell
mkdir fluss-quickstart-gateway-lakehouse
cd fluss-quickstart-gateway-lakehouse
```

2. Create a `lib` directory and download the Paimon S3 plugin jar required
   by the Fluss servers:

```shell
mkdir lib
curl -fL -o "lib/paimon-s3-$PAIMON_VERSION$.jar" \
  "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/$PAIMON_VERSION$/paimon-s3-$PAIMON_VERSION$.jar"
```

:::info
The `apache/fluss-quickstart-flink` image already includes the Flink-side
dependencies used by this guide. Only the Fluss server-side `paimon-s3`
plugin still needs to be downloaded and mounted into the Fluss containers.
:::

3. Create a `docker-compose.yml` file with the following content:

```yaml
services:
  #begin RustFS (S3-compatible storage)
  rustfs:
    image: rustfs/rustfs:1.0.0-alpha.83
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      - RUSTFS_ACCESS_KEY=rustfsadmin
      - RUSTFS_SECRET_KEY=rustfsadmin
      - RUSTFS_CONSOLE_ENABLE=true
    volumes:
      - rustfs-data:/data
    command: /data
  rustfs-init:
    image: minio/mc
    depends_on:
      - rustfs
    entrypoint: >
      /bin/sh -c "
      until mc alias set rustfs http://rustfs:9000 rustfsadmin rustfsadmin; do
        echo 'Waiting for RustFS...';
        sleep 1;
      done;
      mc mb --ignore-existing rustfs/fluss;
      "
  #end
  coordinator-server:
    image: apache/fluss:$FLUSS_DOCKER_VERSION$
    command: coordinatorServer
    depends_on:
      zookeeper:
        condition: service_started
      rustfs-init:
        condition: service_completed_successfully
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://coordinator-server:9123
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.region: us-east-1
        s3.path-style-access: true
        s3.assumed.role.arn: arn:aws:iam::000000000000:role/rustfsadmin
        s3.assumed.role.sts.endpoint: http://rustfs:9000
        datalake.enabled: true
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: s3://fluss/paimon
        datalake.paimon.s3.endpoint: http://rustfs:9000
        datalake.paimon.s3.access-key: rustfsadmin
        datalake.paimon.s3.secret-key: rustfsadmin
        datalake.paimon.s3.path.style.access: true
    volumes:
      - ./lib/paimon-s3-$PAIMON_VERSION$.jar:/opt/fluss/plugins/paimon/paimon-s3-$PAIMON_VERSION$.jar
  tablet-server:
    image: apache/fluss:$FLUSS_DOCKER_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://tablet-server:9123
        data.dir: /tmp/fluss/data
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.region: us-east-1
        s3.path-style-access: true
        s3.assumed.role.arn: arn:aws:iam::000000000000:role/rustfsadmin
        s3.assumed.role.sts.endpoint: http://rustfs:9000
        datalake.enabled: true
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: s3://fluss/paimon
        datalake.paimon.s3.endpoint: http://rustfs:9000
        datalake.paimon.s3.access-key: rustfsadmin
        datalake.paimon.s3.secret-key: rustfsadmin
        datalake.paimon.s3.path.style.access: true
    volumes:
      - ./lib/paimon-s3-$PAIMON_VERSION$.jar:/opt/fluss/plugins/paimon/paimon-s3-$PAIMON_VERSION$.jar
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  #begin Fluss Gateway
  gateway:
    image: fluss-gateway:dev
    depends_on:
      - coordinator-server
    ports:
      - "8080:8080"
    environment:
      - FLUSS_GATEWAY__CLUSTER__DEFAULT__BOOTSTRAP__SERVERS=coordinator-server:9123
  #end
  jobmanager:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    ports:
      - "8083:8081"
    entrypoint: ["/opt/flink/init_paimon.sh"]
    command: ["jobmanager"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
  taskmanager:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    depends_on:
      - jobmanager
    entrypoint: ["/opt/flink/init_paimon.sh"]
    command: ["taskmanager"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.task.off-heap.size: 128m
  sql-client:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    depends_on:
      - jobmanager
    entrypoint: ["/opt/flink/init_paimon.sh"]
    command: ["/opt/sql-client/sql-client"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        rest.address: jobmanager

volumes:
  rustfs-data:
```

The Docker Compose environment consists of the following containers:
- **Fluss Cluster:** a Fluss `CoordinatorServer`, a Fluss `TabletServer` and
  a `ZooKeeper` server.
- **Fluss Gateway:** a stateless REST service used to create tables and
  write records in this guide, listening on `localhost:8080`.
- **Flink Cluster**: a Flink `JobManager`, a Flink `TaskManager`, and a
  Flink SQL client container, used to run the Lakehouse Tiering Service and
  query results.
- **RustFS**: an S3-compatible storage system used both as Fluss remote
  storage and Paimon's filesystem warehouse.

:::tip
[RustFS](https://github.com/rustfs/rustfs) is used as replacement for S3 in
this quickstart example, for your production setup you may want to
configure this to use cloud file system. See [here](/maintenance/tiered-storage/filesystems/overview.md)
for information on how to setup cloud file systems.
:::

4. Start all containers.

```shell
docker compose up -d
docker compose ps
```

:::note
The `sql-client` service may exit after `docker compose up -d` because no
interactive terminal is attached. This is expected. You will start a new SQL
client container with `docker compose run --rm sql-client` later in this guide.
:::

5. Wait until the Gateway can reach Fluss:

```shell
until curl -fsS \
  http://localhost:8080/v1/clusters/default/databases >/dev/null; do
  sleep 2
done
```

:::note
All the following commands involving `docker compose` should be executed in the
working directory that contains the `docker-compose.yml` file.
:::

Congratulations, you are all set!

## Create a datalake-enabled table via the Gateway

Set the endpoint and resource names used in the examples:

```bash
GATEWAY_URL=http://localhost:8080
CLUSTER=default
DATABASE=gateway_demo
```

### Create a database

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases" \
  -d "{\"database\":\"$DATABASE\"}"
```

### Create the table with lakehouse integration enabled

Set `table.datalake.enabled` and `table.datalake.freshness` in `configs` at
creation time — the same properties the `lakehouse.md` guide sets via
`WITH (...)` in Flink SQL:

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables" \
  -d '{
    "table_name": "orders",
    "columns": [
      {"name": "order_id", "data_type": {"type": "INTEGER"}, "nullable": false},
      {"name": "customer", "data_type": {"type": "STRING"}, "nullable": true},
      {"name": "amount_cents", "data_type": {"type": "BIGINT"}, "nullable": true},
      {"name": "status", "data_type": {"type": "STRING"}, "nullable": true}
    ],
    "primary_key": ["order_id"],
    "distribution": {"bucket_count": 1, "bucket_keys": ["order_id"]},
    "configs": {
      "table.datalake.enabled": "true",
      "table.datalake.freshness": "30s"
    }
  }'
```

:::note
`amount_cents` stores the order amount as an integer number of cents, to
keep the HTTP payload simple.
:::

### Write records

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/orders/records" \
  -d '{
    "entries": [
      {"id": "order-1", "upsert": {"order_id": 1, "customer": "Alice", "amount_cents": 4599, "status": "placed"}},
      {"id": "order-2", "upsert": {"order_id": 2, "customer": "Bob", "amount_cents": 12000, "status": "placed"}},
      {"id": "order-3", "upsert": {"order_id": 3, "customer": "Carol", "amount_cents": 750, "status": "placed"}}
    ]
  }'
```

:::note
`amount_cents` values fit comfortably in a JSON number here. For `BIGINT`
or `DECIMAL` values that exceed JSON number precision (values beyond 2^53),
pass them as base-10 strings instead — e.g. `"amount_cents": "9999999999999999"`.
:::

A successful response looks like:

```json
{
  "row_count": 3,
  "success_count": 3,
  "error_count": 0,
  "successes": [{"id": "order-1"}, {"id": "order-2"}, {"id": "order-3"}],
  "failures": []
}
```

## Query Immediately

Open the Flink SQL client:

```shell
docker compose run --rm sql-client
```
Create the Fluss catalog and switch to it:

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'paimon.s3.access-key' = 'rustfsadmin',
    'paimon.s3.secret-key' = 'rustfsadmin'
);
```

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
USE gateway_demo;
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';
```
Query the table immediately after writing – no need to wait for tiering:

```sql title="Flink SQL"
SELECT order_id, customer, amount_cents, status FROM orders;
```

The result includes all three orders even though Paimon has not committed
its first snapshot yet. Union Read gets the newest rows directly from Fluss
and combines them with any data already tiered:

```text
+----------+----------+---------------+---------+
| order_id | customer | amount_cents  | status  |
+----------+----------+---------------+---------+
|        1 |    Alice |          4599 | placed  |
|        2 |      Bob |         12000 | placed  |
|        3 |    Carol |           750 | placed  |
+----------+----------+---------------+---------+
```

## Start the Lakehouse Tiering Service

Submit the Lakehouse Tiering Service as a detached Flink job:

```shell
docker compose exec jobmanager \
    /opt/flink/bin/flink run -d \
    /opt/flink/opt/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse s3://fluss/paimon \
    --datalake.paimon.s3.endpoint http://rustfs:9000 \
    --datalake.paimon.s3.access.key rustfsadmin \
    --datalake.paimon.s3.secret.key rustfsadmin \
    --datalake.paimon.s3.path.style.access true
```

The [Flink Web UI](http://localhost:8083/) should show one running tiering job.

## Verify Data in Paimon

After approximately 30 seconds, inspect the Paimon snapshots:

```sql title="Flink SQL"
SELECT snapshot_id, total_record_count
FROM orders$lake$snapshots
ORDER BY snapshot_id;
```

Then check the lake row count:

```sql title="Flink SQL"
SELECT COUNT(*) AS lake_rows FROM orders$lake;
```

The count becomes `3` after the tiering cycle commits the rows.

To see the difference between the two query forms, write one more record
through the Gateway from another terminal:

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/orders/records" \
  -d '{"entries": [{"id": "order-4", "upsert": {"order_id": 4, "customer": "Dave", "amount_cents": 2200, "status": "placed"}}]}'
```

Re-run `SELECT ... FROM orders` in the SQL client – `order_id 4` appears
immediately in the unified view. The lake-only view, `orders$lake`, will
reflect it once the tiering service has processed it.

The two query forms serve different purposes:

| Query | Data read | Expected freshness |
|---|---|---|
| `SELECT ... FROM orders` | Fluss hot data + latest Paimon snapshot | Sub-second |
| `SELECT ... FROM orders$lake` | Paimon only | ~30s tiering window |

### Quitting the SQL Client

```sql title="Flink SQL"
exit;
```

## What This Quickstart Demonstrates

The application in this quickstart never touches Kafka, Flink, or Paimon directly.
It only makes HTTP requests to the Gateway. Specifically, there is no need for:

- a Kafka topic or producer;
- a dedicated Flink ingestion job;
- a Paimon writer;
- separate table definitions for the streaming and lake layers.

The Lakehouse Tiering Service does run on Flink, but as shared infrastructure
that serves all Lake-enabled Fluss tables — not as code the application owns
or operates.

## Preview limitations

- The Gateway's 1.0 preview only implements `trust` mode security — it does
  not authenticate callers or terminate TLS. Don't expose `$GATEWAY_URL`
  directly in production; put it behind an authenticated, TLS-terminating
  ingress.
- The Gateway does not support record reads, primary-key/prefix lookups, or
  log scans — this is why the guide reads back through Flink SQL.
- HTTP 200 from a write request can still contain partial failures; always
  check both `successes` and `failures` in the response body.

See the full [Fluss Gateway reference](/docs/gateway/index.md) for details.

## Clean up

Exit the SQL client, then delete the table and database through the
Gateway, then stop the containers:

```bash
curl -sS --fail-with-body -X DELETE \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/orders"
curl -sS --fail-with-body -X DELETE \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE"
```

```shell
docker compose down -v
```

## Learn more

Now that you're up and running with the Fluss Gateway and a real-time
lakehouse, check out the [Fluss Gateway reference](/docs/gateway/index.md)
for the full REST API, or the [Streaming Lakehouse](lakehouse.md) guide for
the equivalent all-Flink-SQL workflow.
