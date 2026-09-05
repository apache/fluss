---
title: Real-Time Lakehouse via the HTTP Gateway
sidebar_position: 3
---

This guide walks through the same real-time lakehouse pattern as the
[Streaming Lakehouse](lakehouse.md) quickstart, but creates the table and
writes records entirely over the [Fluss Gateway](/docs/gateway/index.md)
REST API instead of Flink SQL. You'll create a datalake-enabled table with
`curl`, ingest JSON records with `curl`, then use Flink SQL only to run the
Lakehouse Tiering Service and query the unified real-time + historical data
(Union Read).

:::caution Preview
Fluss Gateway is introduced as a preview in Fluss 1.0. Its API and
configuration may change in later releases. The Gateway does not yet
support reading records — this guide reads data back through Flink SQL.
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

The Gateway isn't published as a Docker Hub image yet, so this guide builds
it from source. Run this from the root of your Fluss source checkout (the
script resolves the repository root itself, so you don't need to `cd` into
`docker/fluss-gateway` first):

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
curl -fL -o "lib/paimon-s3-$PAIMON_VERSION$.jar" "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/$PAIMON_VERSION$/paimon-s3-$PAIMON_VERSION$.jar"
```

:::info
The `apache/fluss-quickstart-flink` image already includes the Flink-side
dependencies used by this guide. Only the Fluss server-side `paimon-s3`
plugin still needs to be downloaded and mounted into the Fluss containers.
:::

3. Create a `docker-compose.yml` file with the following content. This
   reuses the same Paimon-backed Fluss + Flink + RustFS stack as the
   [Streaming Lakehouse](lakehouse.md) guide, with a `gateway` service added:

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

4. To start all containers, run:

```shell
docker compose up -d
```

Run

```shell
docker compose ps
```

to check whether all containers are running properly, including `gateway`.
Check that the Gateway process is up and ready to accept requests:

```shell
curl -sS --fail-with-body http://localhost:8080/health
curl -sS --fail-with-body http://localhost:8080/ready
```

:::note
`/health` only reports process liveness; `/ready` reports whether the
Gateway accepts requests but does not check Fluss connectivity itself. The
`gateway` service starts as soon as the `coordinator-server` container
starts, not once it's actually accepting connections, so the first
`/ready` call (or the first database-creation call below) can briefly
return an error or HTTP 503 right after `docker compose up`. Retry after a
few seconds if that happens.
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

## Start the Lakehouse Tiering Service

To integrate with [Apache Paimon](https://paimon.apache.org/), start the
`Lakehouse Tiering Service`. Open a new terminal, navigate to the
`fluss-quickstart-gateway-lakehouse` directory, and run:

```shell
docker compose exec jobmanager \
    /opt/flink/bin/flink run \
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

You should see a Flink Job tiering data from Fluss to Paimon running in the
[Flink Web UI](http://localhost:8083/).

## Query with Union Read

The Gateway's 1.0 preview doesn't support record reads, so this guide uses
Flink SQL to query the table you created over REST — Fluss tables are
identical regardless of which API created them.

Enter the Flink SQL CLI container:

```shell
docker compose run sql-client
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
```

```sql title="Flink SQL"
USE gateway_demo;
```

Switch to batch mode and query only the Paimon-tiered snapshot with the
`$lake` suffix:

```sql title="Flink SQL"
SET 'sql-client.execution.result-mode' = 'tableau';
```

```sql title="Flink SQL"
SET 'execution.runtime-mode' = 'batch';
```

```sql title="Flink SQL"
-- wait for the ~30s datalake.freshness window before running this
SELECT snapshot_id, total_record_count FROM orders$lake$snapshots;
```

```sql title="Flink SQL"
SELECT order_id, customer, amount_cents, status FROM orders$lake;
```

Now query the table directly, which performs a Union Read. For a
primary-key table, `orders` isn't a raw concatenation of two
stores — it gives you the **current unified view** of the table's state,
combining whatever's still in Fluss with what's already tiered to Paimon.
`orders$lake`, by contrast, is the **lake-only view**: high
performance, but reflecting only what's been tiered so far.

```sql title="Flink SQL"
SELECT order_id, customer, amount_cents, status FROM orders;
```

To see this difference, write one more record through the Gateway from
another terminal:

```bash
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/orders/records" \
  -d '{"entries": [{"id": "order-4", "upsert": {"order_id": 4, "customer": "Dave", "amount_cents": 2200, "status": "placed"}}]}'
```

Re-run the query on `orders` in the SQL client — `order_id 4`
appears immediately in the unified view. The lake-only view,
`orders$lake`, will reflect it once the tiering service has
processed it, subject to the configured `table.datalake.freshness`.

### Quitting SQL Client

```sql title="Flink SQL"
quit;
```

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