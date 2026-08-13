---
title: Real-Time User Profile
sidebar_position: 4
---

# Real-Time User Profile

This tutorial demonstrates how to build a real-time user profiling system using three core Apache Fluss features: the **Auto-Increment Column**, the **Aggregation Merge Engine**, and the built-in **RoaringBitmap SQL functions**. You will learn how to automatically map high-cardinality email identifiers to compact integer UIDs, accumulate click metrics, and count unique visitors — all directly in the storage layer, keeping the Flink job entirely stateless.

## How the System Works

### Core Concepts

- **Identity Mapping**: Incoming email strings are automatically mapped to compact `INT` UIDs using Fluss's auto-increment column — no manual ID management required.
- **Storage-Level Aggregation**: Click counts are summed and unique visitor bitmaps are OR-ed directly inside the Fluss TabletServers via the Aggregation Merge Engine.
- **Built-in Bitmap Functions**: `rb_build_agg` and `rb_cardinality` are registered natively in FlussCatalog — no external JAR or `CREATE TEMPORARY FUNCTION` required.

### Data Flow

1. **Ingestion**: Raw click events arrive with an email address and a click count.
2. **Mapping**: A Flink lookup join against `user_dict` resolves the email to a UID. If the email is new, the `insert-if-not-exists` hint instructs Fluss to generate a new UID automatically.
3. **Aggregation**: The resolved UID is written to `user_profiles`. The Aggregation Merge Engine sums `total_clicks` and OR-s the `unique_visitors` bitmap at the storage layer — no windowing or Flink state required.

## Prerequisites

Before proceeding, ensure that [Docker](https://docs.docker.com/engine/install/) and the [Docker Compose plugin](https://docs.docker.com/compose/install/linux/) are installed on your machine.

## Environment Setup

1. Create a working directory and navigate into it.
   ```shell
   mkdir fluss-user-profile
   cd fluss-user-profile
   ```

2. Create a `docker-compose.yml` file with the following content:
   ```yaml
   services:
     coordinator-server:
       image: apache/fluss:$FLUSS_DOCKER_VERSION$
       command: coordinatorServer
       depends_on:
         - zookeeper
       environment:
         - |
           FLUSS_PROPERTIES=
           zookeeper.address: zookeeper:2181
           bind.listeners: FLUSS://coordinator-server:9123
           remote.data.dir: /tmp/fluss/remote
       volumes:
         - fluss-remote-data:/tmp/fluss/remote
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
           remote.data.dir: /tmp/fluss/remote
       volumes:
         - fluss-remote-data:/tmp/fluss/remote
     zookeeper:
       restart: always
       image: zookeeper:3.9.2
     jobmanager:
       image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
       ports:
         - "8083:8081"
       command: jobmanager
       environment:
         - |
           FLINK_PROPERTIES=
           jobmanager.rpc.address: jobmanager
           rest.address: jobmanager
           rest.port: 8081
       volumes:
         - fluss-remote-data:/tmp/fluss/remote
     taskmanager:
       image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
       depends_on:
         - jobmanager
       command: taskmanager
       environment:
         - |
           FLINK_PROPERTIES=
           jobmanager.rpc.address: jobmanager
           taskmanager.numberOfTaskSlots: 2
       volumes:
         - fluss-remote-data:/tmp/fluss/remote
     sql-client:
       image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
       command: ["/opt/sql-client/sql-client"]
       depends_on:
         - jobmanager
       environment:
         - |
           FLINK_PROPERTIES=
           jobmanager.rpc.address: jobmanager
           rest.address: jobmanager
           rest.port: 8081
       volumes:
         - fluss-remote-data:/tmp/fluss/remote

   volumes:
     fluss-remote-data:
   ```

3. Start all services.
   ```shell
   docker compose up -d
   ```

4. Confirm all containers are running.
   ```shell
   docker compose ps
   ```
   You should see `coordinator-server`, `tablet-server`, `zookeeper`, `jobmanager`, `taskmanager`, and `sql-client` all in the `running` state.

:::note
All the following commands involving `docker compose` should be executed in the working directory that contains the `docker-compose.yml` file.
:::

## Enter the SQL Client

Use the following command to enter the Flink SQL Client:

```shell
docker compose run --entrypoint bash sql-client -c "
\${FLINK_HOME}/bin/sql-client.sh \
  -Drest.address=jobmanager \
  -Drest.port=8081 \
  -i /opt/sql-client/sql/sql-client.sql
"
```

## Step 1: Create the Fluss Catalog

Run these statements one by one in the SQL Client.

:::tip
Run SQL statements one by one to avoid errors.
:::

```sql
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
```

```sql
USE CATALOG fluss_catalog;
```

:::note
Once you switch to the Fluss catalog, all RoaringBitmap SQL functions (`rb_build_agg`, `rb_cardinality`, `rb_or_agg`, and others) are available immediately — no `CREATE TEMPORARY FUNCTION` statement is needed.
:::

## Step 2: Create the User Dictionary Table

Create the `user_dict` table to map email addresses to integer UIDs. The `auto-increment.fields` property instructs Fluss to automatically assign a unique `INT` UID for every new email it receives.

```sql
CREATE TABLE user_dict (
    email STRING,
    uid   INT,
    PRIMARY KEY (email) NOT ENFORCED
) WITH (
    'auto-increment.fields' = 'uid'
);
```

## Step 3: Create the Aggregated Profile Table

Create the `user_profiles` table using the **Aggregation Merge Engine**. Each user's UID is the primary key. `total_clicks` is summed and `unique_visitors` accumulates a [RoaringBitmap](https://roaringbitmap.org/) of all UIDs seen — both computed directly at the storage layer.

```sql
CREATE TABLE user_profiles (
    uid             INT,
    total_clicks    BIGINT,
    unique_visitors BYTES,
    PRIMARY KEY (uid) NOT ENFORCED
) WITH (
    'table.merge-engine'             = 'aggregation',
    'fields.total_clicks.agg'        = 'sum',
    'fields.unique_visitors.agg'     = 'rbm32'
);
```

## Step 4: Ingest and Process Data

Create a temporary source table to simulate raw click events using the Faker connector.

:::note
Java Faker's `numberBetween(min, max)` treats `max` as exclusive. The expression below produces click counts of 1–10.
:::

```sql
CREATE TEMPORARY TABLE raw_events (
    email       STRING,
    click_count INT,
    proctime    AS PROCTIME()
) WITH (
    'connector'                     = 'faker',
    'rows-per-second'               = '1',
    'fields.email.expression'       = '#{internet.emailAddress}',
    'fields.click_count.expression' = '#{number.numberBetween ''1'',''11''}'
);
```

Now run the pipeline. The `lookup.insert-if-not-exists` hint ensures that if an email is not found in `user_dict`, Fluss generates a new `uid` automatically. `rb_build_agg(d.uid)` builds a one-element RoaringBitmap from each UID — the Aggregation Merge Engine OR-s it into the stored bitmap, giving an exact unique visitor count per user over time.

```sql
INSERT INTO user_profiles
SELECT
    d.uid,
    CAST(e.click_count AS BIGINT),
    rb_build_agg(d.uid)
FROM raw_events AS e
JOIN user_dict /*+ OPTIONS('lookup.insert-if-not-exists' = 'true') */
FOR SYSTEM_TIME AS OF e.proctime AS d
ON e.email = d.email
GROUP BY d.uid, e.click_count;
```

## Step 5: Verify Results

Open a **second terminal**, navigate to the working directory, and launch another SQL Client session to query results while the pipeline runs.

```shell
docker compose run --entrypoint bash sql-client -c "
\${FLINK_HOME}/bin/sql-client.sh \
  -Drest.address=jobmanager \
  -Drest.port=8081
"
```

Set up the catalog:

```sql
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss_catalog;
SET 'sql-client.execution.result-mode' = 'tableau';
```

Query the aggregated profile table. `rb_cardinality` converts the stored bitmap into a human-readable unique visitor count:

```sql
SELECT
    uid,
    total_clicks,
    rb_cardinality(unique_visitors) AS unique_visitor_count
FROM user_profiles;
```

You should see rows appearing for each new user, with `total_clicks` and `unique_visitor_count` growing in real time.

To verify the email-to-UID dictionary mapping:

```sql
SELECT * FROM user_dict LIMIT 10;
```

Each email should have a unique compact `INT` uid automatically assigned by Fluss.

## Clean Up

Exit the SQL Client by typing `exit;`, then stop all services.

```shell
docker compose down -v
```

## Architectural Benefits

- **Stateless Flink Jobs:** Offloading identity mapping, click aggregation, and bitmap union to Fluss makes the Flink job lightweight, with fast checkpoints and minimal recovery time.
- **Compact Storage:** Using auto-incremented `INT` UIDs instead of raw email strings reduces memory and storage footprint significantly.
- **Exact Unique Counting:** RoaringBitmap provides exact distinct counts — no approximations like HyperLogLog.
- **Exactly-Once Accuracy:** The Undo Recovery mechanism in the Fluss Flink connector ensures replayed data during failovers does not result in double-counting.

## What's Next?

For the full reference of all RoaringBitmap SQL functions available in FlussCatalog (`rb_or_agg`, `rb_and`, `rb_contains`, `rb_to_array`, and more), see the [SQL Functions](../../engine-flink/sql-functions/) documentation.