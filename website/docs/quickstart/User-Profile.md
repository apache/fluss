---
title: Real-Time User Profile
sidebar_position: 4
---

# Real-Time User Profile

This tutorial demonstrates how to build a production-grade, real-time user profiling system using Apache Fluss. You will learn how to map high-cardinality string identifiers (like emails) to compact integers and aggregate user behavior directly in the storage layer with exactly-once guarantees.

<img width="1024" height="535" alt="Image" src="https://github.com/user-attachments/assets/91b30bbf-b1f0-42a7-89b1-03013a4d2ca2" />

## How the System Works

### Core Concepts
*   **Identity Mapping**: High-cardinality strings (Emails) ➔ Compact 32-bit `INT` UIDs.
*   **Offloaded Aggregation**: Computation happens in Fluss TabletServers, keeping Flink state-free.
*   **Optimized Storage**: Native [RoaringBitmap](https://roaringbitmap.org/) support for sub-second unique visitor (UV) counts.

### Data Flow
1.  **Ingestion**: Raw event streams (e.g., clicks, page views) enter the system from a source like [Apache Kafka](https://kafka.apache.org/).
2.  **Mapping**: The [Apache Flink](https://flink.apache.org/) job performs a temporal lookup join against the `user_dict` table. If a user is new, the `insert-if-not-exists` hint triggers Fluss to automatically generate a unique `INT` UID using its Auto-Increment feature.
3.  **Merge**: The **Aggregation Merge Engine** updates clicks and bitmaps in the storage layer.
4.  **Recovery**: The **Undo Recovery** mechanism ensures exactly-once accuracy during failovers.

## Environment Setup

### Prerequisites

Before proceeding, ensure that [Docker](https://docs.docker.com/engine/install/) and the [Docker Compose plugin](https://docs.docker.com/compose/install/linux/) are installed.

### Starting the Playground

1. Create a working directory.
   ```shell
   mkdir fluss-user-profile
   cd fluss-user-profile
   ```

2. Set the Fluss version environment variable.
   ```shell
   # Set to the version you want to use, e.g., 0.6.0 or 0.10-SNAPSHOT
   export FLUSS_DOCKER_VERSION=0.6.0 
   export FLINK_VERSION="1.20"
   ```

3. Create a `lib` directory and download the required JARs.
   ```shell
   mkdir lib
   # Download Flink Faker for data generation
   curl -fL -o lib/flink-faker-0.5.3.jar https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar
   # Download Fluss Connector
   curl -fL -o "lib/fluss-flink-${FLINK_VERSION}-${FLUSS_DOCKER_VERSION}.jar" "https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-${FLINK_VERSION}/${FLUSS_DOCKER_VERSION}/fluss-flink-${FLINK_VERSION}-${FLUSS_DOCKER_VERSION}.jar"
   ```

4. Create a `docker-compose.yml` file.
   ```yaml
   services:
     coordinator-server:
       image: apache/fluss:${FLUSS_DOCKER_VERSION}
       command: coordinatorServer
       depends_on:
         - zookeeper
       environment:
         - |
           FLUSS_PROPERTIES=
           zookeeper.address: zookeeper:2181
           bind.listeners: FLUSS://coordinator-server:9123
           remote.data.dir: /tmp/fluss/remote-data
     tablet-server:
       image: apache/fluss:${FLUSS_DOCKER_VERSION}
       command: tabletServer
       depends_on:
         - coordinator-server
       environment:
         - |
           FLUSS_PROPERTIES=
           zookeeper.address: zookeeper:2181
           bind.listeners: FLUSS://tablet-server:9123
           data.dir: /tmp/fluss/data
           remote.data.dir: /tmp/fluss/remote-data
     zookeeper:
       restart: always
       image: zookeeper:3.9.2
     jobmanager:
       image: flink:${FLINK_VERSION}
       ports:
         - "8081:8081"
       environment:
         - |
           FLINK_PROPERTIES=
           jobmanager.rpc.address: jobmanager
       entrypoint: ["sh", "-c", "cp -v /tmp/lib/*.jar /opt/flink/lib && exec /docker-entrypoint.sh jobmanager"]
       volumes:
         - ./lib:/tmp/lib
     taskmanager:
       image: flink:${FLINK_VERSION}
       depends_on:
         - jobmanager
       environment:
         - |
           FLINK_PROPERTIES=
           jobmanager.rpc.address: jobmanager
           taskmanager.numberOfTaskSlots: 2
       entrypoint: ["sh", "-c", "cp -v /tmp/lib/*.jar /opt/flink/lib && exec /docker-entrypoint.sh taskmanager"]
       volumes:
         - ./lib:/tmp/lib
     sql-client:
       image: flink:${FLINK_VERSION}
       depends_on:
         - jobmanager
       environment:
         - |
           FLINK_PROPERTIES=
           jobmanager.rpc.address: jobmanager
           rest.address: jobmanager
       entrypoint: ["sh", "-c", "cp -v /tmp/lib/*.jar /opt/flink/lib && exec /docker-entrypoint.sh bin/sql-client.sh"]
       volumes:
         - ./lib:/tmp/lib
   ```

5. Start the environment.
   ```shell
   docker compose up -d
   ```

6. Launch the Flink SQL Client.
   ```shell
   docker compose run sql-client
   ```

## Step 1: Create the Fluss Catalog

In the SQL Client, create and use the Fluss catalog.

```sql
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
```

## Step 2: Create the User Dictionary Table

Create the `user_dict` table to map emails to UIDs. We use `table.auto-increment.fields` to automatically generate unique IDs.

```sql
CREATE TABLE user_dict (
    email STRING,
    uid INT,
    PRIMARY KEY (email) NOT ENFORCED
) WITH (
    'table.type' = 'pk',
    'table.auto-increment.fields' = 'uid'
);
```

## Step 3: Create the Aggregated Profile Table

Create the `user_profiles` table using the **Aggregation Merge Engine**. This pushes the aggregation logic (summing clicks, unioning bitmaps) to the Fluss TabletServers.

:::tip
We use `rbm64` ([RoaringBitmap](https://roaringbitmap.org/)) for efficient unique visitor counting. This allows you to store millions of unique IDs in a highly compressed format.
:::

```sql
CREATE TABLE user_profiles (
    profile_id INT,
    unique_visitors BYTES,
    total_clicks BIGINT,
    PRIMARY KEY (profile_id) NOT ENFORCED
) WITH (
    'table.type' = 'pk',
    'table.merge-engine' = 'aggregation',
    'fields.unique_visitors.agg' = 'rbm64',
    'fields.total_clicks.agg' = 'sum'
);
```

## Step 4: Ingest and Process Data

Create a temporary source table to simulate raw user events using the Faker connector.

```sql
CREATE TEMPORARY TABLE raw_events (
    email STRING,
    click_count INT,
    profile_group_id INT,
    proctime AS PROCTIME()
) WITH (
    'connector' = 'faker',
    'rows-per-second' = '1',
    'fields.email.expression' = '#{internet.emailAddress}',
    'fields.click_count.expression' = '#{number.numberBetween ''1'',''10''}',
    'fields.profile_group_id.expression' = '#{number.numberBetween ''1'',''5''}'
);
```

Now, run the pipeline. The `lookup.insert-if-not-exists` hint ensures that if an email is not found in `user_dict`, Fluss generates a new `uid` on the fly.

```sql
INSERT INTO user_profiles
SELECT
    d.uid,
    TO_BINARY(d.uid), -- Convert INT to BYTES for rbm64
    CAST(e.click_count AS BIGINT)
FROM raw_events AS e
JOIN user_dict /*+ OPTIONS('lookup.insert-if-not-exists' = 'true') */
FOR SYSTEM_TIME AS OF e.proctime AS d
ON e.email = d.email;
```

## Step 5: Verify Results

You can query the `user_profiles` table to see the aggregated metrics updating in real-time. To make the output readable, we use the `rbm64_cardinality` function to turn the bytes back into a human-readable count.

```sql
-- Set result mode to tableau for better visualization
SET 'sql-client.execution.result-mode' = 'tableau';

-- Query the profiles with human-readable UV counts
SELECT 
    profile_id, 
    total_clicks, 
    rbm64_cardinality(unique_visitors) as unique_visitor_count 
FROM user_profiles;
```

You should see rows with `profile_id`, `total_clicks`, and `unique_visitor_count`. The `total_clicks` will increase as more events arrive for the same user.

To verify the dictionary mapping:
```sql
SELECT * FROM user_dict;
```

## Clean Up

Exit the SQL Client (`exit;`) and stop the Docker containers.

```shell
docker compose down -v
```

## Architectural Benefits

*   **Stateless Flink Jobs:** Offloading the dictionary and aggregation state to Fluss makes Flink jobs lightweight, enabling faster checkpoints and nearly instant recovery.
*   **Compact Storage:** Using `INT` UIDs instead of `STRING` emails reduces the memory footprint for deduplication tasks by up to 90%.
*   **Exactly-Once Accuracy:** Even during failovers, the **Undo Recovery** mechanism ensures replayed data does not result in double-counting, maintaining perfect accuracy.
