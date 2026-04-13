---
title: Real-Time User Profile
sidebar_position: 4
---

# Real-Time User Profile

This tutorial demonstrates how to build a real-time user profiling system using two core Apache Fluss features: the **Auto-Increment Column** and the **Aggregation Merge Engine**. You will learn how to automatically map high-cardinality string identifiers (like emails) to compact integer UIDs, and accumulate user click metrics directly in the storage layer keeping the Flink job entirely stateless.

## How the System Works

### Core Concepts

- **Identity Mapping**: Incoming email strings are automatically mapped to compact `INT` UIDs using Fluss's auto-increment column, no manual ID management required.
- **Storage-Level Aggregation**: Click counts are accumulated directly in the Fluss TabletServers via the Aggregation Merge Engine. The Flink job remains stateless and lightweight.

### Data Flow

1. **Ingestion**: Raw click events arrive with an email address and a click count.
2. **Mapping**: A Flink lookup join against `user_dict` resolves the email to a UID. If the email is new, the `insert-if-not-exists` hint instructs Fluss to generate a new UID automatically.
3. **Aggregation**: The resolved UID becomes the primary key in `user_profiles`. Each event's click count is summed at the storage layer via the Aggregation Merge Engine, no windowing or state in Flink required.

## Environment Setup

### Prerequisites

Before proceeding, ensure that [Docker](https://docs.docker.com/engine/install/) and the [Docker Compose plugin](https://docs.docker.com/compose/install/linux/) are installed.

### Starting the Playground

1. Create a working directory.
   ```shell
   mkdir fluss-user-profile
   cd fluss-user-profile
   ```

2. Set the version environment variables.
   ```shell
   export FLUSS_DOCKER_VERSION=0.9.0-incubating
   export FLINK_VERSION="1.20"
   ```
   :::note
   If you open a new terminal window, remember to re-run these export commands.
   :::

3. Create a `lib` directory and download the required JARs.
   ```shell
   mkdir lib

   # Download Flink Faker for data generation
   curl -fL -o lib/flink-faker-0.5.3.jar \
     https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar

   # Download Fluss Connector
   curl -fL -o "lib/fluss-flink-${FLINK_VERSION}-${FLUSS_DOCKER_VERSION}.jar" \
     "https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-${FLINK_VERSION}/${FLUSS_DOCKER_VERSION}/fluss-flink-${FLINK_VERSION}-${FLUSS_DOCKER_VERSION}.jar"
   ```

4. Verify both JARs downloaded correctly.
   ```shell
   ls -lh lib/
   ```
   You should see: `flink-faker-0.5.3.jar` and `fluss-flink-1.20-0.9.0-incubating.jar`.

5. Create the `docker-compose.yml` file using the heredoc command below to avoid indentation issues.
   ```shell
   cat > docker-compose.yml << 'EOF'
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
           remote.data.dir: /remote-data
       volumes:
         - fluss-remote-data:/remote-data
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
           remote.data.dir: /remote-data
       volumes:
         - fluss-remote-data:/remote-data
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
         - fluss-remote-data:/remote-data
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
         - fluss-remote-data:/remote-data
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
         - fluss-remote-data:/remote-data

   volumes:
     fluss-remote-data:
   EOF
   ```

6. Start the environment.
   ```shell
   docker compose up -d
   ```

7. Confirm all containers are running.
   ```shell
   docker compose ps
   ```
   You should see `coordinator-server`, `tablet-server`, `zookeeper`, `jobmanager`, and `taskmanager` all in the `running` state.

8. Launch the Flink SQL Client.
   ```shell
   docker compose run sql-client
   ```

## Step 1: Create the Fluss Catalog

In the SQL Client, run these statements one by one.

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

Create the `user_profiles` table using the **Aggregation Merge Engine**. Each user's UID is the primary key, and `total_clicks` accumulates their click activity directly at the storage layer via the `sum` aggregator.

```sql
CREATE TABLE user_profiles (
    uid          INT,
    total_clicks BIGINT,
    PRIMARY KEY (uid) NOT ENFORCED
) WITH (
    'table.merge-engine'      = 'aggregation',
    'fields.total_clicks.agg' = 'sum'
);
```

## Step 4: Ingest and Process Data

Create a temporary source table to simulate raw click events using the Faker connector.

:::note
Java Faker's `numberBetween(min, max)` treats `max` as exclusive. The expressions below are set to produce click counts of 1–10 and a pool of 100 distinct simulated email users.
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

Now run the pipeline. The `lookup.insert-if-not-exists` hint ensures that if an email is not found in `user_dict`, Fluss generates a new `uid` for it automatically. The resolved `uid` becomes the primary key of `user_profiles`, making the dictionary mapping the central link between the two tables.

```sql
INSERT INTO user_profiles
SELECT
    d.uid,
    CAST(e.click_count AS BIGINT)
FROM raw_events AS e
JOIN user_dict /*+ OPTIONS('lookup.insert-if-not-exists' = 'true') */
FOR SYSTEM_TIME AS OF e.proctime AS d
ON e.email = d.email;
```

## Step 5: Verify Results

Open a **second terminal**, re-run the export commands, and launch another SQL Client session to query results while the pipeline runs.

```shell
cd fluss-user-profile
export FLUSS_DOCKER_VERSION=0.9.0-incubating
export FLINK_VERSION="1.20"
docker compose run sql-client
```

Set up the catalog in the new session:

```sql
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
```

```sql
USE CATALOG fluss_catalog;
```

Query the aggregated profile table:

```sql
SET 'sql-client.execution.result-mode' = 'tableau';
```

```sql
SELECT uid, total_clicks FROM user_profiles;
```

You should see rows appearing for each new user, with `total_clicks` accumulating in real time as more events arrive for the same email.

To verify that email-to-UID mapping is working correctly:

```sql
SELECT * FROM user_dict LIMIT 10;
```

Each email should have a unique compact `INT` uid automatically assigned by Fluss.

## Clean Up

Exit the SQL Client by typing `exit;`, then stop the Docker containers.

```shell
docker compose down -v
```

## Architectural Benefits

- **Stateless Flink Jobs:** Offloading both the identity dictionary and the click aggregation to Fluss makes the Flink job lightweight, with fast checkpoints and minimal recovery time.
- **Compact Storage:** Using auto-incremented `INT` UIDs instead of raw email strings reduces memory and storage footprint significantly.
- **Exactly-Once Accuracy:** The **Undo Recovery** mechanism in the Fluss Flink connector ensures that replayed data during failovers does not result in double-counting.

## What's Next?

This quickstart demonstrates the core mechanics. For a deeper dive into real-time user profiling with bitmap-based unique visitor counting using the `rbm64` aggregator, see the [Real-Time Profiles blog post](/blog/realtime-profiles-fluss).