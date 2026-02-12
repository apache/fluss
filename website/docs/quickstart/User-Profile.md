---
title: Real-Time User Profile
sidebar_position: 4
---

# Real-Time User Profile

This tutorial demonstrates how to build a production-grade, real-time user profiling system. You will learn how to map high-cardinality string identifiers (like emails) to compact integers and aggregate user behavior directly in the storage layer with exactly-once guarantees.

![arch](/img/user_profile.png)

## How the System Works

* **Ingestion**: Raw event streams (e.g., clicks, page views) enter the system from a source like [Apache Kafka](https://kafka.apache.org/).

* **Identity Mapping**: The [Apache Flink](https://flink.apache.org/) job performs a temporal lookup join against the `user_dict` table. If a user is new, the `insert-if-not-exists` hint triggers Fluss to automatically generate a unique `INT` UID using its Auto-Increment feature.

* **Lightweight Processing**: Because the dictionary mapping and heavy aggregations are offloaded to Fluss, Flink remains a lightweight processing engine focused purely on routing and simple transformations.

* **Storage-Level Aggregation**: The generated UID and metrics are sent to the `user_profiles` table. The Aggregation Merge Engine handles the actual computation (e.g., summing clicks or merging [RoaringBitmaps](https://roaringbitmap.org/)) directly in the TabletServer storage layer.

* **Durable Integrity**: The Undo Recovery mechanism ensures that even if a Flink job fails and replays data, uncommitted changes in the storage layer are rolled back, maintaining exactly-once accuracy.

## The Scenario

In high-scale tracking, user IDs are often long strings (UUIDs or Emails). Directly aggregating these in a stream processor like [Apache Flink](https://flink.apache.org/) is expensive.

*   **The Problem:** Storing billions of strings in Flink state causes State Bloat and slows down checkpoints.
*   **The Solution:** Use **Auto-Increment** to create a compact "ID Dictionary" and the **Aggregation Merge Engine** to maintain metrics like "Total Clicks" and "Unique Visitor Bitmaps" directly in Fluss.

## Prerequisites

*   A running Fluss cluster.
*   A Flink environment with the Fluss connector installed.
*   A source table `raw_events` defined.

## Step 1: Create the User Dictionary Table

First, we create a table to store the stable mapping between a user's email and a compact system ID. Per best practices, we use `INT` for the `uid` to optimize storage and computation for downstream bitmaps.

```sql
CREATE TABLE user_dict (
    email STRING,
    uid INT, -- 32-bit INT is optimized for RoaringBitmaps
    PRIMARY KEY (email) NOT ENFORCED
) WITH (
    'table.type' = 'pk',
    'table.auto-increment.fields' = 'uid'
);
```

## Step 2: Create the Aggregated Profile Table

This table uses the Aggregation Merge Engine. Instead of Flink managing the state, Fluss pushes the logic (summing clicks and unioning bitmaps) down to the TabletServers.

We use `rbm64` ([RoaringBitmap](https://roaringbitmap.org/)) for efficient unique visitor counting.

```sql
CREATE TABLE user_profiles (
    profile_id INT,
    unique_visitors BYTES, -- Stores the aggregated RoaringBitmap
    total_clicks BIGINT,
    PRIMARY KEY (profile_id) NOT ENFORCED
) WITH (
    'table.type' = 'pk',
    'table.merge-engine' = 'aggregation',
    'fields.unique_visitors.agg' = 'rbm64',
    'fields.total_clicks.agg' = 'sum'
);
```

## Step 3: Process Data with Flink SQL

We connect the pipeline using a Lookup Join. The lookup.insert-if-not-exists hint ensures that if a user is missing, Fluss automatically generates a new uid and returns it to the join result.

```sql
INSERT INTO user_profiles
SELECT
    d.uid,
    -- TO_BINARY is required to convert the INT to the BYTES format 
    -- expected by the rbm64 aggregation engine.
    TO_BINARY(d.uid),
    CAST(e.click_count AS BIGINT)
FROM raw_events AS e
         JOIN user_dict /*+ OPTIONS('lookup.insert-if-not-exists' = 'true') */
    FOR SYSTEM_TIME AS OF e.proctime AS d
              ON e.email = d.email;
```

## Architectural Benefits

*   **Stateless Flink Jobs:** Offloading logic to Fluss makes Flink jobs lightweight, enabling faster checkpoints and nearly instant recovery.
*   **Compact Storage:** Using INT UIDs instead of STRING emails reduces the memory footprint for deduplication tasks by up to 90%.
*   **Exactly-Once Accuracy:** Even during failovers, the **Undo Recovery** mechanism ensures replayed data does not result in double-counting, maintaining perfect accuracy.
