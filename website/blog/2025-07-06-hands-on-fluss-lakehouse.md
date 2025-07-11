---
slug: hands-on-fluss-lakehouse
title: "Hands-on Fluss Lakehouse"
authors: [gyang94]
toc_max_heading_level: 5
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Hands-on Fluss Lakehouse

Fluss persists historical data in a lakehouse storage layer while keeping real-time data in the Fluss server. A built-in tiering service continuously syncs fresh events into the lakehouse—enabling multiple query engines to run analytics over both hot and cold data. Fluss’s union-read feature then allows Flink jobs to transparently query from both the Fluss cluster and the lakehouse for seamless real-time processing.

![](assets/hands_on_fluss_lakehouse/streamhouse.png)

In this tutorial, we’ll show you how to build a local Fluss lakehouse environment, perform essential data operations, and get hands-on experience with the end-to-end Fluss lakehouse architecture.

## Integrate the Lakehouse Locally

We’ll use **Fluss 0.7** and **Flink 1.20** to run the tiering service on a local cluster, with **Paimon** as the lake format. Follow these steps:

### Fluss Cluster Setup

1. Download Fluss

Get the Fluss 0.7 binary release from the [official site](https://alibaba.github.io/fluss-docs/downloads/).

2. Configure the Data Lake

Edit `<FLUSS_HOME>/conf/server.yaml` and add:

```java
data.dir: /tmp/fluss-data
remote.data.dir: /tmp/fluss-remote-data

datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/fluss-paimon-data
```

3. Start Fluss

```java
<FLUSS_HOME>/bin/local-cluster.sh start
```

### Flink Cluster Setup

1. Download Flink

Grab the flink 1.20 binary package from the [Flink downloads page](https://flink.apache.org/downloads/).

2. Add the Fluss Connector

Download `fluss-flink-1.20-0.7.0.jar` from the [Fluss site](https://alibaba.github.io/fluss-docs/downloads/) and copy it into:

```java
<FLINK_HOME>/lib
```

3. Add Paimon Support

- Download `paimon-flink-1.20-1.01.jar` from the [Paimon project site](https://paimon.apache.org/docs/1.0/project/download/) into `<FLINK_HOME>/lib`.
- Copy the Paimon plugin jars from Fluss into `<FLINK_HOME>/lib` .

```java
<FLUSS_HOME>/plugins/paimon/fluss-lake-paimon-0.7.0.jar
<FLUSS_HOME>/plugins/paimon/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
```


4. Increase Task Slots

Edit `<FLINK_HOME>/conf/config.yaml`:

```java
// e.g. increase the slots number to 5
numberOfTaskSlots: 5 
```

5. Start Flink

```java
<FLINK_HOME>/bin/start-cluster.sh
```

6. Verify

Open your browser to `http://localhost:8081/` and confirm the cluster is up.

### Launching the Tiering Service

1. Get the Tiering Job Jar

Download the `fluss-flink-tiering-0.7.0.jar` .

2. Submit the Job

```
<FLINK_HOME>/bin/flink run \
    <path_to_jar>/fluss-flink-tiering-0.7.0.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/fluss-paimon-data
```

3. Confirm Deployment

In the Flink UI, look for the **Fluss Lake Tiering Service** job. Once it’s running, your local tiering pipeline is operational.

![](assets/hands_on_fluss_lakehouse/tiering-serivce-job.png)

## Data Processing

In this section, we’ll use the Flink SQL Client to interact with our Fluss lakehouse and run both batch and streaming queries.

1. Launch the SQL Client

```
<FLINK_HOME>/bin/sql-client.sh
```

2. Create the Catalog and Table

```sql
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',   
    'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;

CREATE TABLE t_user (
    `id` BIGINT,
    `name` string NOT NULL,
    `age` int,
    `birth` DATE,
    PRIMARY KEY (`id`) NOT ENFORCED
)WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

3. Write data

Inserts two records.

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

INSERT INTO t_user(id,name,age,birth) VALUES
(1,'Alice',18,DATE '2000-06-10'),
(2,'Bob',20,DATE '2001-06-20');
```

4. Union Read

You can run a simple SQL query to get data from this table. By default, Flink will union data from both the Fluss cluster and the lakehouse:

```sql

Flink SQL> select * from t_user;
+----+-------+-----+------------+
| id |  name | age |      birth |
+----+-------+-----+------------+
|  1 | Alice |  18 | 2000-06-10 |
|  2 |   Bob |  20 | 2001-06-20 |
+----+-------+-----+------------+
```

Users can read data only from the lake table, by appending a `$lake`  after the table name.

```sql
Flink SQL> select * from t_user$lake;
+----+-------+-----+------------+----------+----------+----------------------------+
| id |  name | age |      birth | __bucket | __offset |                __timestamp |
+----+-------+-----+------------+----------+----------+----------------------------+
|  1 | Alice |  18 | 2000-06-10 |        0 |       -1 | 1970-01-01 07:59:59.999000 |
|  2 |   Bob |  20 | 2001-06-20 |        0 |       -1 | 1970-01-01 07:59:59.999000 |
+----+-------+-----+------------+----------+----------+----------------------------+
```

The two records appear in data lake. The tiering service works for syncing data from Fluss to data lake.

Notice that in the paimon lake table, there are three system columns defined there: `__bucket`, `__offset` and `__timestamp` . The __bucket column shows which bucket this row in. The __offset and __timestamp columns are used for streaming mode data processing.

5. Streaming Inserts

Let’s switch to streaming mode and insert two new records.

```sql
Flink SQL> SET 'execution.runtime-mode' = 'streaming';

Flink SQL> INSERT INTO t_user(id,name,age,birth) VALUES
(3,'Catlin',25,DATE '2002-06-10'),
(4,'Dylan',28,DATE '2003-06-20');
```

Query the lake:

```sql

Flink SQL> select * from t_user$lake;
+----+-------+-----+------------+----------+----------+----------------------------+
| id |  name | age |      birth | __bucket | __offset |                __timestamp |
+----+-------+-----+------------+----------+----------+----------------------------+
|  1 | Alice |  18 | 2000-06-10 |        0 |       -1 | 1970-01-01 07:59:59.999000 |
|  2 |   Bob |  20 | 2001-06-20 |        0 |       -1 | 1970-01-01 07:59:59.999000 |
+----+-------+-----+------------+----------+----------+----------------------------+


Flink SQL> select * from t_user$lake;
+----+--------+-----+------------+----------+----------+----------------------------+
| id |   name | age |      birth | __bucket | __offset |                __timestamp |
+----+--------+-----+------------+----------+----------+----------------------------+
|  1 |  Alice |  18 | 2000-06-10 |        0 |       -1 | 1970-01-01 07:59:59.999000 |
|  2 |    Bob |  20 | 2001-06-20 |        0 |       -1 | 1970-01-01 07:59:59.999000 |
|  3 | Catlin |  25 | 2002-06-10 |        0 |        2 | 2025-06-24 23:16:46.740000 |
|  4 |  Dylan |  28 | 2003-06-20 |        0 |        3 | 2025-06-24 23:16:46.740000 |
+----+--------+-----+------------+----------+----------+----------------------------+

```

In the first time, the two new records have not been synced into lake table. After waiting for a while they appear in the second time query.

The __offset and __timestamp columns for these two records are not the default values now. It shows the offset and timestamp when the records entered into the table.

6. Inspect the Paimon Files

On your local filesystem, you can verify the Parquet files and manifest under `/tmp/fluss-paimon-data` .

```
/tmp/fluss-paimon-data ❯ tree .                                  
.
├── default.db
└── fluss.db
    └── t_user
        ├── bucket-0
        │         ├── changelog-bd4303c8-80f1-4b7b-9dc6-c6a3ce96aa47-0.parquet
        │         ├── changelog-e0209369-16ba-462c-af0a-815f57d72553-0.parquet
        │         ├── data-bd4303c8-80f1-4b7b-9dc6-c6a3ce96aa47-1.parquet
        │         └── data-e0209369-16ba-462c-af0a-815f57d72553-1.parquet
        ├── manifest
        │         ├── manifest-bbb24f9d-a4b9-4ea6-82ef-97b16c7f23d8-0
        │         ├── manifest-bbb24f9d-a4b9-4ea6-82ef-97b16c7f23d8-1
        │         ├── manifest-e7bf8240-a442-40f3-8b82-8ff5619dc7e9-0
        │         ├── manifest-e7bf8240-a442-40f3-8b82-8ff5619dc7e9-1
        │         ├── manifest-list-8e23b5ce-0b39-407f-b8ba-7aaaee629154-0
        │         ├── manifest-list-8e23b5ce-0b39-407f-b8ba-7aaaee629154-1
        │         ├── manifest-list-8e23b5ce-0b39-407f-b8ba-7aaaee629154-2
        │         ├── manifest-list-e10b824d-1a07-47a2-8f91-b12c170cfc43-0
        │         ├── manifest-list-e10b824d-1a07-47a2-8f91-b12c170cfc43-1
        │         └── manifest-list-e10b824d-1a07-47a2-8f91-b12c170cfc43-2
        ├── schema
        │         └── schema-0
        └── snapshot
            ├── LATEST
            ├── snapshot-1
            └── snapshot-2
```

7. View Snapshots

Users can also check the snapshots from the system table, by appending `$lake$snapshots` after thefluss table name.

```
Flink SQL> select * from t_user$lake$snapshots;
+-------------+-----------+----------------------+-------------------------+-------------+----------+
| snapshot_id | schema_id |          commit_user |             commit_time | commit_kind | ...      |
+-------------+-----------+----------------------+-------------------------+-------------+----------+
|           1 |         0 | __fluss_lake_tiering | 2025-06-24 00:42:25.615 |      APPEND | ...      |
|           2 |         0 | __fluss_lake_tiering | 2025-06-24 23:17:45.341 |      APPEND | ...      |
+-------------+-----------+----------------------+-------------------------+-------------+----------+
2 rows in set (0.33 seconds)
```

## Summary

In this guide, we gave a concise overview of the Fluss lakehouse architecture, walked through a step-by-step local setup of both Fluss and Flink clusters, and showcased practical data-processing examples using real-time and historical data. Those steps give you practical experience with Fluss’s lakehouse architecture in a local environment.