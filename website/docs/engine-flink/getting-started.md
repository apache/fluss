---
sidebar_label: "Getting Started"
title: "Getting Started with Flink"
sidebar_position: 1
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

# Getting Started with Flink Engine
## Quick Start
For a quick introduction to running Flink, refer to the [Quick Start](quickstart/flink.md) guide.


## Support Flink Versions
| Fluss Connector Versions | Supported Flink Versions |
|--------------------------|--------------------------| 
| $FLUSS_VERSION_SHORT$    | 1.18, 1.19, 1.20         |


## Feature Support
Fluss supports Apache Flink's Table API and Flnk's DataStream API.

For Flink's Table API, Fluss supports the following features:

| Feature support                                   | Flink | Notes                                  |
|---------------------------------------------------|-------|----------------------------------------|
| [SQL create catalog](ddl.md#create-catalog)       | ✔️    |                                        |
| [SQl create database](ddl.md#create-database)     | ✔️    |                                        |
| [SQL drop database](ddl.md#drop-database)         | ✔️    |                                        |
| [SQL create table](ddl.md#create-table)           | ✔️    |                                        |
| [SQL create table like](ddl.md#create-table-like) | ✔️    |                                        |
| [SQL drop table](ddl.md#drop-table)               | ✔️    |                                        |
| [SQL show partitions](ddl.md#show-partitions)     | ✔️    |                                        |
| [SQL add partiiton](ddl.md#add-partition)         | ✔️    |                                        |
| [SQL drop partiiton](ddl.md#drop-partition)       | ✔️    |                                        |
| [SQL select](reads.md)                            | ✔️    | Support both streaming and batch mode. |
| [SQL limit](reads.md#limit-read)                  | ✔️    | Only for Log Table                     |
| [SQL insert into](writes.md)                      | ✔️    | Support both streaming and batch mode. |
| [SQL delete from](writes.md#delete-from)          | ✔️    | Only in batch mode.                    |
| [SQL update](writes.md#update)                    | ✔️    | Only in batch mode.                    |
| [SQL lookup join](lookups.md)                     | ✔️    |                                        |

For Flink's DataStream API, you can see [DataStream API](docs/engine-flink/datastream.mdx) for more details.

## Preparation when using Flink SQL Client
- **Download Flink**

Flink runs on all UNIX-like environments, i.e. Linux, Mac OS X, and Cygwin (for Windows).
If you haven’t downloaded Flink, you can download [the binary release](https://flink.apache.org/downloads.html) of Flink, then extract the archive with the following command.
```shell
tar -xzf flink-1.20.1-bin-scala_2.12.tgz
```
- **Copy Fluss Flink Bundled Jar**

Download [Fluss Flink Bundled jar](/downloads) and copy to the `lib` directory of your Flink home.

```shell
cp fluss-flink-$FLUSS_VERSION$.jar <FLINK_HOME>/lib/
```
:::note
If you use [Amazon S3](http://aws.amazon.com/s3/), [Aliyun OSS](https://www.aliyun.com/product/oss) or [HDFS(Hadoop Distributed File System)](https://hadoop.apache.org/docs/stable/) as Fluss's [remote storage](maintenance/tiered-storage/remote-storage.md),
you should download the corresponding [Fluss filesystem jar](/downloads#filesystem-jars) and also copy it to the lib directory of your Flink home.
:::

- **Start a local cluster**

To start a local cluster, run the bash script that comes with Flink:
```shell
<FLINK_HOME>/bin/start-cluster.sh
```
You should be able to navigate to the web UI at [localhost:8081](http://localhost:8081/) to view the Flink dashboard and see that the cluster is up and running. You can also check its status with the following command:
```shell
ps aux | grep flink
```
- **Start a sql client**

To quickly stop the cluster and all running components, you can use the provided script:
```shell
<FLINK_HOME>/bin/sql-client.sh
```


## Creating a Catalog
You can use the following SQL statement to create a catalog.
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'localhost:9123'
);
```

:::note
1. The `bootstrap.servers` means the Fluss server address. Before you config the `bootstrap.servers`,
   you should start the Fluss server first. See [Deploying Fluss](install-deploy/overview.md#how-to-deploy-fluss)
   for how to build a Fluss cluster.
   Here, it is assumed that there is a Fluss cluster running on your local machine and the CoordinatorServer port is 9123.
2. The` bootstrap.servers` configuration is used to discover all nodes within the Fluss cluster. It can be set with one or more (up to three) Fluss server addresses (either CoordinatorServer or TabletServer) separated by commas.
:::

## Creating a Table
```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

```sql title="Flink SQL"
CREATE TABLE pk_table (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT,
  PRIMARY KEY (shop_id, user_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4'
);
```

## Data Writing
To append new data to a table, you can use `INSERT INTO` in batch mode or streaming mode:
```sql title="Flink SQL"
-- Execute the flink job in batch mode for current session context
SET 'execution.runtime-mode' = 'batch';
```

```sql title="Flink SQL"
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';
```

```sql title="Flink SQL"
INSERT INTO pk_table VALUES
  (1234, 1234, 1, 1),
  (12345, 12345, 2, 2),
  (123456, 123456, 3, 3);
```

To update data record with the primary key (1234, 1234) in a Flink streaming job, use the UPDATE statement as follows:

```sql title="Flink SQL"
-- should run in batch mode
UPDATE pk_table SET total_amount = 4 WHERE shop_id = 1234 and user_id = 1234;
```

To delete the data record with primary key `(12345, 12345)`, use `DELETE FROM`:

```sql title="Flink SQL"
-- should run in batch mode
DELETE FROM pk_table WHERE shop_id = 12345 and user_id = 12345;
```

## Data Reading

To retrieve data with the primary key `(1234, 1234)`, you can perform a point query by applying a filter on the primary key:
```sql title="Flink SQL"
-- should run in batch mode
SELECT * FROM pk_table WHERE shop_id = 1234 and user_id = 1234;
```

To preview a subset of the data in a table, you can use a `LIMIT` clause.
```sql title="Flink SQL"
-- should run in batch mode
SELECT * FROM pk_table LIMIT 10;
```

Fluss supports processing incremental data reading in flink streaming jobs:
```sql title="Flink SQL"
-- Submit the flink job in streaming mode for current session.
SET 'execution.runtime-mode' = 'streaming';
```

```sql title="Flink SQL"
-- reading changelogs from the primary-key table from beginning.
SELECT * FROM pk_table /*+ OPTIONS('scan.startup.mode' = 'earliest') */;
```

## Type Conversion
Fluss's integration for Flink automatically converts between Flink and Fluss types.

### Fluss -> Apache Flink

| Fluss         | Flink         |
|---------------|---------------|
| BOOLEAN       | BOOLEAN       |
| TINYINT       | TINYINT       |
| SMALLINT      | SMALLINT      |
| INT           | INT           |
| BIGINT        | BIGINT        |
| FLOAT         | FLOAT         |
| DOUBLE        | DOUBLE        |
| CHAR          | CHAR          |
| STRING        | STRING        |
| DECIMAL       | DECIMAL       |
| DATE          | DATE          |
| TIME          | TIME          |
| TIMESTAMP     | TIMESTAMP     |
| TIMESTAMP_LTZ | TIMESTAMP_LTZ |
| BYTES         | BYTES         |

### Apache Flink -> Fluss

| Flink         | Fluss                                         | 
|---------------|-----------------------------------------------|
| BOOLEAN       | BOOLEAN                                       |       
| SMALLINT      | SMALLINT                                      |
| INT           | INT                                           |
| BIGINT        | BIGINT                                        |
| FLOAT         | FLOAT                                         |
| DOUBLE        | DOUBLE                                        |
| CHAR          | CHAR                                          |
| STRING        | STRING                                        |
| DECIMAL       | DECIMAL                                       |
| DATE          | DATE                                          |
| TIME          | TIME                                          |
| TIMESTAMP     | TIMESTAMP                                     |
| TIMESTAMP_LTZ | TIMESTAMP_LTZ                                 |
| BYTES         | BYTES                                         |
| VARCHAR       | Not supported, suggest to use STRING instead. |
| VARBINARY     | Not supported, suggest to use BYTES instead.  |
| INTERVAL      | Not supported                                 |
| ARRAY         | Not supported                                 |
| MAP           | Not supported                                 |
| MULTISET      | Not supported                                 |
| ROW           | Not supported                                 |
| RAW           | Not supported                                 |