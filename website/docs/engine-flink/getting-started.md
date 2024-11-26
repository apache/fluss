---
sidebar_label: "Getting Started"
sidebar_position: 1
---

# Getting Started with Flink Engine
## Quick Start
For a quick introduction to running Flink, refer to the [Quick Start](../quickstart/flink.md) guide.


## Support Flink Versions
| Fluss Connector Versions | Supported Flink Versions |
|--------------------------|--------------------------| 
| 0.5                      | 1.18, 1.19, 1.20         |


## Feature Support
Fluss only supports Apache Flink's Table API.


| Feature support                                   | Flink | Notes                                  |
|---------------------------------------------------|-------|----------------------------------------|
| [SQL create catalog](ddl.md#create-catalog)       | ✔️    |                                        |
| [SQl create database](ddl.md#create-database)     | ✔️    |                                        |
| [SQL drop database](ddl.md#drop-database)         | ✔️    |                                        |
| [SQL create table](ddl.md#create-table)           | ✔️    |                                        |
| [SQL create table like](ddl.md#create-table-like) | ✔️    |                                        |
| [SQL drop table](ddl.md#drop-table)               | ✔️    |                                        |                                                                   |
| [SQL select](reads.md)                            | ✔️    | Support both streaming and batch mode. |
| [SQL insert into](writes.md)                      | ✔️    | Support both streaming and batch mode. |
| [SQL lookup join](lookups.md)                     | ✔️    |                                        |

## Preparation when using Flink SQL Client
- **Download Flink**

Flink runs on all UNIX-like environments, i.e. Linux, Mac OS X, and Cygwin (for Windows).
If you haven’t downloaded Flink, you can download [the binary release](https://flink.apache.org/downloads.html) of Flink, then extract the archive with the following command.
```shell
tar -xzf flink-*.tgz
```

- **Copy Fluss Connector Jar**

Download [Fluss connector jar](/downloads) and copy to the lib directory of your Flink home.

```shell
cp fluss-connector-flink-*.jar <FLINK_HOME>/lib/
```

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


## Adding catalogs
A catalog is created and named by executing the following query (replace `<catalog_name>` with your catalog name):
```sql title="Flink SQL Client"
CREATE CATALOG fluss_catalog WITH (
  'type'='fluss',
  'bootstrap.servers' = 'fluss-server-1:9123'
);
```

The `bootstrap.servers` is used to discover all the nodes in the Fluss cluster. You can configure it with 1 or 3 Fluss server (either CoordinatorServer or TabletServer) addresses using comma separated.


## Creating a table
```sql title="Flink SQL Client"
USE CATALOG `fluss_catalog`;

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

## Writing
To append new data to a table, you can use `INSERT INTO` in batch mode or streaming mode:
```sql title="Flink SQL Client"
-- Execute the flink job in batch mode for current session context
SET 'execution.runtime-mode' = 'batch';
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';

INSERT INTO pk_table VALUES
  (1234, 1234, 1, 1),
  (12345, 12345, 2, 2),
  (123456, 123456, 3, 3);
```

To update the data whose primary key is `(1234, 1234)` and  a Flink streaming job, use `UPDATE`:
```sql title="Flink SQL Client"
-- should run in batch mode
UPDATE pk_table SET total_amount = 4 WHERE shop_id = 1234 and user_id = 1234;
```

To delete the data whose primary key is `(12345, 12345)`, use `DELETE FROM`:

```sql title="Flink SQL Client"
-- should run in batch mode
DELETE FROM pk_table WHERE shop_id = 12345 and user_id = 12345;
```

## Reading

To lookup the date whose primary key is `(1234, 1234)`, you can perform a point query by applying a filter on primary key:

```sql title="Flink SQL Client"
-- should run in batch mode
SELECT * FROM pk_table WHERE shop_id = 1234 and user_id = 1234;
```

To preview some data in a table, you can use the LIMIT query:
```sql title="Flink SQL Client"
-- should run in batch mode
SELECT * FROM pk_table LIMIT 10;
```

Fluss supports processing incremental data in flink streaming jobs which starts from a timestamp:
```sql title="Flink SQL Client"
-- Submit the flink job in streaming mode for current session.
SET 'execution.runtime-mode' = 'streaming';
-- reading changelogs from the primary-key table since 2023-12-09 00:00:00
SELECT * FROM pk_table /*+ OPTIONS('scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp' = '2023-12-09 00:00:00') */;
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

| Flink         | Fluss          | 
|---------------|----------------|
| BOOLEAN       | BOOLEAN        |       
| SMALLINT      | SMALLINT       |
| INT           | INT            |
| BIGINT        | BIGINT         |
| FLOAT         | FLOAT          |
| DOUBLE        | DOUBLE         |
| CHAR          | CHAR           |
| STRING        | STRING         |
| DECIMAL       | DECIMAL        |
| DATE          | DATE           |
| TIME          | TIME           |
| TIMESTAMP     | TIMESTAMP      |
| TIMESTAMP_LTZ | TIMESTAMP_LTZ  |
| BYTES         | BYTES          |
| VARCHAR       | Not supported, suggest to use STRING instead.  |
| VARBINARY     | Not supported, suggest to use BYTES instead.  |
| INTERVAL      | Not supported  |
| ARRAY         | Not supported  |
| MAP           | Not supported  |
| MULTISET      | Not supported  |
| ROW           | Not supported  |
| RAW           | Not supported  |