---
sidebar_label: Flink
sidebar_position: 1
---

# Real-Time Analytics With Flink

This guide will get you up and running with Apache Flink to do real-time analytics, covering some powerful features of Fluss,
including integrating with Paimon.
The guide is derived from from [TPC-H](https://www.tpc.org/tpch/) **Q5**.

For more information on working with Flink, refer to the [Apache Flink Engine](engine-flink/getting-started.md) section.

## Environment Setup
### Prerequisites
Before proceeding with this guide, ensure that [Docker](https://docs.docker.com/engine/install/) is installed on your machine.

### Starting components required
We will use `docker-compose` to spin up all the required components for this tutorial.

1. Create a directory to serve as your working directory for this guide and add the `docker-compose.yml` file to it.

```shell
mkdir fluss-quickstart-flink
cd fluss-quickstart-flink
```

2. Create `docker-compose.yml` file with the following content:
```yaml
services:
  coordinator-server:
    image: fluss/fluss
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        coordinator.host: coordinator-server
        remote.data.dir: /tmp/fluss/remote-data
        lakehouse.storage: paimon
        paimon.catalog.metastore: filesystem
        paimon.catalog.warehouse: /tmp/paimon
  tablet-server:
    image: fluss/fluss
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        tablet-server.host: tablet-server
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        lakehouse.storage: paimon
        paimon.catalog.metastore: filesystem
        paimon.catalog.warehouse: /tmp/paimon
    volumes:
      - shared-tmpfs:/tmp/fluss/remote-data
  zookeeper:
    restart: always
    image: zookeeper:3.8.4

  jobmanager:
    image: fluss/quickstart-flink
    ports:
      - "8083:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - shared-tmpfs:/tmp/paimon
  taskmanager:
    image: fluss/quickstart-flink
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.framework.off-heap.size: 256m
    volumes:
      - shared-tmpfs:/tmp/paimon

volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```

The Docker Compose environment consists of the following containers:
- **Fluss Cluster:** a Fluss `CoordinatorServer`, a Fluss `TabletServer` and a `ZooKeeper` server.
- **Flink Cluster**: a Flink `JobManager` and a Flink `TaskManager` container to execute queries.

**Note:** The `fluss/quickstart-flink` image is based on [flink:1.20.0-java17](https://hub.docker.com/layers/library/flink/1.20-java17/images/sha256-381ed7399c95b6b03a7b5ee8baca91fd84e24def9965ce9d436fb22773d66717) and
includes the [fluss-connector-flink](engine-flink/getting-started.md), [paimon-flink](https://paimon.apache.org/docs/0.8/flink/quick-start/) and
[flink-connector-faker](https://flink-packages.org/packages/flink-faker) to simplify this guide.

3. To start all containers, run the following command in the directory that contains the `docker-compose.yml` file:
```shell
docker-compose up -d
```
This command automatically starts all the containers defined in the Docker Compose configuration in a detached mode.
Run `docker ps` to check whether these containers are running properly.

You can also visit http://localhost:8083/ to see if Flink is running normally.

:::note
- If you want to run with your own Flink environment, remember to download the [fluss-connector-flink](engine-flink/getting-started.md), [flink-connector-faker](https://github.com/knaufk/flink-faker/releases), [paimon-flink](https://paimon.apache.org/docs/0.8/flink/quick-start/) connector jars and then put them to `FLINK_HOME/lib/`.
- All the following commands involving docker-compose should be executed in the directory of the `docker-compose.yml` file.
  :::

## Enter into SQL-Client
First, use the following command to enter the Flink SQL CLI Container:
```shell
docker-compose exec jobmanager ./sql-client
```

**Note**:
To simplify this guide, three temporary tables have been pre-created with `faker` to generate data. You can view their schemas by running the following commands: `DESCRIBE TABLE source_customer`, `DESCRIBE TABLE source_order`, and `DESCRIBE TABLE source_nation`.

## Create Fluss Tables
### Create Fluss Catalog
Use the following SQL to create a Fluss catalog:
```sql title="Flink SQL Client"
CREATE CATALOG my_fluss WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG my_fluss;
```

### Create Tables
Running the following SQL to create Fluss tables to be used in this guide:
```sql  title="Flink SQL Client"
CREATE TABLE fluss_order (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `ptime` AS PROCTIME(),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);

CREATE TABLE fluss_customer (
    `cust_key` INT NOT NULL,
    `name` STRING,
    `phone` STRING,
    `nation_key` INT NOT NULL,
    `acctbal` DECIMAL(15, 2),
    `mktsegment` STRING,
    PRIMARY KEY (`cust_key`) NOT ENFORCED
);

CREATE TABLE `fluss_nation` (
  `nation_key` INT NOT NULL,
  `name`       STRING,
   PRIMARY KEY (`nation_key`) NOT ENFORCED
);

CREATE TABLE enriched_orders (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `cust_name` STRING,
    `cust_phone` STRING,
    `cust_acctbal` DECIMAL(15, 2),
    `cust_mktsegment` STRING,
    `nation_name` STRING,
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```

## Streaming into Fluss

First, run the following sql to sync data from source tables to Fluss tables:
```sql  title="Flink SQL Client"
EXECUTE STATEMENT SET
BEGIN
    INSERT INTO fluss_nation SELECT * FROM `default_catalog`.`default_database`.source_nation;
    INSERT INTO fluss_customer SELECT * FROM `default_catalog`.`default_database`.source_customer;
    INSERT INTO fluss_order SELECT * FROM `default_catalog`.`default_database`.source_order;
END;
```

Fluss primary-key tables support high QPS point lookup on primary keys, so it's efficient to
[lookup join](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/joins/#lookup-join)
primary-key tables `fluss_customer` and `fluss_nation` to enrich the `fluss_orders` table.

```sql  title="Flink SQL Client"
INSERT INTO enriched_orders
SELECT o.order_key, 
       o.cust_key, 
       o.total_price,
       o.order_date, 
       o.order_priority,
       o.clerk,
       c.name,
       c.phone,
       c.acctbal, 
       c.mktsegment,
       n.name
FROM fluss_order o 
LEFT JOIN fluss_customer FOR SYSTEM_TIME AS OF `o`.`ptime` AS `c` 
    ON o.cust_key = c.cust_key
LEFT JOIN fluss_nation FOR SYSTEM_TIME AS OF `o`.`ptime` AS `n` 
    ON c.nation_key = n.nation_key;
```


## Query on Fluss Tables
Now, you can have a real-time analytics on Fluss tables. For example, to count the number of orders of one
particular customer, running the following sql to see the real-time result.

```sql  title="Flink SQL Client"
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';
    
-- switch to batch mode
SET 'execution.runtime-mode' = 'batch';
    
-- use limit to query the enriched_orders table
SELECT * FROM enriched_orders LIMIT 2;
```

The result looks like:
```
+-----------+----------+-------------+------------+----------------+--------+------------+----------------+--------------+-----------------+-------------+
| order_key | cust_key | total_price | order_date | order_priority |  clerk |  cust_name |     cust_phone | cust_acctbal | cust_mktsegment | nation_name |
+-----------+----------+-------------+------------+----------------+--------+------------+----------------+--------------+-----------------+-------------+
|  23199744 |        9 |      266.44 | 2024-08-29 |           high | Clerk1 |   Joe King |   908.207.8513 |       124.28 |       FURNITURE |      JORDAN |
|  10715776 |        2 |      924.43 | 2024-11-04 |         medium | Clerk3 | Rita Booke | (925) 775-0717 |       172.39 |       FURNITURE |      UNITED |
+-----------+----------+-------------+------------+----------------+--------+------------+----------------+--------------+-----------------+-------------+
```

You may be interested in the particular customer, you can lookup it by `cust_key` with the following SQL:
```sql title="Flink SQL Client"
-- lookup by primary key
SELECT * FROM fluss_customer WHERE `cust_key` = 1;
```
The result looks like:
```shell
+----------+---------------+--------------+------------+---------+------------+
| cust_key |          name |        phone | nation_key | acctbal | mktsegment |
+----------+---------------+--------------+------------+---------+------------+
|        1 | Al K. Seltzer | 817-617-7960 |          1 |  533.41 | AUTOMOBILE |
+----------+---------------+--------------+------------+---------+------------+
```

The result should be returned quickly since Fluss supports fast lookup by primary key for primary key table.

## Update/Delete rows on Fluss Tables

You can use `UPDATE` and `DELETE` statement to update/delete rows on Fluss tables.
### Update
```sql title="Flink SQL Client"
-- update by primary key
UPDATE fluss_customer SET `name` = 'fluss_updated' WHERE `cust_key` = 1;
```
Then you can lookup the row:
```sql title="Flink SQL Client"
SELECT * FROM fluss_customer WHERE `cust_key` = 1;
```
The result looks like:
```shell
+----------+---------------+--------------+------------+---------+------------+
| cust_key |          name |        phone | nation_key | acctbal | mktsegment |
+----------+---------------+--------------+------------+---------+------------+
|        1 | fluss_updated | 817-617-7960 |          1 |  533.41 | AUTOMOBILE |
+----------+---------------+--------------+------------+---------+------------+
```
The `name` column is updated to `fluss_updated`.

### Delete
```sql title="Flink SQL Client
DELETE FROM fluss_customer WHERE `cust_key` = 1;
```
Then, you should get empty set if lookup the row by the following SQL:
```sql title="Flink SQL Client"
SELECT * FROM fluss_customer WHERE `cust_key` = 1;
```

## Integrate with Paimon
### Start lakehouse tiering service
To integrate with Paimon, you must start the lakehouse tiering service firstly. Open a new terminal and change working directory to `fluss-quickstart-flink`.
Run the following command in the working directly `fluss-quickstart-flink` to start the lakehouse tiering service:
```shell
docker-compose exec coordinator-server ./bin/lakehouse.sh -D flink.rest.address=jobmanager -D flink.rest.port=8081 -D flink.execution.checkpointing.interval=30s
```
You should see a Flink Job named `fluss-paimon-tiering-service` running in the [Flink Web UI](http://localhost:8083/).

### Streaming into Fluss datalake-enabled tables
By default, table is datalake disabled, so the lakehouse tiering service won't tier the data of the table to datalake.

You must create a table with table option `table.datalake.enabled = true` to enabled lakehouse as a tiered storage for the table.
Back to sql-client, run the following SQL to create a datalake-enabled table
```sql  title="Flink SQL Client"
CREATE TABLE datalake_enriched_orders (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `cust_name` STRING,
    `cust_phone` STRING,
    `cust_acctbal` DECIMAL(15, 2),
    `cust_mktsegment` STRING,
    `nation_name` STRING,
    PRIMARY KEY (`order_key`) NOT ENFORCED
) WITH ('table.datalake.enabled' = 'true');
```

Then, streaming writing data to the datalake-enabled table `datalake_enriched_orders`:
```sql  title="Flink SQL Client"
-- switch to streaming mode
SET 'execution.runtime-mode' = 'streaming';

INSERT INTO datalake_enriched_orders
SELECT o.order_key,
       o.cust_key,
       o.total_price,
       o.order_date,
       o.order_priority,
       o.clerk,
       c.name,
       c.phone,
       c.acctbal,
       c.mktsegment,
       n.name
FROM fluss_order o
       LEFT JOIN fluss_customer FOR SYSTEM_TIME AS OF `o`.`ptime` AS `c`
                 ON o.cust_key = c.cust_key
       LEFT JOIN fluss_nation FOR SYSTEM_TIME AS OF `o`.`ptime` AS `n`
                 ON c.nation_key = n.nation_key;
```

### Real-Time Analytics on Fluss datalake-enabled tables
Now, the data of the table `datalake_enriched_orders` is in Fluss(for rel-time data) and Paimon(for historical data).

When you query with specifying table `datalake_enriched_orders`, Fluss will union the data in Fluss and Paimon to get the full result.

In the case you only want to query the data in Paimon directly which is high performance without extra union, you can specifying table `datalake_enriched_orders$lake` with `$lake` suffix.
With that, you will also get all the optimization and features of a Flink Paimon table source, including [system table](https://paimon.apache.org/docs/master/concepts/system-tables/) using with `datalake_enriched_orders$snapshots`, etc.

Use the following SQL to query the snapshots on Paimon:
```sql  title="Flink SQL Client"
-- switch to batch mode
SET 'execution.runtime-mode' = 'batch';

-- to query snapshots in paimon
SELECT snapshot_id, total_record_count FROM datalake_enriched_orders$lake$snapshots;
```
The result looks like:
```shell
+-------------+--------------------+
| snapshot_id | total_record_count |
+-------------+--------------------+
|           1 |                650 |
+-------------+--------------------+
```
If it return empty, you may need to wait for checkpoint finish, around 30s.

Then, you can run the following SQL to do analytics on Paimon data:
```sql  title="Flink SQL Client"
-- to sum prices of all orders in paimon
SELECT sum(total_price) as sum_price FROM datalake_enriched_orders$lake;
```
The result looks like:
```shell
+------------+
|  sum_price |
+------------+
| 1669519.92 |
+------------+
```

If want to result with sub-second data freshness, you can query the table directly with union Fluss and Paimon data:
```sql  title="Flink SQL Client"
-- to sum prices of all orders in fluss and paimon
SELECT sum(total_price) as sum_price FROM datalake_enriched_orders;
```
The result looks like:
```
+------------+
|  sum_price |
+------------+
| 1777908.36 |
+------------+
```
You can run the real-time analytics query multi-times, the result should be different in every one run since the data are written to Fluss in real-time.


At last, you can use the following command to see the files in paimon:
```shell
docker-compose exec taskmanager tree /tmp/paimon/fluss.db
```
It looks like:
```shell
/tmp/paimon/fluss.db
└── datalake_enriched_orders
    ├── bucket-0
    │   ├── changelog-aef1810f-85b2-4eba-8eb8-9b136dec5bdb-0.orc
    │   └── data-aef1810f-85b2-4eba-8eb8-9b136dec5bdb-1.orc
    ├── manifest
    │   ├── manifest-aaa007e1-81a2-40b3-ba1f-9df4528bc402-0
    │   ├── manifest-aaa007e1-81a2-40b3-ba1f-9df4528bc402-1
    │   ├── manifest-list-ceb77e1f-7d17-4160-9e1f-f334918c6e0d-0
    │   ├── manifest-list-ceb77e1f-7d17-4160-9e1f-f334918c6e0d-1
    │   └── manifest-list-ceb77e1f-7d17-4160-9e1f-f334918c6e0d-2
    ├── schema
    │   └── schema-0
    └── snapshot
        ├── EARLIEST
        ├── LATEST
        └── snapshot-1
```
It's standard format of Paimon which enables you query on it with other engines, like [StartRocks](https://docs.starrocks.io/docs/data_source/catalog/paimon_catalog/).


## Clean up
After finishing the tutorial, run `exit` to exit Flink SQL CLI Container and then run `docker-compose down -v` to stop all containers.

## Learn more
Now that you're up an running with Fluss and Flink, check out the [Apache Flink Engine](engine-flink/getting-started.md) docs to learn more features with Flink!