---
title: Real-Time Analytics with Flink (Iceberg)
sidebar_position: 2
---

# Real-Time Analytics With Flink (Iceberg)

This guide will get you up and running with Apache Flink to do real-time analytics, covering some powerful features of Fluss,
including integrating with Apache Iceberg.
The guide is derived from [TPC-H](https://www.tpc.org/tpch/) **Q5**.

For more information on working with Flink, refer to the [Apache Flink Engine](engine-flink/getting-started.md) section.

## Environment Setup
### Prerequisites

Before proceeding with this guide, ensure that [Docker](https://docs.docker.com/engine/install/) and the [Docker Compose plugin](https://docs.docker.com/compose/install/linux/) are installed on your machine.
All commands were tested with Docker version 27.4.0 and Docker Compose version v2.30.3.

:::note
We encourage you to use a recent version of Docker and [Compose v2](https://docs.docker.com/compose/releases/migrate/) (however, Compose v1 might work with a few adaptions).
:::

### Starting required components

We will use `docker compose` to spin up the required components for this tutorial.

1. Create a working directory for this guide.

```shell
mkdir fluss-quickstart-flink-iceberg
cd fluss-quickstart-flink-iceberg
```

2. Create a `lib` directory and download the required Hadoop jar file:

```shell
mkdir lib
wget -O lib/hadoop-apache-3.3.5-2.jar https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar
```

This jar file provides Hadoop 3.3.5 dependencies required for Iceberg's Hadoop catalog integration.

:::info
The `lib` directory serves as a staging area for additional jars needed by the Fluss coordinator server. The docker-compose configuration (see step 3) mounts this directory and copies all jars to `/opt/fluss/plugins/iceberg/` inside the coordinator container at startup.

You can add more jars to this `lib` directory based on your requirements:
- **Cloud storage support**: For AWS S3 integration with Iceberg, add the corresponding Iceberg bundle jars (e.g., `iceberg-aws-bundle`)
- **Custom Hadoop configurations**: Add jars for specific HDFS distributions or custom authentication mechanisms
- **Other catalog backends**: Add jars needed for alternative Iceberg catalog implementations (e.g., Rest, Hive, Glue)

Any jar placed in the `lib` directory will be automatically loaded by the Fluss coordinator server, making it available for Iceberg integration.
:::

3. Create a `docker-compose.yml` file with the following content:


```yaml
services:
  zookeeper:
    restart: always
    image: zookeeper:3.9.2

  coordinator-server:
    image: apache/fluss:$FLUSS_DOCKER_VERSION$
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://coordinator-server:9123
        remote.data.dir: /tmp/fluss/remote-data
        datalake.format: iceberg
        datalake.iceberg.type: hadoop
        datalake.iceberg.warehouse: /tmp/iceberg
    volumes:
      - shared-tmpfs:/tmp/iceberg
      - ./lib:/tmp/lib
    entrypoint: ["sh", "-c", "cp -v /tmp/lib/*.jar /opt/fluss/plugins/iceberg/ && exec /docker-entrypoint.sh coordinatorServer"]

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
        remote.data.dir: /tmp/fluss/remote-data
        kv.snapshot.interval: 0s
        datalake.format: iceberg
        datalake.iceberg.type: hadoop
        datalake.iceberg.warehouse: /tmp/iceberg
    volumes:
      - shared-tmpfs:/tmp/iceberg

  jobmanager:
    image: apache/fluss-quickstart-flink:1.20-$FLUSS_DOCKER_VERSION$
    ports:
      - "8083:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - shared-tmpfs:/tmp/iceberg

  taskmanager:
    image: apache/fluss-quickstart-flink:1.20-$FLUSS_DOCKER_VERSION$
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
      - shared-tmpfs:/tmp/iceberg

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

**Note:** The `apache/fluss-quickstart-flink` image is based on [flink:1.20.1-java17](https://hub.docker.com/layers/library/flink/1.20-java17/images/sha256:bf1af6406c4f4ad8faa46efe2b3d0a0bf811d1034849c42c1e3484712bc83505) and
includes the [fluss-flink](engine-flink/getting-started.md), [iceberg-flink](https://iceberg.apache.org/docs/latest/flink/) and
[flink-connector-faker](https://flink-packages.org/packages/flink-faker) to simplify this guide.

3. To start all containers, run:
```shell
docker compose up -d
```
This command automatically starts all the containers defined in the Docker Compose configuration in detached mode.

Run
```shell
docker container ls -a
```
to check whether all containers are running properly.

You can also visit http://localhost:8083/ to see if Flink is running normally.

:::note
- If you want to additionally use an observability stack, follow one of the provided quickstart guides [here](maintenance/observability/quickstart.md) and then continue with this guide.
- If you want to run with your own Flink environment, remember to download the [fluss-flink connector jar](/downloads), [flink-connector-faker](https://github.com/knaufk/flink-faker/releases) and [iceberg-flink connector jar](https://iceberg.apache.org/docs/latest/flink/) and then put them to `FLINK_HOME/lib/`.
- All the following commands involving `docker compose` should be executed in the created working directory that contains the `docker-compose.yml` file.
:::

Congratulations, you are all set!

## Enter into SQL-Client
First, use the following command to enter the Flink SQL CLI Container:
```shell
docker compose exec jobmanager ./sql-client
```

**Note**:
To simplify this guide, three temporary tables have been pre-created with `faker` connector to generate data.
You can view their schemas by running the following commands:

```sql title="Flink SQL"
SHOW CREATE TABLE source_customer;
```

```sql title="Flink SQL"
SHOW CREATE TABLE source_order;
```

```sql title="Flink SQL"
SHOW CREATE TABLE source_nation;
```

## Create Fluss Tables
### Create Fluss Catalog
Use the following SQL to create a Fluss catalog:
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
```

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

:::info
By default, catalog configurations are not persisted across Flink SQL client sessions.
For further information how to store catalog configurations, see [Flink's Catalog Store](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/catalogs/#catalog-store).
:::

### Create Tables
Running the following SQL to create Fluss tables to be used in this guide:
```sql  title="Flink SQL"
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
```

```sql  title="Flink SQL"
CREATE TABLE fluss_customer (
    `cust_key` INT NOT NULL,
    `name` STRING,
    `phone` STRING,
    `nation_key` INT NOT NULL,
    `acctbal` DECIMAL(15, 2),
    `mktsegment` STRING,
    PRIMARY KEY (`cust_key`) NOT ENFORCED
);
```

```sql  title="Flink SQL"
CREATE TABLE fluss_nation (
  `nation_key` INT NOT NULL,
  `name`       STRING,
   PRIMARY KEY (`nation_key`) NOT ENFORCED
);
```

```sql  title="Flink SQL"
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

First, run the following SQL to sync data from source tables to Fluss tables:
```sql  title="Flink SQL"
EXECUTE STATEMENT SET
BEGIN
    INSERT INTO fluss_nation SELECT * FROM `default_catalog`.`default_database`.source_nation;
    INSERT INTO fluss_customer SELECT * FROM `default_catalog`.`default_database`.source_customer;
    INSERT INTO fluss_order SELECT * FROM `default_catalog`.`default_database`.source_order;
END;
```

Fluss primary-key tables support high QPS point lookup queries on primary keys. Performing a [lookup join](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/joins/#lookup-join) is really efficient and you can use it to enrich
the `fluss_orders` table with information from the `fluss_customer` and `fluss_nation` primary-key tables.

```sql  title="Flink SQL"
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


## Run Ad-hoc Queries on Fluss Tables
You can now perform real-time analytics directly on Fluss tables.
For instance, to calculate the number of orders placed by a specific customer, you can execute the following SQL query to obtain instant, real-time results.

```sql  title="Flink SQL"
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';
```

```sql  title="Flink SQL"
-- switch to batch mode
SET 'execution.runtime-mode' = 'batch';
```

```sql  title="Flink SQL"
-- use limit to query the enriched_orders table
SELECT * FROM enriched_orders LIMIT 2;
```

**Sample Output**
```
+-----------+----------+-------------+------------+----------------+--------+------------+----------------+--------------+-----------------+-------------+
| order_key | cust_key | total_price | order_date | order_priority |  clerk |  cust_name |     cust_phone | cust_acctbal | cust_mktsegment | nation_name |
+-----------+----------+-------------+------------+----------------+--------+------------+----------------+--------------+-----------------+-------------+
|  23199744 |        9 |      266.44 | 2024-08-29 |           high | Clerk1 |   Joe King |   908.207.8513 |       124.28 |       FURNITURE |      JORDAN |
|  10715776 |        2 |      924.43 | 2024-11-04 |         medium | Clerk3 | Rita Booke | (925) 775-0717 |       172.39 |       FURNITURE |      UNITED |
+-----------+----------+-------------+------------+----------------+--------+------------+----------------+--------------+-----------------+-------------+
```
If you are interested in a specific customer, you can retrieve their details by performing a lookup on the `cust_key`.

```sql title="Flink SQL"
-- lookup by primary key
SELECT * FROM fluss_customer WHERE `cust_key` = 1;
```
**Sample Output**
```shell
+----------+---------------+--------------+------------+---------+------------+
| cust_key |          name |        phone | nation_key | acctbal | mktsegment |
+----------+---------------+--------------+------------+---------+------------+
|        1 | Al K. Seltzer | 817-617-7960 |          1 |  533.41 | AUTOMOBILE |
+----------+---------------+--------------+------------+---------+------------+
```
**Note:** Overall the query results are returned really fast, as Fluss enables efficient primary key lookups for tables with defined primary keys.

## Update/Delete rows on Fluss Tables

You can use `UPDATE` and `DELETE` statements to update/delete rows on Fluss tables.
### Update
```sql title="Flink SQL"
-- update by primary key
UPDATE fluss_customer SET `name` = 'fluss_updated' WHERE `cust_key` = 1;
```
Then you can `lookup` the specific row:
```sql title="Flink SQL"
SELECT * FROM fluss_customer WHERE `cust_key` = 1;
```
**Sample Output**
```shell
+----------+---------------+--------------+------------+---------+------------+
| cust_key |          name |        phone | nation_key | acctbal | mktsegment |
+----------+---------------+--------------+------------+---------+------------+
|        1 | fluss_updated | 817-617-7960 |          1 |  533.41 | AUTOMOBILE |
+----------+---------------+--------------+------------+---------+------------+
```
Notice that the `name` column has been updated to `fluss_updated`.

### Delete
```sql title="Flink SQL"
DELETE FROM fluss_customer WHERE `cust_key` = 1;
```
The following SQL query should return an empty result.
```sql title="Flink SQL"
SELECT * FROM fluss_customer WHERE `cust_key` = 1;
```

## Integrate with Iceberg
### Start the Lakehouse Tiering Service
To integrate with [Apache Iceberg](https://iceberg.apache.org/), you need to start the `Lakehouse Tiering Service`.
Open a new terminal, navigate to the `fluss-quickstart-flink-iceberg` directory, and execute the following command within this directory to start the service:
```shell
docker compose exec jobmanager \
    /opt/flink/bin/flink run \
    /opt/flink/opt/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type hadoop \
    --datalake.iceberg.warehouse /tmp/iceberg
```
You should see a Flink Job to tier data from Fluss to Iceberg running in the [Flink Web UI](http://localhost:8083/).

### Streaming into Fluss datalake-enabled tables

By default, tables are created with data lake integration disabled, meaning the Lakehouse Tiering Service will not tier the table's data to the data lake.

To enable lakehouse functionality as a tiered storage solution for a table, you must create the table with the configuration option `table.datalake.enabled = true`.
Return to the `SQL client` and execute the following SQL statement to create a table with data lake integration enabled:
```sql  title="Flink SQL"
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
    `nation_name` STRING
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

Next, perform streaming data writing into the **datalake-enabled** table, `datalake_enriched_orders`:
```sql  title="Flink SQL"
-- switch to streaming mode
SET 'execution.runtime-mode' = 'streaming';
```

```sql  title="Flink SQL"
-- insert tuples into datalake_enriched_orders
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
FROM (
    SELECT *, PROCTIME() as ptime
    FROM `default_catalog`.`default_database`.source_order
) o
LEFT JOIN fluss_customer FOR SYSTEM_TIME AS OF o.ptime AS c
    ON o.cust_key = c.cust_key
LEFT JOIN fluss_nation FOR SYSTEM_TIME AS OF o.ptime AS n
    ON c.nation_key = n.nation_key;
```

### Real-Time Analytics on Fluss datalake-enabled Tables

The data for the `datalake_enriched_orders` table is stored in Fluss (for real-time data) and Iceberg (for historical data).

When querying the `datalake_enriched_orders` table, Fluss uses a union operation that combines data from both Fluss and Iceberg to provide a complete result set -- combines **real-time** and **historical** data.

If you wish to query only the data stored in Iceberg—offering high-performance access without the overhead of unioning data—you can use the `datalake_enriched_orders$lake` table by appending the `$lake` suffix.
This approach also enables all the optimizations and features of a Flink Iceberg table source, including [system table](https://iceberg.apache.org/docs/latest/flink-queries/#inspecting-tables) such as `datalake_enriched_orders$lake$snapshots`.


```sql  title="Flink SQL"
-- switch to batch mode
SET 'execution.runtime-mode' = 'batch';
```


```sql  title="Flink SQL"
-- query snapshots in iceberg
SELECT snapshot_id, operation FROM datalake_enriched_orders$lake$snapshots;
```

**Sample Output:**
```shell
+---------------------+-----------+
|         snapshot_id | operation |
+---------------------+-----------+
| 7792523713868625335 |    append |
| 7960217942125627573 |    append |
+---------------------+-----------+
```
**Note:** Make sure to wait for the configured `datalake.freshness` (~30s) to complete before querying the snapshots, otherwise the result will be empty.

Run the following SQL to do analytics on Iceberg data:
```sql  title="Flink SQL"
-- to sum prices of all orders in iceberg
SELECT sum(total_price) as sum_price FROM datalake_enriched_orders$lake;
```
**Sample Output:**
```shell
+-----------+
| sum_price |
+-----------+
| 432880.93 |
+-----------+
```

To achieve results with sub-second data freshness, you can query the table directly, which seamlessly unifies data from both Fluss and Iceberg:

```sql  title="Flink SQL"
-- to sum prices of all orders (combining fluss and iceberg data)
SELECT sum(total_price) as sum_price FROM datalake_enriched_orders;
```

**Sample Output:**
```shell
+-----------+
| sum_price |
+-----------+
| 558660.03 |
+-----------+
```

You can execute the real-time analytics query multiple times, and the results will vary with each run as new data is continuously written to Fluss in real-time.

Finally, you can use the following command to view the files stored in Iceberg:
```shell
docker compose exec taskmanager tree /tmp/iceberg/fluss
```

**Sample Output:**
```shell
/tmp/iceberg/fluss
└── datalake_enriched_orders
    ├── data
    │   └── 00000-0-abc123.parquet
    └── metadata
        ├── snap-1234567890123456789-1-abc123.avro
        └── v1.metadata.json
```
The files adhere to Iceberg's standard format, enabling seamless querying with other engines such as [Spark](https://iceberg.apache.org/docs/latest/spark-queries/) and [Trino](https://trino.io/docs/current/connector/iceberg.html).

## Clean up
After finishing the tutorial, run `exit` to exit Flink SQL CLI Container and then run
```shell
docker compose down -v
```
to stop all containers.

## Learn more
Now that you're up and running with Fluss and Flink with Iceberg, check out the [Apache Flink Engine](engine-flink/getting-started.md) docs to learn more features with Flink or [this guide](/maintenance/observability/quickstart.md) to learn how to set up an observability stack for Fluss and Flink.