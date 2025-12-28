---
title: Real-Time Analytics with Flink
sidebar_position: 1
---


# Real-Time Analytics With Flink

This guide will get you up and running with Apache Flink to do real-time analytics, covering some powerful features of Fluss.
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
mkdir fluss-quickstart-flink
cd fluss-quickstart-flink
```

2. Create a `lib` directory and download the required jar files. You can adjust the Flink version as needed. Please make sure to download the compatible versions of [fluss-flink connector jar](/downloads) and [flink-connector-faker](https://github.com/knaufk/flink-faker/releases)

```shell
export FLINK_VERSION="1.20"
```

```shell
mkdir lib
wget -O lib/flink-faker-0.5.3.jar https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar
wget -O "lib/fluss-flink-${FLINK_VERSION}-$FLUSS_DOCKER_VERSION$.jar" "https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-${FLINK_VERSION}/$FLUSS_DOCKER_VERSION$/fluss-flink-${FLINK_VERSION}-$FLUSS_DOCKER_VERSION$.jar"
```

3. Create a `docker-compose.yml` file with the following content:

```yaml
services:
  #begin Fluss cluster
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
        remote.data.dir: /tmp/fluss/remote-data
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
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  #end
  #begin Flink cluster
  jobmanager:
    image: flink:${FLINK_VERSION}
    ports:
      - "8083:8081"
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
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.framework.off-heap.size: 256m
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
  #end
```

The Docker Compose environment consists of the following containers:
- **Fluss Cluster:** a Fluss `CoordinatorServer`, a Fluss `TabletServer` and a `ZooKeeper` server.
- **Flink Cluster**: a Flink `JobManager`, a Flink `TaskManager`, and a Flink SQL client container to execute queries.

3. To start all containers, run:
```shell
docker compose up -d
```
This command automatically starts all the containers defined in the Docker Compose configuration in detached mode.

Run 
```shell
docker compose ps
```
to check whether all containers are running properly.

You can also visit http://localhost:8083/ to see if Flink is running normally.

:::note
- If you want to additionally use an observability stack, follow one of the provided quickstart guides [here](maintenance/observability/quickstart.md) and then continue with this guide.
- All the following commands involving `docker compose` should be executed in the created working directory that contains the `docker-compose.yml` file.
:::

Congratulations, you are all set!

## Enter into SQL-Client
First, use the following command to enter the Flink SQL CLI Container:
```shell
docker compose run sql-client
```

To simplify this guide, we will create three temporary tables with `faker` connector to generate data:

```sql title="Flink SQL"
CREATE TEMPORARY TABLE source_order (
    `order_key` BIGINT,
    `cust_key` INT,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '10',
  'number-of-rows' = '10000',
  'fields.order_key.expression' = '#{number.numberBetween ''0'',''100000000''}',
  'fields.cust_key.expression' = '#{number.numberBetween ''0'',''20''}',
  'fields.total_price.expression' = '#{number.randomDouble ''3'',''1'',''1000''}',
  'fields.order_date.expression' = '#{date.past ''100'' ''DAYS''}',
  'fields.order_priority.expression' = '#{regexify ''(low|medium|high){1}''}',
  'fields.clerk.expression' = '#{regexify ''(Clerk1|Clerk2|Clerk3|Clerk4){1}''}'
);
```

```sql title="Flink SQL"
CREATE TEMPORARY TABLE source_customer (
    `cust_key` INT,
    `name` STRING,
    `phone` STRING,
    `nation_key` INT NOT NULL,
    `acctbal` DECIMAL(15, 2),
    `mktsegment` STRING,
    PRIMARY KEY (`cust_key`) NOT ENFORCED
) WITH (
  'connector' = 'faker',
  'number-of-rows' = '200',
  'fields.cust_key.expression' = '#{number.numberBetween ''0'',''20''}',
  'fields.name.expression' = '#{funnyName.name}',
  'fields.nation_key.expression' = '#{number.numberBetween ''1'',''5''}',
  'fields.phone.expression' = '#{phoneNumber.cellPhone}',
  'fields.acctbal.expression' = '#{number.randomDouble ''3'',''1'',''1000''}',
  'fields.mktsegment.expression' = '#{regexify ''(AUTOMOBILE|BUILDING|FURNITURE|MACHINERY|HOUSEHOLD){1}''}'
);
```

```sql title="Flink SQL"
CREATE TEMPORARY TABLE `source_nation` (
  `nation_key` INT NOT NULL,
  `name`       STRING,
   PRIMARY KEY (`nation_key`) NOT ENFORCED
) WITH (
  'connector' = 'faker',
  'number-of-rows' = '100',
  'fields.nation_key.expression' = '#{number.numberBetween ''1'',''5''}',
  'fields.name.expression' = '#{regexify ''(CANADA|JORDAN|CHINA|UNITED|INDIA){1}''}'
);
```

```sql title="Flink SQL"
-- drop records silently if a null value would have to be inserted into a NOT NULL column
SET 'table.exec.sink.not-null-enforcer'='DROP';
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
-- execute DML job synchronously
SET 'table.dml-sync' = 'true';
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

## Clean up
After finishing the tutorial, run `exit` to exit Flink SQL CLI Container and then run 
```shell
docker compose down -v
```
to stop all containers.

## Learn more
Now that you're up and running with Fluss and Flink, check out the [Apache Flink Engine](engine-flink/getting-started.md) docs to learn more features with Flink or [this guide](/maintenance/observability/quickstart.md) to learn how to set up an observability stack for Fluss and Flink.
