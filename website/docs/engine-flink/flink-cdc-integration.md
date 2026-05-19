---
sidebar_label: Flink CDC
title: Flink CDC Integration
sidebar_position: 9
---

# Flink CDC Integration

[Flink CDC](https://nightlies.apache.org/flink/flink-cdc-docs-master/) is a streaming data integration tool built on top of Apache Flink that can capture real-time changes from various databases. Flink CDC supports Fluss as a [Pipeline Sink Connector](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/pipeline-connectors/fluss/), making it straightforward to sync CDC data from databases like PostgreSQL, MySQL, and Oracle into Fluss.

There are two ways to sync database changes into Fluss using Flink CDC:

- **Flink SQL with CDC connectors** — Use SQL to define CDC source tables and write data into Fluss tables. Best for per-table synchronization with SQL-native workflows.
- **Flink CDC Pipeline Connector** — Use a YAML pipeline definition to sync entire databases (including multiple tables) into Fluss. Best for whole-database synchronization.

## Prerequisites

- A running **Fluss cluster** (CoordinatorServer + TabletServer). See [Deploying with Docker](../install-deploy/deploying-with-docker.md) for setup instructions.
- A running **Flink cluster** with the required connector JARs. See [Getting Started with Flink](getting-started.md) for Flink setup.
- The required connector JARs placed under `<FLINK_HOME>/lib/`. The examples below use MySQL as the source, but other databases (PostgreSQL, Oracle, etc.) are also supported — see [Further Reading](#further-reading) for the full list of connectors.
  - For SQL approach: [flink-sql-connector-mysql-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-mysql-cdc) and the [Fluss Flink connector](getting-started.md)
  - For Pipeline approach: [flink-cdc-pipeline-connector-fluss](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-fluss)

## Example 1: Sync MySQL to Fluss with Flink SQL

This example shows how to capture changes from a MySQL table and write them into a Fluss primary-key table using Flink SQL.

### Step 1: Start MySQL with Docker

Start a MySQL instance with binlog enabled using the Debezium example image:

```shell
docker run -d --name mysql \
  -e MYSQL_ROOT_PASSWORD=123456 \
  -e MYSQL_USER=mysqluser \
  -e MYSQL_PASSWORD=mysqlpw \
  -p 3306:3306 \
  debezium/example-mysql:1.1
```

Wait for the container to start, then connect to MySQL:

```shell
docker exec -it mysql mysql -uroot -p123456
```

Create a sample database and table:

```sql
CREATE DATABASE mydb;
USE mydb;
CREATE TABLE orders (
  order_id INT AUTO_INCREMENT PRIMARY KEY,
  customer_name VARCHAR(255),
  product VARCHAR(255),
  quantity INT,
  order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO orders (customer_name, product, quantity) VALUES
  ('Alice', 'Laptop', 1),
  ('Bob', 'Phone', 2),
  ('Charlie', 'Tablet', 3);
```

### Step 2: Create a MySQL CDC Source Table in Flink SQL

Open the Flink SQL CLI and create a CDC source table that captures changes from the MySQL `orders` table:

```sql title="Flink SQL"
CREATE TABLE mysql_orders (
  order_id INT,
  customer_name STRING,
  product STRING,
  quantity INT,
  order_date TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'localhost',
  'port' = '3306',
  'username' = 'root',
  'password' = '123456',
  'database-name' = 'mydb',
  'table-name' = 'orders'
);
```

### Step 3: Create a Fluss Sink Table

Create a Fluss Catalog and a primary-key table in Fluss to receive the CDC data:

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;

CREATE DATABASE IF NOT EXISTS mydb;

CREATE TABLE mydb.orders (
  order_id INT,
  customer_name STRING,
  product STRING,
  quantity INT,
  order_date TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
);
```

### Step 4: Sync Data

Switch back to the default catalog and start the synchronization job:

```sql title="Flink SQL"
USE CATALOG default_catalog;

INSERT INTO fluss_catalog.mydb.orders
SELECT * FROM mysql_orders;
```

This starts a streaming job that continuously captures changes from MySQL and writes them to Fluss.

### Step 5: Query the Data in Fluss

You can now query the synced data in Fluss:

```sql title="Flink SQL"
-- Switch to Fluss catalog
USE CATALOG fluss_catalog;

-- Point query by primary key
SELECT * FROM mydb.orders WHERE order_id = 1;

-- Streaming read to observe real-time changes
SELECT * FROM mydb.orders;
```

Try inserting or updating rows in MySQL — changes will be captured and reflected in Fluss in real time. Open a MySQL client:

```shell
docker exec -it mysql mysql -uroot -p123456 mydb
```

Then execute:

```sql
INSERT INTO orders (customer_name, product, quantity) VALUES ('Dave', 'Monitor', 2);
UPDATE orders SET quantity = 5 WHERE customer_name = 'Alice';
```

## Example 2: Sync MySQL to Fluss with Pipeline Connector

For whole-database synchronization, the Flink CDC Pipeline Connector allows you to define a YAML pipeline that syncs all tables from a MySQL database into Fluss automatically — without writing any SQL.

:::note
This example reuses the MySQL container started in [Example 1](#example-1-sync-mysql-to-fluss-with-flink-sql). If you haven't started it yet, follow [Step 1](#step-1-start-mysql-with-docker) first.
:::

### Step 1: Define the Pipeline YAML

Create a file named `mysql-to-fluss.yaml`:

```yaml
source:
  type: mysql
  name: MySQL Source
  hostname: 127.0.0.1
  port: 3306
  username: root
  password: 123456
  tables: mydb.\.*
  server-id: 5401-5404

sink:
  type: fluss
  name: Fluss Sink
  bootstrap.servers: localhost:9123

pipeline:
  name: MySQL to Fluss Pipeline
  parallelism: 2
```

### Step 2: Submit the Pipeline

Submit the pipeline using the Flink CDC CLI:

```shell
./bin/flink-cdc.sh mysql-to-fluss.yaml
```

This will automatically create the corresponding tables in Fluss and start syncing data from all matching MySQL tables.

For the full list of Pipeline Connector options, see the [Fluss Pipeline Connector Documentation](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/pipeline-connectors/fluss/).

## Clean Up

After finishing the examples, stop and remove the MySQL container:

```shell
docker stop mysql && docker rm mysql
```

## Further Reading

- [Flink CDC Official Documentation](https://nightlies.apache.org/flink/flink-cdc-docs-master/)
- [Flink CDC Pipeline Connectors Overview](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/pipeline-connectors/overview/)
- [Flink SQL CDC Source Connectors](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/overview/)
- [Fluss Flink Engine Options](options.md)
