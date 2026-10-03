---
sidebar_label: Flink CDC
title: Flink CDC Integration
sidebar_position: 10
---

# Flink CDC Integration

[Flink CDC](https://nightlies.apache.org/flink/flink-cdc-docs-master/) is a streaming data integration tool built on top of Apache Flink that can capture real-time changes from various databases. Flink CDC supports Fluss as a [Pipeline Sink Connector](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/pipeline-connectors/fluss/), making it straightforward to sync CDC data from databases like MySQL, PostgreSQL, and Oracle into Fluss.

There are two ways to sync database changes into Fluss using Flink CDC:

- **Flink SQL with CDC connectors** — Use SQL to define CDC source tables and write data into Fluss tables. Best for per-table synchronization with SQL-native workflows.
- **Flink CDC Pipeline Connector** — Use a YAML pipeline definition to sync entire databases (including multiple tables) into Fluss. Best for whole-database synchronization.

## Prerequisites

- A running **Fluss cluster** (CoordinatorServer + TabletServer). See [Deploying with Docker](../install-deploy/deploying-with-docker.md) for setup instructions.
- A running **Flink cluster** with the required connector JARs. See [Getting Started with Flink](getting-started.md) for Flink setup.
- The required connector JARs placed under `<FLINK_HOME>/lib/`. The examples below use PostgreSQL as the source, but other databases (MySQL, Oracle, etc.) are also supported — see [Further Reading](#further-reading) for the full list of connectors.
  - For SQL approach: [flink-sql-connector-postgres-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-postgres-cdc) 
  - For Pipeline approach: [flink-cdc-pipeline-connector-fluss](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-fluss)

:::note
The Pipeline approach (Example 2) uses `./bin/flink-cdc.sh`, which is part of the **standalone Flink CDC distribution** (`flink-cdc-<version>-bin.tar.gz`) — it is **not** included in the standard Flink release. Download it from the [Flink CDC releases page](https://github.com/apache/flink-cdc/releases) and extract it. You can point it to your Flink installation via the `--flink-home` flag when submitting the pipeline.
:::

## Example 1: Sync PostgreSQL to Fluss with Flink SQL

This example shows how to capture changes from a PostgreSQL table and write them into a Fluss primary-key table using Flink SQL.

### Step 1: Start PostgreSQL with Docker

Start a PostgreSQL instance with logical replication enabled:

```shell
docker run -d --name postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15 \
  postgres -c wal_level=logical
```

Wait for the container to start, then connect to PostgreSQL and create the database:

```shell
docker exec -it postgres psql -U postgres
```

```sql
CREATE DATABASE mydb;
```

Then create the table:


```sql
CREATE TABLE orders (
  order_id SERIAL PRIMARY KEY,
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

### Step 2: Create a PostgreSQL CDC Source Table in Flink SQL

Open the Flink SQL CLI and create a CDC source table that captures changes from the PostgreSQL `orders` table:

```sql title="Flink SQL"
CREATE TABLE pg_orders (
  order_id INT,
  customer_name STRING,
  product STRING,
  quantity INT,
  order_date TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'localhost',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'mydb',
  'schema-name' = 'public',
  'table-name' = 'orders',
  'slot.name' = 'flink_slot'
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
SELECT * FROM pg_orders;
```

This starts a streaming job that continuously captures changes from PostgreSQL and writes them to Fluss.

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

Try inserting or updating rows in PostgreSQL — changes will be captured and reflected in Fluss in real time. Open a PostgreSQL client:

```shell
docker exec -it postgres psql -U postgres mydb
```

Then execute:

```sql
INSERT INTO orders (customer_name, product, quantity) VALUES ('Dave', 'Monitor', 2);
UPDATE orders SET quantity = 5 WHERE customer_name = 'Alice';
```

## Example 2: Sync PostgreSQL to Fluss with Pipeline Connector

For whole-database synchronization, the Flink CDC Pipeline Connector allows you to define a YAML pipeline that syncs all tables from a PostgreSQL database into Fluss automatically — without writing any SQL.

:::note
This example reuses the PostgreSQL container started in [Example 1](#example-1-sync-postgresql-to-fluss-with-flink-sql). If you haven't started it yet, follow [Step 1](#step-1-start-postgresql-with-docker) first.
:::

### Step 1: Define the Pipeline YAML

Create a file named `postgres-to-fluss.yaml`:

```yaml
source:
  type: postgres
  name: PostgreSQL Source
  hostname: 127.0.0.1
  port: 5432
  username: postgres
  password: postgres
  tables: public.\.*
  slot.name: flink_pipeline_slot

sink:
  type: fluss
  name: Fluss Sink
  bootstrap.servers: localhost:9123

pipeline:
  name: PostgreSQL to Fluss Pipeline
  parallelism: 2
```

### Step 2: Submit the Pipeline

Submit the pipeline using the Flink CDC CLI, passing `--flink-home` to point to your Flink installation:

```shell
./bin/flink-cdc.sh --flink-home /path/to/flink postgres-to-fluss.yaml
```

This will automatically create the corresponding tables in Fluss and start syncing data from all matching PostgreSQL tables.

For the full list of Pipeline Connector options, see the [Fluss Pipeline Connector Documentation](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/pipeline-connectors/fluss/).

## Clean Up

Before stopping the container, drop the replication slots created by the examples. PostgreSQL replication slots are not removed automatically when a Flink job stops — leaving them active causes WAL files to accumulate and can exhaust disk space.

```sql
SELECT  pg_drop_replication_slot(slot_name) 
FROM    pg_replication_slots
WHERE   slot_name IN ('flink_slot', 'flink_pipeline_slot');
```

Then stop and remove the container:

```shell
docker stop postgres && docker rm postgres
```

## Further Reading

- [Flink CDC Official Documentation](https://nightlies.apache.org/flink/flink-cdc-docs-master/)
- [Flink CDC Pipeline Connectors Overview](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/pipeline-connectors/overview/)
- [Flink SQL CDC Source Connectors](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/overview/)
- [Fluss Flink Engine Options](options.md)
