---
title: Building a Streaming Lakehouse
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you set up a basic Streaming Lakehouse using Fluss with Paimon or Iceberg, and help you better understand the powerful feature of Union Read.

## Environment Setup
### Prerequisites

Before proceeding with this guide, ensure that [Docker](https://docs.docker.com/engine/install/) and the [Docker Compose plugin](https://docs.docker.com/compose/install/linux/) are installed on your machine.
All commands were tested with Docker version 27.4.0 and Docker Compose version v2.30.3.

:::note
We encourage you to use a recent version of Docker and [Compose v2](https://docs.docker.com/compose/releases/migrate/) (however, Compose v1 might work with a few adaptions).
:::

### Starting required components

<Tabs groupId="lake-tabs">
  <TabItem value="paimon" label="Paimon" default>

We will use `docker compose` to spin up the required components for this tutorial.

1. Create a working directory for this guide.

```shell
mkdir fluss-quickstart-paimon
cd fluss-quickstart-paimon
```

2. Create a `lib` directory and download the Paimon S3 plugin jar required by the Fluss servers:

```shell
mkdir lib
curl -fL -o "lib/paimon-s3-$PAIMON_VERSION$.jar" "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/$PAIMON_VERSION$/paimon-s3-$PAIMON_VERSION$.jar"
```

:::info
The `apache/fluss-quickstart-flink` image already includes the Flink-side dependencies used by this guide. Only the Fluss server-side `paimon-s3` plugin still needs to be downloaded and mounted into the Fluss containers.
:::

3. Create a `docker-compose.yml` file with the following content:

```yaml
services:
  #begin RustFS (S3-compatible storage)
  rustfs:
    image: rustfs/rustfs:1.0.0-alpha.83
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      - RUSTFS_ACCESS_KEY=rustfsadmin
      - RUSTFS_SECRET_KEY=rustfsadmin
      - RUSTFS_CONSOLE_ENABLE=true
    volumes:
      - rustfs-data:/data
    command: /data
  rustfs-init:
    image: minio/mc
    depends_on:
      - rustfs
    entrypoint: >
      /bin/sh -c "
      until mc alias set rustfs http://rustfs:9000 rustfsadmin rustfsadmin; do
        echo 'Waiting for RustFS...';
        sleep 1;
      done;
      mc mb --ignore-existing rustfs/fluss;
      "
  #end
  coordinator-server:
    image: apache/fluss:$FLUSS_DOCKER_VERSION$
    command: coordinatorServer
    depends_on:
      zookeeper:
        condition: service_started
      rustfs-init:
        condition: service_completed_successfully
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://coordinator-server:9123
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.region: us-east-1
        s3.path-style-access: true
        s3.assumed.role.arn: arn:aws:iam::000000000000:role/rustfsadmin
        s3.assumed.role.sts.endpoint: http://rustfs:9000
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: s3://fluss/paimon
        datalake.paimon.s3.endpoint: http://rustfs:9000
        datalake.paimon.s3.access-key: rustfsadmin
        datalake.paimon.s3.secret-key: rustfsadmin
        datalake.paimon.s3.path.style.access: true
    volumes:
      - ./lib/paimon-s3-$PAIMON_VERSION$.jar:/opt/fluss/plugins/paimon/paimon-s3-$PAIMON_VERSION$.jar
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
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.region: us-east-1
        s3.path-style-access: true
        s3.assumed.role.arn: arn:aws:iam::000000000000:role/rustfsadmin
        s3.assumed.role.sts.endpoint: http://rustfs:9000
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: s3://fluss/paimon
        datalake.paimon.s3.endpoint: http://rustfs:9000
        datalake.paimon.s3.access-key: rustfsadmin
        datalake.paimon.s3.secret-key: rustfsadmin
        datalake.paimon.s3.path.style.access: true
    volumes:
      - ./lib/paimon-s3-$PAIMON_VERSION$.jar:/opt/fluss/plugins/paimon/paimon-s3-$PAIMON_VERSION$.jar
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  jobmanager:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    ports:
      - "8083:8081"
    entrypoint: ["/opt/flink/init_paimon.sh"]
    command: ["jobmanager"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
  taskmanager:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    depends_on:
      - jobmanager
    entrypoint: ["/opt/flink/init_paimon.sh"]
    command: ["taskmanager"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.task.off-heap.size: 128m
  sql-client:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    depends_on:
      - jobmanager
    entrypoint: ["/opt/flink/init_paimon.sh"]
    command: ["/opt/sql-client/sql-client"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        rest.address: jobmanager

volumes:
  rustfs-data:
```

The Docker Compose environment consists of the following containers:
- **Fluss Cluster:** a Fluss `CoordinatorServer`, a Fluss `TabletServer` and a `ZooKeeper` server.
- **Flink Cluster**: a Flink `JobManager`, a Flink `TaskManager`, and a Flink SQL client container to execute queries. The quickstart image already keeps the base Fluss dependencies in `FLINK_HOME/lib`, and `init_paimon.sh` prints a clear activation banner before copying the Paimon-specific jars into place.
- **RustFS**: an S3-compatible storage system used both as Fluss remote storage and Paimon's filesystem warehouse.


:::tip
[RustFS](https://github.com/rustfs/rustfs) is used as replacement for S3 in this quickstart example, for your production setup you may want to configure this to use cloud file system. See [here](/maintenance/filesystems/overview.md) for information on how to setup cloud file systems
:::

4. To start all containers, run:
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

  </TabItem>

  <TabItem value="iceberg" label="Iceberg">

We will use `docker compose` to spin up the required components for this tutorial.

1. Create a working directory for this guide.

```shell
mkdir fluss-quickstart-iceberg
cd fluss-quickstart-iceberg
```

2. Create a `lib` directory and download the Iceberg jars required by the Fluss servers:

```shell
mkdir lib
curl -fL -o lib/iceberg-aws-1.10.1.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.10.1/iceberg-aws-1.10.1.jar
curl -fL -o lib/iceberg-aws-bundle-1.10.1.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar
curl -fL -o lib/postgresql-42.7.4.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
```

:::info
The `apache/fluss-quickstart-flink` image already includes the Flink-side dependencies used by this guide. Only the Fluss server-side Iceberg and JDBC jars still need to be downloaded and mounted into the Fluss containers.
:::

3. Create a `docker-compose.yml` file with the following content:

```yaml
services:
  #begin RustFS (S3-compatible storage)
  rustfs:
    image: rustfs/rustfs:1.0.0-alpha.83
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      - RUSTFS_ACCESS_KEY=rustfsadmin
      - RUSTFS_SECRET_KEY=rustfsadmin
      - RUSTFS_CONSOLE_ENABLE=true
    volumes:
      - rustfs-data:/data
    command: /data
  rustfs-init:
    image: minio/mc
    depends_on:
      - rustfs
    entrypoint: >
      /bin/sh -c "
      until mc alias set rustfs http://rustfs:9000 rustfsadmin rustfsadmin; do
        echo 'Waiting for RustFS...';
        sleep 1;
      done;
      mc mb --ignore-existing rustfs/fluss;
      "
  #end
  postgres:
    image: postgres:17
    environment:
      - POSTGRES_USER=iceberg
      - POSTGRES_PASSWORD=iceberg
      - POSTGRES_DB=iceberg
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U iceberg"]
      interval: 3s
      timeout: 3s
      retries: 5
  coordinator-server:
    image: apache/fluss:$FLUSS_DOCKER_VERSION$
    command: coordinatorServer
    depends_on:
      postgres:
        condition: service_healthy
      zookeeper:
        condition: service_started
      rustfs-init:
        condition: service_completed_successfully
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://coordinator-server:9123
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.region: us-east-1
        s3.path-style-access: true
        s3.assumed.role.arn: arn:aws:iam::000000000000:role/rustfsadmin
        s3.assumed.role.sts.endpoint: http://rustfs:9000
        datalake.format: iceberg
        datalake.iceberg.catalog-impl: org.apache.iceberg.jdbc.JdbcCatalog
        datalake.iceberg.name: fluss_catalog
        datalake.iceberg.uri: jdbc:postgresql://postgres:5432/iceberg
        datalake.iceberg.jdbc.user: iceberg
        datalake.iceberg.jdbc.password: iceberg
        datalake.iceberg.warehouse: s3://fluss/iceberg
        datalake.iceberg.io-impl: org.apache.iceberg.aws.s3.S3FileIO
        datalake.iceberg.s3.endpoint: http://rustfs:9000
        datalake.iceberg.s3.access-key-id: rustfsadmin
        datalake.iceberg.s3.secret-access-key: rustfsadmin
        datalake.iceberg.s3.path-style-access: true
        datalake.iceberg.client.region: us-east-1
    volumes:
      - ./lib/iceberg-aws-1.10.1.jar:/opt/fluss/plugins/iceberg/iceberg-aws-1.10.1.jar
      - ./lib/iceberg-aws-bundle-1.10.1.jar:/opt/fluss/plugins/iceberg/iceberg-aws-bundle-1.10.1.jar
      - ./lib/postgresql-42.7.4.jar:/opt/fluss/plugins/iceberg/postgresql-42.7.4.jar
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
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.region: us-east-1
        s3.path-style-access: true
        s3.assumed.role.arn: arn:aws:iam::000000000000:role/rustfsadmin
        s3.assumed.role.sts.endpoint: http://rustfs:9000
        datalake.format: iceberg
        datalake.iceberg.catalog-impl: org.apache.iceberg.jdbc.JdbcCatalog
        datalake.iceberg.name: fluss_catalog
        datalake.iceberg.uri: jdbc:postgresql://postgres:5432/iceberg
        datalake.iceberg.jdbc.user: iceberg
        datalake.iceberg.jdbc.password: iceberg
        datalake.iceberg.warehouse: s3://fluss/iceberg
        datalake.iceberg.io-impl: org.apache.iceberg.aws.s3.S3FileIO
        datalake.iceberg.s3.endpoint: http://rustfs:9000
        datalake.iceberg.s3.access-key-id: rustfsadmin
        datalake.iceberg.s3.secret-access-key: rustfsadmin
        datalake.iceberg.s3.path-style-access: true
        datalake.iceberg.client.region: us-east-1
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  jobmanager:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    ports:
      - "8083:8081"
    entrypoint: ["/opt/flink/init_iceberg.sh"]
    command: ["jobmanager"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
  taskmanager:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    depends_on:
      - jobmanager
    entrypoint: ["/opt/flink/init_iceberg.sh"]
    command: ["taskmanager"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.task.off-heap.size: 128m
  sql-client:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    depends_on:
      - jobmanager
    entrypoint: ["/opt/flink/init_iceberg.sh"]
    command: ["/opt/sql-client/sql-client"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        rest.address: jobmanager

volumes:
  rustfs-data:
```

The Docker Compose environment consists of the following containers:
- **Fluss Cluster:** a Fluss `CoordinatorServer`, a Fluss `TabletServer` and a `ZooKeeper` server.
- **Flink Cluster**: a Flink `JobManager`, a Flink `TaskManager`, and a Flink SQL client container to execute queries. The quickstart image already keeps the base Fluss dependencies in `FLINK_HOME/lib`, and `init_iceberg.sh` prints a clear activation banner before copying the Iceberg-specific jars into place.
- **PostgreSQL**: stores Iceberg catalog metadata (used by `JdbcCatalog`).
- **RustFS**: an S3-compatible storage system used both as Fluss remote storage and Iceberg's filesystem warehouse.

:::tip
[RustFS](https://github.com/rustfs/rustfs) is used as replacement for S3 in this quickstart example, for your production setup you may want to configure this to use cloud file system. See [here](/maintenance/filesystems/overview.md) for information on how to setup cloud file systems
:::

4. To start all containers, run:
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

  </TabItem>
</Tabs>

## Enter into SQL-Client

<Tabs groupId="lake-tabs">
  <TabItem value="paimon" label="Paimon" default>

First, use the following command to enter the Flink SQL CLI Container:
```shell
docker compose run sql-client
```

To simplify this guide, the demo source tables backed by the `faker` connector are already pre-created in the SQL client session.

  </TabItem>

  <TabItem value="iceberg" label="Iceberg">

First, use the following command to enter the Flink SQL CLI Container:
```shell
docker compose run sql-client
```

To simplify this guide, the demo source tables backed by the `faker` connector are already pre-created in the SQL client session.

  </TabItem>
</Tabs>


## Create Fluss Tables
### Create Fluss Catalog
Use the following SQL to create a Fluss catalog:

<Tabs groupId="lake-tabs">
  <TabItem value="paimon" label="Paimon" default>

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'paimon.s3.access-key' = 'rustfsadmin',
    'paimon.s3.secret-key' = 'rustfsadmin'
);
```
  </TabItem>

  <TabItem value="iceberg" label="Iceberg">

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'iceberg.jdbc.password' = 'iceberg',
    'iceberg.s3.access-key-id' = 'rustfsadmin',
    'iceberg.s3.secret-access-key' = 'rustfsadmin'
);
```
  </TabItem>
</Tabs>

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

:::info
By default, catalog configurations are not persisted across Flink SQL client sessions.
For further information how to store catalog configurations, see [Flink's Catalog Store](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/catalogs/#catalog-store).
:::

### Create Tables
<Tabs groupId="lake-tabs">
  <TabItem value="paimon" label="Paimon" default>


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

  </TabItem>

  <TabItem value="iceberg" label="Iceberg">


Running the following SQL to create Fluss tables to be used in this guide:
```sql  title="Flink SQL"
CREATE TABLE fluss_order (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `ptime` AS PROCTIME()
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

  </TabItem>
</Tabs>
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

## Lakehouse Integration
### Start the Lakehouse Tiering Service

<Tabs groupId="lake-tabs">
  <TabItem value="paimon" label="Paimon" default>

To integrate with [Apache Paimon](https://paimon.apache.org/), you need to start the `Lakehouse Tiering Service`.
Open a new terminal, navigate to the `fluss-quickstart-paimon` directory, and execute the following command within this directory to start the service:
```shell
docker compose exec jobmanager \
    /opt/flink/bin/flink run \
    /opt/flink/opt/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse s3://fluss/paimon \
    --datalake.paimon.s3.endpoint http://rustfs:9000 \
    --datalake.paimon.s3.access.key rustfsadmin \
    --datalake.paimon.s3.secret.key rustfsadmin \
    --datalake.paimon.s3.path.style.access true
```
You should see a Flink Job to tier data from Fluss to Paimon running in the [Flink Web UI](http://localhost:8083/).

  </TabItem>

  <TabItem value="iceberg" label="Iceberg">

To integrate with [Apache Iceberg](https://iceberg.apache.org/), you need to start the `Lakehouse Tiering Service`.
Open a new terminal, navigate to the `fluss-quickstart-iceberg` directory, and execute the following command within this directory to start the service:
```shell
docker compose exec jobmanager \
    /opt/flink/bin/flink run \
    /opt/flink/opt/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.catalog-impl org.apache.iceberg.jdbc.JdbcCatalog \
    --datalake.iceberg.name fluss_catalog \
    --datalake.iceberg.uri "jdbc:postgresql://postgres:5432/iceberg" \
    --datalake.iceberg.jdbc.user iceberg \
    --datalake.iceberg.jdbc.password iceberg \
    --datalake.iceberg.warehouse "s3://fluss/iceberg" \
    --datalake.iceberg.io-impl org.apache.iceberg.aws.s3.S3FileIO \
    --datalake.iceberg.s3.endpoint "http://rustfs:9000" \
    --datalake.iceberg.s3.access-key-id rustfsadmin \
    --datalake.iceberg.s3.secret-access-key rustfsadmin \
    --datalake.iceberg.s3.path-style-access true \
    --datalake.iceberg.client.region us-east-1
```
You should see a Flink Job to tier data from Fluss to Iceberg running in the [Flink Web UI](http://localhost:8083/).

  </TabItem>
</Tabs>

### Streaming into Fluss datalake-enabled tables

<Tabs groupId="lake-tabs">
  <TabItem value="paimon" label="Paimon" default>

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
    `nation_name` STRING,
    PRIMARY KEY (`order_key`) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

Next, perform streaming data writing into the **datalake-enabled** table, `datalake_enriched_orders`:

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
FROM fluss_order o
LEFT JOIN fluss_customer FOR SYSTEM_TIME AS OF `o`.`ptime` AS `c`
    ON o.cust_key = c.cust_key
LEFT JOIN fluss_nation FOR SYSTEM_TIME AS OF `o`.`ptime` AS `n`
    ON c.nation_key = n.nation_key;
```

  </TabItem>

  <TabItem value="iceberg" label="Iceberg">

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
FROM fluss_order o
LEFT JOIN fluss_customer FOR SYSTEM_TIME AS OF `o`.`ptime` AS `c`
    ON o.cust_key = c.cust_key
LEFT JOIN fluss_nation FOR SYSTEM_TIME AS OF `o`.`ptime` AS `n`
    ON c.nation_key = n.nation_key;
```

  </TabItem>
</Tabs>

### Real-Time Analytics on Fluss datalake-enabled Tables

<Tabs groupId="lake-tabs">
  <TabItem value="paimon" label="Paimon" default>

The data for the `datalake_enriched_orders` table is stored in Fluss (for real-time data) and Paimon (for historical data).

When querying the `datalake_enriched_orders` table, Fluss uses a union operation that combines data from both Fluss and Paimon to provide a complete result set -- combines **real-time** and **historical** data.

If you wish to query only the data stored in Paimon—offering high-performance access without the overhead of unioning data—you can use the `datalake_enriched_orders$lake` table by appending the `$lake` suffix. 
This approach also enables all the optimizations and features of a Flink Paimon table source, including [system table](https://paimon.apache.org/docs/$PAIMON_VERSION_SHORT$/concepts/system-tables/) such as `datalake_enriched_orders$lake$snapshots`.

To query the snapshots directly from Paimon, use the following SQL:

```sql  title="Flink SQL"
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';
```

```sql  title="Flink SQL"
-- switch to batch mode
SET 'execution.runtime-mode' = 'batch';
```

```sql  title="Flink SQL"
-- query snapshots in paimon
SELECT snapshot_id, total_record_count FROM datalake_enriched_orders$lake$snapshots;
```

**Sample Output:**
```shell
+-------------+--------------------+
| snapshot_id | total_record_count |
+-------------+--------------------+
|           1 |                650 |
+-------------+--------------------+
```
**Note:** Make sure to wait for the configured `datalake.freshness` (~30s) to complete before querying the snapshots, otherwise the result will be empty.

Run the following SQL to do analytics on Paimon data:
```sql  title="Flink SQL"
-- to sum prices of all orders in paimon
SELECT sum(total_price) as sum_price FROM datalake_enriched_orders$lake;
```
**Sample Output:**
```shell
+------------+
|  sum_price |
+------------+
| 1669519.92 |
+------------+
```

To achieve results with sub-second data freshness, you can query the table directly, which seamlessly unifies data from both Fluss and Paimon:
```sql  title="Flink SQL"
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
You can execute the real-time analytics query multiple times, and the results will vary with each run as new data is continuously written to Fluss in real-time.

The files adhere to Paimon's standard format, enabling seamless querying with other engines such as [Spark](https://paimon.apache.org/docs/$PAIMON_VERSION_SHORT$/spark/quick-start/) and [Trino](https://paimon.apache.org/docs/$PAIMON_VERSION_SHORT$/ecosystem/trino/).

  </TabItem>

  <TabItem value="iceberg" label="Iceberg">

The data for the `datalake_enriched_orders` table is stored in Fluss (for real-time data) and Iceberg (for historical data).

When querying the `datalake_enriched_orders` table, Fluss uses a union operation that combines data from both Fluss and Iceberg to provide a complete result set -- combines **real-time** and **historical** data.

If you wish to query only the data stored in Iceberg—offering high-performance access without the overhead of unioning data—you can use the `datalake_enriched_orders$lake` table by appending the `$lake` suffix.
This approach also enables all the optimizations and features of a Flink Iceberg table source, including [system table](https://iceberg.apache.org/docs/latest/flink-queries/#inspecting-tables) such as `datalake_enriched_orders$lake$snapshots`.


```sql  title="Flink SQL"
-- use tableau result mode
SET 'sql-client.execution.result-mode' = 'tableau';
```

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

The files adhere to Iceberg's standard format, enabling seamless querying with other engines such as [Spark](https://iceberg.apache.org/docs/latest/spark-queries/) and [Trino](https://trino.io/docs/current/connector/iceberg.html).

  </TabItem>
</Tabs>

### Quitting Sql Client

The following command allows you to quit Flink SQL Client.
```sql title="Flink SQL"
quit;
```

### Tiered Storage

You can visit http://localhost:9001/ and sign in with `rustfsadmin` / `rustfsadmin` to view the files stored on tiered storage.

## Clean up
Run the following to stop all containers.
```shell
docker compose down -v
```
