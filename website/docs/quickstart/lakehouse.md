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

2. Create directories and download required jars:

```shell
mkdir -p lib opt

# Flink connectors
curl -fL -o lib/flink-faker-0.5.3.jar https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar
curl -fL -o "lib/fluss-flink-1.20-$FLUSS_DOCKER_VERSION$.jar" "https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/$FLUSS_DOCKER_VERSION$/fluss-flink-1.20-$FLUSS_DOCKER_VERSION$.jar"
curl -fL -o "lib/paimon-flink-1.20-$PAIMON_VERSION$.jar" "https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.20/$PAIMON_VERSION$/paimon-flink-1.20-$PAIMON_VERSION$.jar"

# Fluss lake plugin
curl -fL -o "lib/fluss-lake-paimon-$FLUSS_DOCKER_VERSION$.jar" "https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-paimon/$FLUSS_DOCKER_VERSION$/fluss-lake-paimon-$FLUSS_DOCKER_VERSION$.jar"

# Paimon bundle jar
curl -fL -o "lib/paimon-bundle-$PAIMON_VERSION$.jar" "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-bundle/$PAIMON_VERSION$/paimon-bundle-$PAIMON_VERSION$.jar"

# Hadoop bundle jar
curl -fL -o lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# AWS S3 support
curl -fL -o "lib/paimon-s3-$PAIMON_VERSION$.jar" "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/$PAIMON_VERSION$/paimon-s3-$PAIMON_VERSION$.jar"

# Tiering service
curl -fL -o "opt/fluss-flink-tiering-$FLUSS_DOCKER_VERSION$.jar" "https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-tiering/$FLUSS_DOCKER_VERSION$/fluss-flink-tiering-$FLUSS_DOCKER_VERSION$.jar"
```

:::info
You can add more jars to this `lib` directory based on your requirements:
- **Other catalog backends**: Add jars needed for alternative Paimon catalog implementations (e.g., Hive, JDBC)
  :::

3. Create a `docker-compose.yml` file with the following content:

```yaml
services:
  #begin RustFS (S3-compatible storage)
  rustfs:
    image: rustfs/rustfs:latest
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
      - zookeeper
      - rustfs-init
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://coordinator-server:9123
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.path.style.access: true
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: s3://fluss/paimon
        datalake.paimon.s3.endpoint: http://rustfs:9000
        datalake.paimon.s3.access-key: rustfsadmin
        datalake.paimon.s3.secret-key: rustfsadmin
        datalake.paimon.s3.path.style.access: true
    volumes:
      - ./lib/paimon-s3-1.3.1.jar:/opt/fluss/plugins/paimon/paimon-s3-1.3.1.jar
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
        s3.path.style.access: true
        kv.snapshot.interval: 0s
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: s3://fluss/paimon
        datalake.paimon.s3.endpoint: http://rustfs:9000
        datalake.paimon.s3.access-key: rustfsadmin
        datalake.paimon.s3.secret-key: rustfsadmin
        datalake.paimon.s3.path.style.access: true
    volumes:
      - ./lib/paimon-s3-1.3.1.jar:/opt/fluss/plugins/paimon/paimon-s3-1.3.1.jar
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  jobmanager:
    image: flink:1.20-scala_2.12-java17
    ports:
      - "8083:8081"
    entrypoint: ["/bin/bash", "-c"]
    command: >
      "sed -i 's/exec $(drop_privs_cmd)//g' /docker-entrypoint.sh &&
       cp /tmp/jars/*.jar /opt/flink/lib/ 2>/dev/null || true;
       cp /tmp/opt/*.jar /opt/flink/opt/ 2>/dev/null || true;
       /docker-entrypoint.sh jobmanager"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - ./lib:/tmp/jars
      - ./opt:/tmp/opt
  taskmanager:
    image: flink:1.20-scala_2.12-java17
    depends_on:
      - jobmanager
    entrypoint: ["/bin/bash", "-c"]
    command: >
      "sed -i 's/exec $(drop_privs_cmd)//g' /docker-entrypoint.sh &&
       cp /tmp/jars/*.jar /opt/flink/lib/ 2>/dev/null || true;
       cp /tmp/opt/*.jar /opt/flink/opt/ 2>/dev/null || true;
       /docker-entrypoint.sh taskmanager"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.task.off-heap.size: 128m
    volumes:
      - ./lib:/tmp/jars
      - ./opt:/tmp/opt

volumes:
  rustfs-data:
```

The Docker Compose environment consists of the following containers:
- **Fluss Cluster:** a Fluss `CoordinatorServer`, a Fluss `TabletServer` and a `ZooKeeper` server.
- **Flink Cluster**: a Flink `JobManager` and a Flink `TaskManager` container to execute queries.

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
- If you want to run with your own Flink environment, remember to download the [fluss-flink connector jar](/downloads), [flink-connector-faker](https://github.com/knaufk/flink-faker/releases), [paimon-flink connector jar](https://paimon.apache.org/docs/1.3/flink/quick-start/) and then put them to `FLINK_HOME/lib/`.
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

2. Create directories and download required jars:

```shell
mkdir -p lib opt

# Flink connectors
curl -fL -o lib/flink-faker-0.5.3.jar https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar
curl -fL -o "lib/fluss-flink-1.20-$FLUSS_DOCKER_VERSION$.jar" "https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/$FLUSS_DOCKER_VERSION$/fluss-flink-1.20-$FLUSS_DOCKER_VERSION$.jar"
curl -fL -o lib/iceberg-flink-runtime-1.20-1.10.1.jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar"

# Fluss lake plugin
curl -fL -o "lib/fluss-lake-iceberg-$FLUSS_DOCKER_VERSION$.jar" "https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-iceberg/$FLUSS_DOCKER_VERSION$/fluss-lake-iceberg-$FLUSS_DOCKER_VERSION$.jar"

# Hadoop filesystem support
curl -fL -o lib/hadoop-client-api-3.3.5.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.5/hadoop-client-api-3.3.5.jar
curl -fL -o lib/hadoop-client-runtime-3.3.5.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.5/hadoop-client-runtime-3.3.5.jar
curl -fL -o lib/commons-logging-1.2.jar https://repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar

# AWS S3 support (s3a:// scheme)
curl -fL -o lib/hadoop-aws-3.3.5.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.5/hadoop-aws-3.3.5.jar
curl -fL -o lib/aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

# Tiering service
curl -fL -o "opt/fluss-flink-tiering-$FLUSS_DOCKER_VERSION$.jar" "https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-tiering/$FLUSS_DOCKER_VERSION$/fluss-flink-tiering-$FLUSS_DOCKER_VERSION$.jar"
```

:::info
You can add more jars to this `lib` directory based on your requirements:
- **Custom Hadoop configurations**: Add jars for specific HDFS distributions or custom authentication mechanisms
- **Other catalog backends**: Add jars needed for alternative Iceberg catalog implementations (e.g., Rest, Hive, Glue)
:::

3. Create a `docker-compose.yml` file with the following content:

```yaml
services:
  #begin RustFS (S3-compatible storage)
  rustfs:
    image: rustfs/rustfs:latest
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
      - zookeeper
      - rustfs-init
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://coordinator-server:9123
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.path.style.access: true
        datalake.format: iceberg
        datalake.iceberg.type: hadoop
        datalake.iceberg.warehouse: s3a://fluss/iceberg
        datalake.iceberg.iceberg.hadoop.fs.s3a.endpoint: http://rustfs:9000
        datalake.iceberg.iceberg.hadoop.fs.s3a.access.key: rustfsadmin
        datalake.iceberg.iceberg.hadoop.fs.s3a.secret.key: rustfsadmin
        datalake.iceberg.iceberg.hadoop.fs.s3a.path.style.access: true
    volumes:
      - ./lib/fluss-lake-iceberg-$FLUSS_DOCKER_VERSION$.jar:/opt/fluss/plugins/iceberg/fluss-lake-iceberg-$FLUSS_DOCKER_VERSION$.jar
      - ./lib/hadoop-client-api-3.3.5.jar:/opt/fluss/plugins/iceberg/hadoop-client-api-3.3.5.jar
      - ./lib/hadoop-client-runtime-3.3.5.jar:/opt/fluss/plugins/iceberg/hadoop-client-runtime-3.3.5.jar
      - ./lib/hadoop-aws-3.3.5.jar:/opt/fluss/plugins/iceberg/hadoop-aws-3.3.5.jar
      - ./lib/aws-java-sdk-bundle-1.12.367.jar:/opt/fluss/plugins/iceberg/aws-java-sdk-bundle-1.12.367.jar
      - ./lib/commons-logging-1.2.jar:/opt/fluss/plugins/iceberg/commons-logging-1.2.jar
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
        kv.snapshot.interval: 0s
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.path.style.access: true
        datalake.format: iceberg
        datalake.iceberg.type: hadoop
        datalake.iceberg.warehouse: s3a://fluss/iceberg
        datalake.iceberg.iceberg.hadoop.fs.s3a.endpoint: http://rustfs:9000
        datalake.iceberg.iceberg.hadoop.fs.s3a.access.key: rustfsadmin
        datalake.iceberg.iceberg.hadoop.fs.s3a.secret.key: rustfsadmin
        datalake.iceberg.iceberg.hadoop.fs.s3a.path.style.access: true
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  jobmanager:
    image: flink:1.20-scala_2.12-java17
    ports:
      - "8083:8081"
    entrypoint: ["/bin/bash", "-c"]
    command: >
      "sed -i 's/exec $(drop_privs_cmd)//g' /docker-entrypoint.sh &&
       cp /tmp/jars/*.jar /opt/flink/lib/ 2>/dev/null || true;
       cp /tmp/opt/*.jar /opt/flink/opt/ 2>/dev/null || true;
       /docker-entrypoint.sh jobmanager"
    environment:
      - AWS_ACCESS_KEY_ID=rustfsadmin
      - AWS_SECRET_ACCESS_KEY=rustfsadmin
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        flink.hadoop.fs.s3a.endpoint: http://rustfs:9000
        flink.hadoop.fs.s3a.path.style.access: true
    volumes:
      - ./lib:/tmp/jars
      - ./opt:/tmp/opt
  taskmanager:
    image: flink:1.20-scala_2.12-java17
    depends_on:
      - jobmanager
    entrypoint: ["/bin/bash", "-c"]
    command: >
      "sed -i 's/exec $(drop_privs_cmd)//g' /docker-entrypoint.sh &&
       cp /tmp/jars/*.jar /opt/flink/lib/ 2>/dev/null || true;
       cp /tmp/opt/*.jar /opt/flink/opt/ 2>/dev/null || true;
       /docker-entrypoint.sh taskmanager"
    environment:
      - AWS_ACCESS_KEY_ID=rustfsadmin
      - AWS_SECRET_ACCESS_KEY=rustfsadmin
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.task.off-heap.size: 128m
        flink.hadoop.fs.s3a.endpoint: http://rustfs:9000
        flink.hadoop.fs.s3a.path.style.access: true
    volumes:
      - ./lib:/tmp/jars
      - ./opt:/tmp/opt

volumes:
  rustfs-data:
```

The Docker Compose environment consists of the following containers:
- **Fluss Cluster:** a Fluss `CoordinatorServer`, a Fluss `TabletServer` and a `ZooKeeper` server.
- **Flink Cluster**: a Flink `JobManager` and a Flink `TaskManager` container to execute queries.

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
docker compose exec jobmanager ./bin/sql-client.sh
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
  `name` STRING,
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

  </TabItem>

  <TabItem value="iceberg" label="Iceberg">

First, use the following command to enter the Flink SQL CLI Container:
```shell
docker compose exec jobmanager ./bin/sql-client.sh
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
    'bootstrap.servers' = 'coordinator-server:9123'
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
    --datalake.iceberg.type hadoop \
    --datalake.iceberg.warehouse s3a://fluss/iceberg \
    --datalake.iceberg.iceberg.hadoop.fs.s3a.endpoint http://rustfs:9000 \
    --datalake.iceberg.iceberg.hadoop.fs.s3a.access.key rustfsadmin \
    --datalake.iceberg.iceberg.hadoop.fs.s3a.secret.key rustfsadmin \
    --datalake.iceberg.iceberg.hadoop.fs.s3a.path.style.access true
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

### Tiered Storage

You can use the following command to view the files stored on RustFS:

```shell
docker run --rm --net=host \
-e MC_HOST_rustfs=http://rustfsadmin:rustfsadmin@localhost:9000 \
minio/mc ls --recursive rustfs/fluss/                
```

<Tabs groupId="lake-tabs">
  <TabItem value="paimon" label="Paimon" default>

Sample output:

```shell
[2026-02-08 14:04:38 UTC]     0B STANDARD paimon/default.db/
[2026-02-08 14:03:49 UTC]  18KiB STANDARD paimon/fluss.db/datalake_enriched_orders/bucket-0/changelog-05cec7e1-241b-4eb2-98f6-7cba7b028044-0.parquet
[2026-02-08 14:04:49 UTC]  17KiB STANDARD paimon/fluss.db/datalake_enriched_orders/bucket-0/changelog-5b4af110-753a-4a90-a88c-575a358172cd-0.parquet
[2026-02-08 14:03:49 UTC]  18KiB STANDARD paimon/fluss.db/datalake_enriched_orders/bucket-0/data-05cec7e1-241b-4eb2-98f6-7cba7b028044-1.parquet
[2026-02-08 14:04:49 UTC]  17KiB STANDARD paimon/fluss.db/datalake_enriched_orders/bucket-0/data-5b4af110-753a-4a90-a88c-575a358172cd-1.parquet
[2026-02-08 14:03:50 UTC] 2.3KiB STANDARD paimon/fluss.db/datalake_enriched_orders/manifest/manifest-682241ff-2c6e-4e2b-97d1-812a5c8c6513-0
[2026-02-08 14:03:50 UTC] 2.3KiB STANDARD paimon/fluss.db/datalake_enriched_orders/manifest/manifest-682241ff-2c6e-4e2b-97d1-812a5c8c6513-1
[2026-02-08 14:04:49 UTC] 2.3KiB STANDARD paimon/fluss.db/datalake_enriched_orders/manifest/manifest-e9a4dee4-9a39-4cf7-8711-37e971f3a992-0
[2026-02-08 14:04:49 UTC] 2.3KiB STANDARD paimon/fluss.db/datalake_enriched_orders/manifest/manifest-e9a4dee4-9a39-4cf7-8711-37e971f3a992-1
[2026-02-08 14:03:50 UTC]   884B STANDARD paimon/fluss.db/datalake_enriched_orders/manifest/manifest-list-5838d008-153a-47c2-b137-343e52c36cf6-0
[2026-02-08 14:03:50 UTC]   989B STANDARD paimon/fluss.db/datalake_enriched_orders/manifest/manifest-list-5838d008-153a-47c2-b137-343e52c36cf6-1
[2026-02-08 14:03:50 UTC]   985B STANDARD paimon/fluss.db/datalake_enriched_orders/manifest/manifest-list-5838d008-153a-47c2-b137-343e52c36cf6-2
[2026-02-08 14:04:49 UTC]   989B STANDARD paimon/fluss.db/datalake_enriched_orders/manifest/manifest-list-5c48f649-66a5-42a2-aa0c-f73f033dec12-0
[2026-02-08 14:04:49 UTC]   983B STANDARD paimon/fluss.db/datalake_enriched_orders/manifest/manifest-list-5c48f649-66a5-42a2-aa0c-f73f033dec12-1
[2026-02-08 14:04:49 UTC]   984B STANDARD paimon/fluss.db/datalake_enriched_orders/manifest/manifest-list-5c48f649-66a5-42a2-aa0c-f73f033dec12-2
[2026-02-08 14:02:55 UTC] 1.6KiB STANDARD paimon/fluss.db/datalake_enriched_orders/schema/schema-0
[2026-02-08 14:04:49 UTC]     1B STANDARD paimon/fluss.db/datalake_enriched_orders/snapshot/LATEST
[2026-02-08 14:03:50 UTC]   830B STANDARD paimon/fluss.db/datalake_enriched_orders/snapshot/snapshot-1
[2026-02-08 14:04:49 UTC]   831B STANDARD paimon/fluss.db/datalake_enriched_orders/snapshot/snapshot-2
[2026-02-08 14:04:49 UTC]    50B STANDARD remote-data/lake/fluss/datalake_enriched_orders-3/metadata/ad03c401-2002-414f-8545-de9512a60f44.offsets
```
  </TabItem>

  <TabItem value="iceberg" label="Iceberg">

Sample output:

```shell
[2026-02-06 20:11:52 UTC] 9.7KiB STANDARD iceberg/fluss/datalake_enriched_orders/data/__bucket=0/00000-0-315417a0-d83b-4a54-a706-58154f2f049a-00001.parquet
[2026-02-06 20:11:22 UTC] 9.5KiB STANDARD iceberg/fluss/datalake_enriched_orders/data/__bucket=0/00000-0-a5cc57b7-72f0-415a-b817-f6fe29ed17c2-00001.parquet
[2026-02-06 20:10:52 UTC]  14KiB STANDARD iceberg/fluss/datalake_enriched_orders/data/__bucket=0/00000-0-c555c8f6-e200-415e-97c2-4bf5cf283ab5-00001.parquet
[2026-02-06 20:09:53 UTC]  15KiB STANDARD iceberg/fluss/datalake_enriched_orders/data/__bucket=0/00000-0-df6952fd-ecb3-4f9f-afea-60e176c0c69f-00001.parquet
[2026-02-06 20:10:52 UTC] 8.1KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/014f763d-a7e4-4716-bd9c-5fede24c2f27-m0.avro
[2026-02-06 20:09:53 UTC] 8.1KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/50786e7f-d4b6-4821-a173-1927f67017a5-m0.avro
[2026-02-06 20:11:22 UTC] 8.1KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/be02a347-7781-4195-82e3-5c5dbcef06e5-m0.avro
[2026-02-06 20:11:52 UTC] 8.1KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/c606fd42-9452-46d9-a258-d731332ba3af-m0.avro
[2026-02-06 20:09:53 UTC] 4.4KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/snap-2826026129110087788-1-50786e7f-d4b6-4821-a173-1927f67017a5.avro
[2026-02-06 20:11:22 UTC] 4.5KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/snap-4708533898857736722-1-be02a347-7781-4195-82e3-5c5dbcef06e5.avro
[2026-02-06 20:11:52 UTC] 4.5KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/snap-5039287922324762391-1-c606fd42-9452-46d9-a258-d731332ba3af.avro
[2026-02-06 20:10:52 UTC] 4.4KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/snap-7111191572558807319-1-014f763d-a7e4-4716-bd9c-5fede24c2f27.avro
[2026-02-06 20:09:01 UTC] 1.8KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/v1.metadata.json
[2026-02-06 20:09:53 UTC] 2.8KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/v2.metadata.json
[2026-02-06 20:10:52 UTC] 3.8KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/v3.metadata.json
[2026-02-06 20:11:22 UTC] 4.8KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/v4.metadata.json
[2026-02-06 20:11:52 UTC] 5.8KiB STANDARD iceberg/fluss/datalake_enriched_orders/metadata/v5.metadata.json
[2026-02-06 20:11:52 UTC]     1B STANDARD iceberg/fluss/datalake_enriched_orders/metadata/version-hint.text
[2026-02-06 20:11:52 UTC]    50B STANDARD remote-data/lake/fluss/datalake_enriched_orders-3/metadata/b47da549-c4d7-4443-be56-b99e917ce267.offsets
```

  </TabItem>
</Tabs>


## Clean up
After finishing the tutorial, run `exit` to exit Flink SQL CLI Container and then run 
```shell
docker compose down -v
```
to stop all containers.