---
title: Lakekeeper
sidebar_position: 1
---

# Lakekeeper

## Introduction

[Lakekeeper](https://lakekeeper.io/) is an Apache-licensed Iceberg REST Catalog written in Rust. It provides a standards-compliant [Iceberg REST Catalog](https://iceberg.apache.org/concepts/catalog/#decoupling-using-the-rest-catalog) interface, making tiered Iceberg tables discoverable and queryable by any Iceberg-compatible engine.

This guide explains how to configure Fluss to use Lakekeeper as its Iceberg catalog. For general Iceberg integration details (table mapping, data types, limitations), see [Iceberg](../formats/iceberg.md).

## How It Works

When Fluss is configured with Lakekeeper as its Iceberg REST catalog:

1. Fluss creates and manages Iceberg table metadata through Lakekeeper's REST API
2. The [tiering service](maintenance/tiered-storage/lakehouse-storage.md#start-the-datalake-tiering-service) writes data to object storage and commits snapshots via Lakekeeper
3. Any Iceberg-compatible engine (Flink, Spark, Trino, StarRocks, etc.) can discover and query the tiered tables through Lakekeeper

## Prerequisites

### Running Lakekeeper Instance

You need a running Lakekeeper instance with a warehouse configured. Refer to the [Lakekeeper Getting Started](https://docs.lakekeeper.io/getting-started/) guide for deployment instructions.

After deploying Lakekeeper, create a warehouse that points to your object storage (e.g., S3):

```bash
# Bootstrap Lakekeeper (first-time setup)
curl -X POST http://<lakekeeper-host>:8181/management/v1/bootstrap \
    -H "Content-Type: application/json" \
    -d '{"accept-terms-of-use": true}'

# Create a warehouse
curl -X POST http://<lakekeeper-host>:8181/management/v1/warehouse \
    -H "Content-Type: application/json" \
    -d '{
      "warehouse-name": "my-warehouse",
      "project-id": "00000000-0000-0000-0000-000000000000",
      "storage-profile": {
        "type": "s3",
        "bucket": "<your-bucket>",
        "key-prefix": "iceberg",
        "endpoint": "http://<s3-endpoint>:9000",
        "region": "us-east-1",
        "path-style-access": true
      },
      "storage-credential": {
        "type": "s3",
        "credential-type": "access-key",
        "aws-access-key-id": "<access-key>",
        "aws-secret-access-key": "<secret-key>"
      }
    }'
```

> **NOTE**: Adjust the `storage-profile` and `storage-credential` to match your storage backend. Lakekeeper supports S3, GCS, Azure, and local storage. See [Lakekeeper Storage](https://docs.lakekeeper.io) for details.

## Configure Fluss with Lakekeeper

### Cluster Configuration

Add the following to your `server.yaml`:

```yaml
datalake.format: iceberg
datalake.iceberg.type: rest
datalake.iceberg.uri: http://<lakekeeper-host>:8181/catalog
datalake.iceberg.warehouse: <warehouse-name>
```

Fluss strips the `datalake.iceberg.` prefix and passes the remaining properties to the Iceberg REST catalog client. You can add any additional [Iceberg REST catalog properties](https://iceberg.apache.org/docs/1.10.1/configuration/#catalog-properties) using the same prefix. For example:

```yaml
# Optional: pass additional REST catalog properties
datalake.iceberg.header.Authorization: Bearer <token>
```

#### Hadoop Dependencies

Some FileIO implementations require Hadoop classes. Place the pre-bundled Hadoop JAR into `FLUSS_HOME/plugins/iceberg/`:

```bash
wget -P ${FLUSS_HOME}/plugins/iceberg/ \
    https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar
```

See [Iceberg - Hadoop Dependencies](../formats/iceberg.md#1-hadoop-dependencies-configuration) for alternative approaches.

### Start Tiering Service

Follow the [Iceberg tiering service setup](../formats/iceberg.md#start-tiering-service-to-iceberg) to prepare the required JARs and start the tiering service. Use REST catalog parameters when launching the Flink tiering job:

```bash
${FLINK_HOME}/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers <coordinator-host>:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type rest \
    --datalake.iceberg.uri http://<lakekeeper-host>:8181/catalog \
    --datalake.iceberg.warehouse <warehouse-name>
```

## Usage Example

### Create a Datalake-Enabled Table

```sql title="Flink SQL"
USE CATALOG fluss_catalog;

CREATE TABLE orders (
    `order_id` BIGINT,
    `customer_id` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `status` STRING,
    PRIMARY KEY (`order_id`) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

Once the tiering service is running, Fluss automatically creates the corresponding Iceberg table in Lakekeeper and begins tiering data.

### Query Data

```sql title="Flink SQL"
SET 'execution.runtime-mode' = 'batch';

-- Union read: combines fresh data in Fluss with historical data in Iceberg
SELECT COUNT(*) FROM orders;
```

For details on union reads, streaming reads, and reading with other engines, see [Iceberg - Read Tables](../formats/iceberg.md#read-tables).

## Further Reading

- [Iceberg Integration](../formats/iceberg.md) - Table mapping, data types, supported catalog types, and limitations
- [Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md) - General tiered storage setup
- [Lakekeeper Documentation](https://docs.lakekeeper.io) - Deploying and managing Lakekeeper
