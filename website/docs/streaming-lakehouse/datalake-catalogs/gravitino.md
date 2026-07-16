---
title: Gravitino
sidebar_position: 3
---

# Apache Gravitino

## Introduction

[Apache Gravitino](https://gravitino.apache.org/) is a high-performance metadata management system that provides a unified metadata layer for data lake and lakehouse architectures. It offers an [Iceberg REST Catalog](https://iceberg.apache.org/rest-catalog-spec) interface with credential vending support, making tiered Iceberg tables discoverable and queryable by any Iceberg-compatible engine.

This guide explains how to configure Fluss to use Apache Gravitino as its Iceberg REST catalog. For general Iceberg integration details (table mapping, data types, limitations), see [Iceberg Integration](/docs/next/streaming-lakehouse/datalake-formats/iceberg/).

## How It Works

When Fluss is configured with Gravitino as its Iceberg REST catalog:

1. Fluss creates and manages Iceberg table metadata through Gravitino's REST API
2. The [tiering service](/docs/next/install-deploy/deploying-streaming-lakehouse/#starting-the-tiering-service) writes data to object storage and commits snapshots via Gravitino
3. Gravitino provides S3 credentials dynamically through credential vending (no static credentials needed)
4. Any Iceberg-compatible engine (Flink, Spark, Trino, StarRocks, etc.) can discover and query the tiered tables through Gravitino

## Prerequisites

### Running Gravitino Instance

You need a running Gravitino instance (version 1.3.0+). Refer to the [Gravitino Getting Started](https://gravitino.apache.org/docs/1.3.0/getting-started/index) guide for deployment instructions.

### AWS Bundle for S3 Support

Gravitino requires the AWS bundle for S3 support:

**For Docker deployments:**
The `apache/gravitino:latest` image already includes the required bundle.

**For native deployments:**
Download and place the AWS bundle in `iceberg-rest-server/libs/`:

```bash
wget -P ${GRAVITINO_HOME}/iceberg-rest-server/libs/ \
    https://repo1.maven.org/maven2/org/apache/gravitino/iceberg-aws-bundle/1.3.0/iceberg-aws-bundle-1.3.0.jar
```

### Create Gravitino Catalog with Credential Vending

After deploying Gravitino, create a metalake and catalog with credential vending enabled:

```bash
# Create metalake
curl -X POST http://<gravitino-host>:8090/api/metalakes \
    -H "Content-Type: application/json" \
    -d '{"name": "{metalake name}", "comment": "Fluss metadata lake"}'

# Create Iceberg catalog with credential vending
curl -X POST http://<gravitino-host>:8090/api/metalakes/{metalake name}/catalogs \
    -H "Content-Type: application/json" \
    -d '{
      "name": "fluss_iceberg_catalog",
      "type": "RELATIONAL",
      "provider": "lakehouse-iceberg",
      "comment": "Iceberg REST catalog for Fluss",
      "properties": {
        "uri": "jdbc:postgresql://<postgres-host>:5432/gravitino",
        "warehouse": "s3://<bucket>/iceberg-data/",
        "catalog-backend": "jdbc",
        "jdbc-driver": "org.postgresql.Driver",
        "jdbc-user": "postgres",
        "jdbc-password": "password",
        "s3-endpoint": "http://<s3-endpoint>:9000",
        "s3-access-key-id": "<access-key>",
        "s3-secret-access-key": "<secret-key>",
        "credential-providers": "s3-secret-key"
      }
    }'
```

> **NOTE**: The `credential-providers: s3-secret-key` property enables credential vending. Gravitino will provide temporary S3 credentials to Fluss instead of requiring static credentials in Fluss configuration.

## Configure Fluss with Gravitino

### Cluster Configuration

Add the following to your `server.yaml`:

```yaml
datalake.format: iceberg
datalake.iceberg.type: rest
datalake.iceberg.uri: http://<gravitino-host>:9001/iceberg/
datalake.iceberg.warehouse: fluss_iceberg_catalog
datalake.iceberg.header.X-Iceberg-Access-Delegation: vended-credentials
```

Fluss strips the `datalake.iceberg.` prefix and passes the remaining properties to the Iceberg REST catalog client. You can add any additional [Iceberg REST catalog properties](https://iceberg.apache.org/docs/1.10.1/configuration/#catalog-properties) using the same prefix. For example:

```yaml
# Optional: pass additional REST catalog properties
datalake.iceberg.header.Authorization: Bearer <token>
```

**Key Properties:**

- `datalake.iceberg.uri`: Gravitino's Iceberg REST API endpoint (port 9001)
- `datalake.iceberg.warehouse`: The catalog name created in Gravitino (not `metalake.catalog`, just `catalog`)
- `datalake.iceberg.header.X-Iceberg-Access-Delegation: vended-credentials`: Enables credential vending

With credential vending enabled, you do **not** need to specify `s3-access-key-id` or `s3-secret-access-key` in Fluss configuration. However, you still need S3 endpoint configuration:

```yaml
# S3 endpoint configuration (required)
datalake.iceberg.s3.endpoint: http://<s3-endpoint>:9000
datalake.iceberg.s3.path-style-access: true
datalake.iceberg.s3.region: us-east-1
```

#### Hadoop Dependencies

Some FileIO implementations require Hadoop classes. Place the pre-bundled Hadoop JAR into `FLUSS_HOME/plugins/iceberg/`:

```bash
wget -P ${FLUSS_HOME}/plugins/iceberg/ \
    https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar
```

See [Iceberg - Hadoop Dependencies](/docs/next/streaming-lakehouse/datalake-formats/iceberg/#1-hadoop-dependencies-configuration) for alternative approaches.

### Start Tiering Service

Follow the [Iceberg tiering service setup](/docs/next/streaming-lakehouse/datalake-formats/iceberg/#start-tiering-service-to-iceberg) to prepare the required JARs and start the tiering service. Use REST catalog parameters with credential vending when launching the Flink tiering job:

```bash
${FLINK_HOME}/bin/flink run /path/to/fluss-flink-tiering-*.jar \
    --fluss.bootstrap.servers <coordinator-host>:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type rest \
    --datalake.iceberg.uri http://<gravitino-host>:9001/iceberg/ \
    --datalake.iceberg.warehouse fluss_iceberg_catalog \
    --datalake.iceberg.header.X-Iceberg-Access-Delegation vended-credentials \
    --datalake.iceberg.s3.endpoint http://<s3-endpoint>:9000 \
    --datalake.iceberg.s3.path-style-access true
```

> **NOTE**: Credential vending secures the Fluss-to-Iceberg communication but doesn't affect Flink-to-S3 reads. Flink requires AWS SDK credentials for direct S3 access.

## Metalake Configuration

The metalake is **not** specified in Fluss configuration. Instead, it's configured on the Gravitino server side in `gravitino.conf`:

```properties
gravitino.iceberg-rest.catalog-config-provider = dynamic-config-provider
gravitino.iceberg-rest.gravitino-uri = http://localhost:8090
gravitino.iceberg-rest.gravitino-metalake = {metalake name}
```

When Fluss requests a catalog by name (e.g., `fluss_iceberg_catalog`), Gravitino's REST service automatically looks it up in the configured metalake (`{metalake name}`). This allows centralized management without requiring clients to know about metalakes.

## Usage Example

### Create a Datalake-Enabled Table

```sql
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

Once the tiering service is running, Fluss automatically creates the corresponding Iceberg table in Gravitino and begins tiering data.

### Query Data

```sql
SET 'execution.runtime-mode' = 'batch';

-- Union read: combines fresh data in Fluss with historical data in Iceberg
SELECT COUNT(*) FROM orders;
```

For details on union reads, streaming reads, and reading with other engines, see [Iceberg - Read Tables](/docs/next/streaming-lakehouse/datalake-formats/iceberg/#read-tables).

## Further Reading

- [Iceberg Integration](/docs/next/streaming-lakehouse/datalake-formats/iceberg/) - Table mapping, data types, supported catalog types, and limitations
- [Lakehouse Storage](/docs/next/maintenance/tiered-storage/lakehouse-storage/) - General tiered storage setup
- [Gravitino Documentation](https://gravitino.apache.org/docs/latest/) - Deploying and managing Gravitino
- [Gravitino Iceberg REST Service](https://gravitino.apache.org/docs/latest/iceberg-rest-service/) - REST catalog configuration reference
