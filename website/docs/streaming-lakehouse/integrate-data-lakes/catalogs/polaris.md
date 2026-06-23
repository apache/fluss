---
title: Polaris
sidebar_position: 2
---

# Polaris

## Introduction

[Apache Polaris](https://polaris.apache.org/) is an open-source, fully-featured catalog for Apache Iceberg. It implements Iceberg's [REST Catalog](https://iceberg.apache.org/concepts/catalog/#decoupling-using-the-rest-catalog) interface, making Iceberg tables discoverable and queryable by any Iceberg-compatible engine, with role-based access control and credential vending built in.

This guide explains how to configure Fluss to use Polaris as its Iceberg catalog. For general Iceberg integration details (table mapping, data types, limitations), see [Iceberg](../formats/iceberg.md).

## How It Works

When Fluss is configured with Polaris as its Iceberg REST catalog:

1. Fluss creates and manages Iceberg table metadata through Polaris's REST API
2. The [tiering service](maintenance/tiered-storage/lakehouse-storage.md#start-the-datalake-tiering-service) writes data to object storage and commits snapshots via Polaris
3. Any Iceberg-compatible engine (Flink, Spark, Trino, StarRocks, etc.) can discover and query the tiered tables through Polaris

## Prerequisites

### Running Polaris Instance

You need a running Polaris instance with a catalog and a principal (a `client_id` / `client_secret` pair) that can access it. The fastest way to get started is the [Polaris Quickstart](https://polaris.apache.org/), which starts Polaris and automatically creates a `quickstart_catalog` plus a `quickstart_user` principal, printing the principal's credentials in the container logs.

To create a catalog manually, first obtain an access token with your root credentials:

```bash
export TOKEN=$(curl -s http://<polaris-host>:8181/api/catalog/v1/oauth/tokens \
    -d 'grant_type=client_credentials' \
    -d 'client_id=<root-client-id>' \
    -d 'client_secret=<root-client-secret>' \
    -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r '.access_token')
```

Then create a catalog backed by your object storage:

```bash
curl -X POST http://<polaris-host>:8181/api/management/v1/catalogs \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{
      "catalog": {
        "name": "my_catalog",
        "type": "INTERNAL",
        "properties": { "default-base-location": "s3://my-bucket/iceberg" },
        "storageConfigInfo": {
          "storageType": "S3",
          "allowedLocations": ["s3://my-bucket/iceberg"],
          "roleArn": "<your-role-arn>"
        }
      }
    }'
```

> **NOTE**: Adjust the `storageConfigInfo` to match your storage backend. Polaris supports S3, Azure, and GCS. You also need a principal with a role granting `TABLE_WRITE_DATA` on the catalog — see the [Polaris documentation](https://polaris.apache.org/) for catalog, principal, and access-control setup.

## Configure Fluss with Polaris

### Cluster Configuration

Add the following to your `server.yaml`:

```yaml
datalake.format: iceberg
datalake.iceberg.type: rest
datalake.iceberg.uri: http://<polaris-host>:8181/api/catalog
datalake.iceberg.warehouse: <catalog-name>
datalake.iceberg.credential: <client-id>:<client-secret>
datalake.iceberg.scope: PRINCIPAL_ROLE:ALL
datalake.iceberg.header.X-Iceberg-Access-Delegation: vended-credentials
```

Fluss strips the `datalake.iceberg.` prefix and passes the remaining properties to the Iceberg REST catalog client. The `credential` (`client_id:client_secret`) and `scope` properties configure OAuth2 client-credentials authentication; the Iceberg client derives the token endpoint from `uri` (`/v1/oauth/tokens`), which matches the Polaris endpoint. You can add any additional [Iceberg REST catalog properties](https://iceberg.apache.org/docs/1.10.1/configuration/#catalog-properties) using the same prefix.

> With credential vending enabled (`X-Iceberg-Access-Delegation: vended-credentials`), Polaris returns temporary, scoped storage credentials for each table request, so Fluss does not need static object-storage credentials.

#### Hadoop Dependencies

Some FileIO implementations require Hadoop classes. Place the pre-bundled Hadoop JAR into `FLUSS_HOME/plugins/iceberg/`:

```bash
wget -P ${FLUSS_HOME}/plugins/iceberg/ \
    https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar
```

See [Iceberg - Hadoop Dependencies](../formats/iceberg.md#1-hadoop-dependencies-configuration) for alternative approaches.

### Start Tiering Service

Follow the [Iceberg tiering service setup](../formats/iceberg.md#start-tiering-service-to-iceberg) to prepare the required JARs and start the tiering service. Use the REST catalog parameters when launching the Flink tiering job:

```bash
${FLINK_HOME}/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers <coordinator-host>:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type rest \
    --datalake.iceberg.uri http://<polaris-host>:8181/api/catalog \
    --datalake.iceberg.warehouse <catalog-name> \
    --datalake.iceberg.credential <client-id>:<client-secret> \
    --datalake.iceberg.scope PRINCIPAL_ROLE:ALL \
    --datalake.iceberg.header.X-Iceberg-Access-Delegation vended-credentials
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

Once the tiering service is running, Fluss automatically creates the corresponding Iceberg table in Polaris and begins tiering data.

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
- [Apache Polaris Documentation](https://polaris.apache.org/) - Deploying and managing Polaris, catalogs, principals, and access control
