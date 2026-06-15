---
title: AWS Glue
sidebar_position: 2
---

# AWS Glue

## Introduction

[AWS Glue](https://aws.amazon.com/glue/) is a serverless data integration service that includes a central metadata repository known as the AWS Glue Data Catalog. The Glue Data Catalog is compatible with Apache Iceberg, making it a convenient option for managing Iceberg table metadata on AWS.

This guide explains how to configure Fluss to use AWS Glue as its Iceberg catalog. For general Iceberg integration details (table mapping, data types, limitations), see [Iceberg](../formats/iceberg.md).

## How It Works

When Fluss is configured with AWS Glue as its Iceberg catalog:

1. Fluss creates and manages Iceberg database and table metadata within the AWS Glue Data Catalog.
2. The [tiering service](maintenance/tiered-storage/lakehouse-storage.md#start-the-datalake-tiering-service) writes Parquet data files to Amazon S3 and commits snapshots to the Glue Data Catalog.
3. Any Iceberg-compatible engine (Amazon Athena, Spark, Trino, Flink, StarRocks, etc.) can discover and query the tiered tables through AWS Glue.

## Prerequisites

### AWS IAM Permissions

The processes running Fluss servers and the Flink tiering service must have IAM permissions for both Glue and S3. Below is a minimal IAM policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:CreateDatabase",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:DeleteDatabase",
        "glue:CreateTable",
        "glue:GetTable",
        "glue:GetTables",
        "glue:UpdateTable",
        "glue:DeleteTable"
      ],
      "Resource": [
        "arn:aws:glue:<region>:<account-id>:catalog",
        "arn:aws:glue:<region>:<account-id>:database/*",
        "arn:aws:glue:<region>:<account-id>:table/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::<your-bucket>",
        "arn:aws:s3:::<your-bucket>/*"
      ]
    }
  ]
}
```

### Prepare Required JARs

Fluss bundles `iceberg-core` but does **not** bundle the Glue catalog implementation or the AWS SDK. You must supply additional JARs.

#### For Fluss Servers (Coordinator & Tablet Servers)

Place the following JARs in the `${FLUSS_HOME}/plugins/iceberg/` directory:

- **Iceberg AWS Bundle**: [iceberg-aws-bundle-1.10.1.jar](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar)
- **Iceberg AWS**: [iceberg-aws-1.10.1.jar](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.10.1/iceberg-aws-1.10.1.jar)

The bundle JAR contains all required AWS SDK v2 dependencies, while `iceberg-aws` contains the `GlueCatalog` implementation class. Both are required because Fluss's plugin classloader loads `GlueCatalog` via reflection, and the bundle alone does not include it on the expected classpath.

> **NOTE**: You need **both** JARs. The `iceberg-aws-bundle` provides the AWS SDK, and `iceberg-aws` provides `GlueCatalog` and `S3FileIO` classes that Iceberg loads via reflection.

Additionally, you need the Fluss S3 filesystem plugin for remote storage access. Place [fluss-fs-s3-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-fs-s3/$FLUSS_VERSION$/fluss-fs-s3-$FLUSS_VERSION$.jar) in `${FLUSS_HOME}/plugins/s3/`. See [S3 Dependencies](../../../maintenance/filesystems/s3.md#dependencies) for details.

#### For the Flink Tiering Service

Place the following JARs in `${FLINK_HOME}/lib`:

1. **Iceberg AWS Bundle**: `iceberg-aws-bundle-1.10.1.jar` (same as above)
2. **Iceberg AWS**: `iceberg-aws-1.10.1.jar` (same as above — provides `GlueCatalog` class)
3. **Fluss Flink Connector**: [fluss-flink-1.20-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar) (pick the version matching your Flink runtime)
4. **Fluss Lake Iceberg**: [fluss-lake-iceberg-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-lake-iceberg/$FLUSS_VERSION$/fluss-lake-iceberg-$FLUSS_VERSION$.jar)
5. **Fluss S3 Filesystem**: [fluss-fs-s3-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-fs-s3/$FLUSS_VERSION$/fluss-fs-s3-$FLUSS_VERSION$.jar) (if S3 is used as Fluss remote storage)

> **NOTE**: The Glue catalog does **not** require Hadoop classes. You do not need `hadoop-apache-*.jar` or `HADOOP_CLASSPATH` for a Glue + S3FileIO setup.

## Configure Fluss with AWS Glue

### Cluster Configuration

Add the following to your `server.yaml`:

```yaml
datalake.format: iceberg
datalake.iceberg.type: glue
datalake.iceberg.warehouse: s3://<your-bucket>/<warehouse-path>
```

Fluss strips the `datalake.iceberg.` prefix and passes the remaining properties directly to Iceberg's Glue catalog. The properties above become `type=glue` and `warehouse=s3://...` when initializing the catalog.

You can pass any [Iceberg AWS catalog property](https://iceberg.apache.org/docs/1.10.1/aws/#glue-catalog) using the same prefix. Common additional properties:

```yaml
# Specify the AWS region (required if not using default region from credentials chain)
datalake.iceberg.client.region: us-east-1

# Use S3FileIO explicitly (Glue catalog defaults to this when warehouse is s3://)
datalake.iceberg.io-impl: org.apache.iceberg.aws.s3.S3FileIO

# Optional: restrict catalog to a specific Glue database prefix
datalake.iceberg.glue.catalog-id: <aws-account-id>
```

#### Authentication

**IAM Role (Recommended)**: If running on AWS (EKS, ECS, EC2) with an attached IAM role, no credential configuration is needed. The AWS SDK uses the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) automatically.

**Static Credentials (Testing Only)**:

```yaml
datalake.iceberg.client.credentials-provider: org.apache.iceberg.aws.StaticCredentialsProvider
datalake.iceberg.client.credentials-provider.access-key-id: <your-access-key>
datalake.iceberg.client.credentials-provider.secret-access-key: <your-secret-key>
```

### Start Tiering Service

Follow the [Iceberg tiering service setup](../formats/iceberg.md#start-tiering-service-to-iceberg) to prepare the required JARs. Launch the Flink tiering job with Glue parameters:

```bash
${FLINK_HOME}/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers <coordinator-host>:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type glue \
    --datalake.iceberg.warehouse s3://<your-bucket>/<warehouse-path> \
    --datalake.iceberg.client.region <your-aws-region>
```

## Usage Example

### Create a Datalake-Enabled Table

Connect to Fluss via Flink SQL and create a table with data lake tiering enabled:

```sql title="Flink SQL"
USE CATALOG fluss_catalog;

CREATE TABLE customer_orders (
    `order_id` BIGINT,
    `customer_name` STRING,
    `total_amount` DECIMAL(15, 2),
    `order_date` STRING,
    PRIMARY KEY (`order_id`) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

Fluss will create the database (if it does not exist) and a corresponding Iceberg table in the Glue Data Catalog. Once data is ingested and the tiering service commits, Parquet files appear in the S3 warehouse path.

### Query Data with Athena

AWS Athena uses the Glue Data Catalog by default, so tiered tables are immediately queryable:

```sql title="Athena SQL"
-- Replace with your Fluss database name
SELECT * FROM fluss_database.customer_orders LIMIT 10;
```

### Query Data with Flink (Union Read)

```sql title="Flink SQL"
SET 'execution.runtime-mode' = 'batch';

-- Union read: combines fresh data in Fluss with historical data in Iceberg
SELECT COUNT(*) FROM customer_orders;
```

For details on union reads and streaming reads, see [Iceberg - Read Tables](../formats/iceberg.md#read-tables).

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `NoClassDefFoundError: software/amazon/awssdk/...` | Missing AWS SDK | Ensure `iceberg-aws-bundle-1.10.1.jar` is in `plugins/iceberg/` and `${FLINK_HOME}/lib` |
| `NoClassDefFoundError: org/apache/iceberg/aws/glue/GlueCatalog` | Missing Glue catalog impl | Ensure `iceberg-aws-1.10.1.jar` (not just the bundle) is in `plugins/iceberg/` and `${FLINK_HOME}/lib` |
| `ClassNotFoundException: org.apache.iceberg.aws.glue.GlueCatalog` | Missing `iceberg-aws` JAR | The bundle does not include `GlueCatalog` on the plugin classloader path — add `iceberg-aws-1.10.1.jar` alongside the bundle |
| `AccessDeniedException` from Glue API | Insufficient IAM permissions | Check the IAM policy includes all required `glue:*` actions |
| `403 Forbidden` writing to S3 | Missing S3 permissions | Ensure the IAM policy includes `s3:PutObject`, `s3:GetObject`, etc. on the warehouse bucket |
| `Region must be specified` | Missing region config | Add `datalake.iceberg.client.region` or set `AWS_REGION` env var |
| `NoAwsCredentialsException` | S3 plugin can't find credentials | Set `s3.access.key`, `s3.secret.key`, and `s3.session.token` in `server.yaml`, or ensure IMDSv1 is enabled on the instance |
| `NoSuchMethodError: MappedByteBuffer.duplicate()` | Java version too old | Fluss 0.9.x requires **Java 17+** for the Tablet Server. Upgrade from Java 11 to Java 17. |

## Further Reading

- [Iceberg Integration](../formats/iceberg.md) — Table mapping, data types, supported catalog types, and limitations
- [Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md) — General tiered storage setup
- [S3 Filesystem](../../../maintenance/filesystems/s3.md) — Configuring S3 as Fluss remote storage
- [Iceberg AWS Docs](https://iceberg.apache.org/docs/1.10.1/aws/) — Full reference for Glue and S3 properties
