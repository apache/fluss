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

### Java Version

Fluss 0.9.x requires **Java 17 or later** for the Tablet Server. Java 11 will fail with `NoSuchMethodError: MappedByteBuffer.duplicate()`.

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

> **NOTE**: If your account uses **AWS Lake Formation**, the IAM role must also have Lake Formation permissions (Create Table, Describe, Alter, Insert, Select, Delete, Drop) on the target database. Standard IAM `glue:*` permissions alone are not sufficient when Lake Formation governance is enabled.

### Prepare Required JARs

Fluss bundles `iceberg-core` but does **not** bundle the Glue catalog implementation or the AWS SDK. You must supply additional JARs.

#### For Fluss Servers (Coordinator & Tablet Servers)

Place the following JARs in the `${FLUSS_HOME}/plugins/iceberg/` directory:

- **Iceberg AWS Bundle**: [iceberg-aws-bundle-1.10.1.jar](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar)
- **Iceberg AWS**: [iceberg-aws-1.10.1.jar](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.10.1/iceberg-aws-1.10.1.jar)
- **Failsafe**: [failsafe-3.3.2.jar](https://repo1.maven.org/maven2/dev/failsafe/failsafe/3.3.2/failsafe-3.3.2.jar) — required by `iceberg-aws` for Glue API retry logic

Both Iceberg JARs are required. The bundle provides AWS SDK v2 dependencies, while `iceberg-aws` provides the `GlueCatalog` class that Iceberg loads via reflection. The plugin classloader cannot find `GlueCatalog` from the bundle alone. The `failsafe` library is a transitive dependency of `iceberg-aws` that is not bundled.

> **NOTE**: You need **all three** JARs. Using only the bundle will result in `ClassNotFoundException: org.apache.iceberg.aws.glue.GlueCatalog`. Missing `failsafe` will cause `NoClassDefFoundError: dev/failsafe/FailsafeException`.

Additionally, you need the Fluss S3 filesystem plugin for remote storage access. Place [fluss-fs-s3-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-fs-s3/$FLUSS_VERSION$/fluss-fs-s3-$FLUSS_VERSION$.jar) in `${FLUSS_HOME}/plugins/s3/`. See [S3 Dependencies](../../../maintenance/filesystems/s3.md#dependencies) for details.

#### For the Flink Tiering Service

Place the following JARs in `${FLINK_HOME}/lib`:

1. **Iceberg AWS Bundle**: `iceberg-aws-bundle-1.10.1.jar` (same as above)
2. **Iceberg AWS**: `iceberg-aws-1.10.1.jar` (same as above — provides `GlueCatalog` class)
3. **Failsafe**: `failsafe-3.3.2.jar` (same as above — Glue retry logic)
4. **Fluss Flink Connector**: [fluss-flink-1.20-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar) (pick the version matching your Flink runtime)
5. **Fluss Lake Iceberg**: [fluss-lake-iceberg-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-lake-iceberg/$FLUSS_VERSION$/fluss-lake-iceberg-$FLUSS_VERSION$.jar)
6. **Fluss Flink Tiering**: [fluss-flink-tiering-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-flink-tiering/$FLUSS_VERSION$/fluss-flink-tiering-$FLUSS_VERSION$.jar) — the tiering job JAR itself
7. **Fluss S3 Filesystem**: [fluss-fs-s3-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-fs-s3/$FLUSS_VERSION$/fluss-fs-s3-$FLUSS_VERSION$.jar) (if S3 is used as Fluss remote storage)
8. **Hadoop Client**: [hadoop-client-api-3.3.6.jar](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar) and [hadoop-client-runtime-3.3.6.jar](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar) — required by the Iceberg Parquet writer

> **NOTE**: Despite the Glue catalog itself not requiring Hadoop, the **tiering service** needs Hadoop classes (`org.apache.hadoop.conf.Configuration`) for writing Parquet files via Iceberg. Use the `hadoop-client-api` and `hadoop-client-runtime` JARs (not the full Hadoop distribution or `flink-shaded-hadoop-2-uber`) to avoid classpath conflicts with Flink's bundled Avro version. Using the uber JAR causes `NoSuchMethodError: LogicalTypes.timestampNanos()`.

> **WARNING**: Do **not** add S3 credentials to `flink-conf.yaml` by appending key-value pairs — this can break Flink's memory configuration parser. Instead, use environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`) or a `HADOOP_CONF_DIR` with a `core-site.xml` file.

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

**IAM Role (Recommended)**: If running on AWS (EKS, ECS, EC2) with an attached IAM role, no credential configuration is needed for the **Glue catalog connection**. The Iceberg AWS SDK uses the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) automatically.

However, the **Fluss S3 filesystem plugin** (used for `remote.data.dir`) does not support the standard AWS credential chain on environments using IMDSv2 or ECS task roles. You may need to provide explicit S3 credentials in `server.yaml`:

```yaml
# Required for Fluss S3 plugin on ECS Fargate or IMDSv2-only instances
s3.access.key: <your-access-key>
s3.secret.key: <your-secret-key>
s3.session.token: <your-session-token>
s3.endpoint: s3.<your-region>.amazonaws.com
```

> **TIP**: On ECS Fargate, fetch temporary credentials from the container metadata endpoint (`http://169.254.170.2$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`) and inject them into `server.yaml` at startup.

For the **Flink tiering service**, provide credentials via environment variables and a `HADOOP_CONF_DIR`:

```bash
export AWS_ACCESS_KEY_ID=<your-access-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-key>
export AWS_SESSION_TOKEN=<your-session-token>
export AWS_REGION=<your-region>

# Create core-site.xml for Hadoop S3A (used by Iceberg Parquet writer)
export HADOOP_CONF_DIR=/tmp/hadoop-conf
mkdir -p $HADOOP_CONF_DIR
cat > $HADOOP_CONF_DIR/core-site.xml <<EOF
<configuration>
  <property><name>fs.s3a.access.key</name><value>$AWS_ACCESS_KEY_ID</value></property>
  <property><name>fs.s3a.secret.key</name><value>$AWS_SECRET_ACCESS_KEY</value></property>
  <property><name>fs.s3a.session.token</name><value>$AWS_SESSION_TOKEN</value></property>
  <property><name>fs.s3a.endpoint</name><value>s3.$AWS_REGION.amazonaws.com</value></property>
</configuration>
EOF
```

> **WARNING**: Do not append credential properties to `flink-conf.yaml`. This breaks Flink's YAML parser and causes `IllegalConfigurationException: JobManager memory configuration failed`. Use environment variables and `HADOOP_CONF_DIR` instead.

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
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = '<coordinator-host>:9123'
);

USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS my_database;
USE my_database;

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

Fluss will register the database in ZooKeeper and create a corresponding Iceberg table in the Glue Data Catalog (using the same database name). Once data is ingested and the tiering service commits, Parquet files appear in the S3 warehouse path.

> **NOTE**: The database must be created in the Fluss catalog first — Fluss maintains its own database registry in ZooKeeper. The database name in Fluss maps 1:1 to the Glue database name during tiering.

### Query Data with Athena

AWS Athena uses the Glue Data Catalog by default, so tiered tables are immediately queryable:

```sql title="Athena SQL"
SELECT * FROM my_database.customer_orders LIMIT 10;
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
| `ClassNotFoundException: org.apache.iceberg.aws.glue.GlueCatalog` | Missing `iceberg-aws` JAR | Add `iceberg-aws-1.10.1.jar` alongside the bundle in `plugins/iceberg/` |
| `NoClassDefFoundError: dev/failsafe/FailsafeException` | Missing `failsafe` JAR | Add `failsafe-3.3.2.jar` to `plugins/iceberg/` and `${FLINK_HOME}/lib` |
| `NoClassDefFoundError: software/amazon/awssdk/...` | Missing AWS SDK | Ensure `iceberg-aws-bundle-1.10.1.jar` is in `plugins/iceberg/` and `${FLINK_HOME}/lib` |
| `NoClassDefFoundError: org/apache/hadoop/conf/Configuration` | Missing Hadoop JARs in tiering | Add `hadoop-client-api-3.3.6.jar` and `hadoop-client-runtime-3.3.6.jar` to `${FLINK_HOME}/lib` |
| `NoSuchMethodError: LogicalTypes.timestampNanos()` | Avro version conflict | Do NOT use `flink-shaded-hadoop-2-uber` — use `hadoop-client-api` + `hadoop-client-runtime` instead |
| `NoSuchMethodError: MappedByteBuffer.duplicate()` | Java version too old | Fluss 0.9.x requires **Java 17+**. Upgrade from Java 11. |
| `NoAwsCredentialsException` / `No AWS Credentials` | S3 plugin can't find credentials | Set `s3.access.key`, `s3.secret.key`, `s3.session.token` in `server.yaml`. The Fluss S3 plugin does not support IMDSv2 or ECS task role credentials natively. |
| `SecurityTokenException: Region is not set` | S3 token manager missing region | Add `datalake.iceberg.client.region` and set `AWS_REGION` environment variable |
| `AccessDeniedException` from Glue API | Insufficient IAM or Lake Formation permissions | Check IAM policy includes `glue:CreateTable`. If Lake Formation is enabled, grant table-level permissions to the role. |
| `403 Forbidden` / `400 Bad Request` writing to S3 | S3 credentials expired or misconfigured | For ECS/Fargate, refresh credentials from the container metadata endpoint. Ensure `s3.endpoint` matches your region (e.g., `s3.us-gov-west-1.amazonaws.com` for GovCloud). |
| `Table already exists` on CREATE TABLE | Stale Iceberg metadata in Glue from previous run | DROP TABLE in Fluss first, or use a new table name. Glue retains metadata even after Fluss state (ZooKeeper) is reset. |
| `IllegalConfigurationException: JobManager memory configuration failed` | Flink config file corrupted | Do NOT append keys to `flink-conf.yaml`. Use environment variables or `HADOOP_CONF_DIR` for credential config. |

## Further Reading

- [Iceberg Integration](../formats/iceberg.md) — Table mapping, data types, supported catalog types, and limitations
- [Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md) — General tiered storage setup
- [S3 Filesystem](../../../maintenance/filesystems/s3.md) — Configuring S3 as Fluss remote storage
- [Iceberg AWS Docs](https://iceberg.apache.org/docs/1.10.1/aws/) — Full reference for Glue and S3 properties
