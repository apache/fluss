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

> **TIP**: The Fluss binary distribution already includes `fluss-lake-iceberg-$FLUSS_VERSION$.jar` in `plugins/iceberg/`. You do not need to download it separately — only add the three JARs above (iceberg-aws-bundle, iceberg-aws, failsafe).

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

However, the **Fluss S3 filesystem plugin** (used for `remote.data.dir`) has a known limitation: it uses a custom delegation token mechanism that does not support IMDSv2 or ECS task role credentials. This affects:
- ECS Fargate tasks
- EC2 instances with IMDSv2 enforced (no IMDSv1 fallback)

On these environments, you must provide temporary session credentials explicitly in `server.yaml`:

```yaml
# Workaround for Fluss S3 plugin on IMDSv2-only / ECS Fargate environments
# These are short-lived session credentials, NOT long-term access keys
s3.access.key: <temporary-access-key-id>
s3.secret.key: <temporary-secret-access-key>
s3.session.token: <temporary-session-token>
s3.endpoint: s3.<your-region>.amazonaws.com
```

> **NOTE**: The `s3.endpoint` must match your AWS partition. For GovCloud, use `s3.us-gov-west-1.amazonaws.com`. For China regions, use `s3.cn-north-1.amazonaws.com.cn`. Standard AWS uses `s3.<region>.amazonaws.com`.

These are **temporary session credentials** (from IAM role assumption), not permanent access keys. They expire and must be refreshed. On most environments (EC2 with IMDSv1, EKS with IRSA), no credential configuration is needed — the Fluss S3 plugin resolves credentials automatically.

> **TIP**: On ECS Fargate, fetch temporary credentials from the container metadata endpoint (`http://169.254.170.2$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`) and inject them into `server.yaml` at startup.

For the **Flink tiering service**, the same limitation applies. Provide temporary session credentials via environment variables and a `HADOOP_CONF_DIR` (this is the same workaround as above, scoped to ECS Fargate / IMDSv2 environments):

```bash
# Temporary session credentials (from IAM role, not permanent keys)
export AWS_ACCESS_KEY_ID=<temporary-access-key-id>
export AWS_SECRET_ACCESS_KEY=<temporary-secret-access-key>
export AWS_SESSION_TOKEN=<temporary-session-token>
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

> **NOTE**: On EC2 with IMDSv1 or EKS with IRSA, this `HADOOP_CONF_DIR` step is not needed — Hadoop's default credential chain resolves credentials automatically.

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

## Quick Start (Full Bootstrap)

This section shows the complete setup from a blank Linux machine to data queryable in Athena. Adapt paths and versions for your environment.

```bash
# ─── 1. Install prerequisites ───
# Java 17+ required (Amazon Corretto, OpenJDK, etc.)
java -version  # Must show 17+

# ─── 2. Install ZooKeeper ───
wget -q https://archive.apache.org/dist/zookeeper/zookeeper-3.8.4/apache-zookeeper-3.8.4-bin.tar.gz
tar -xzf apache-zookeeper-3.8.4-bin.tar.gz -C /opt/zookeeper --strip-components=1
cat > /opt/zookeeper/conf/zoo.cfg <<EOF
tickTime=2000
dataDir=/var/lib/zookeeper
clientPort=2181
EOF

# ─── 3. Install Fluss ───
wget -q https://dlcdn.apache.org/incubator/fluss/fluss-0.9.1-incubating/fluss-0.9.1-incubating-bin.tgz
tar -xzf fluss-0.9.1-incubating-bin.tgz -C /opt/fluss --strip-components=1

# ─── 4. Add Glue JARs to Fluss plugins/iceberg/ ───
wget -O /opt/fluss/plugins/iceberg/iceberg-aws-bundle-1.10.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar
wget -O /opt/fluss/plugins/iceberg/iceberg-aws-1.10.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.10.1/iceberg-aws-1.10.1.jar
wget -O /opt/fluss/plugins/iceberg/failsafe-3.3.2.jar \
  https://repo1.maven.org/maven2/dev/failsafe/failsafe/3.3.2/failsafe-3.3.2.jar

# ─── 5. Write server.yaml ───
cat > /opt/fluss/conf/server.yaml <<EOF
zookeeper.address: localhost:2181
coordinator.host: localhost
coordinator.port: 9123
tablet-server.host: localhost
tablet-server.id: 0
tablet-server.port: 9124
data.dir: /var/lib/fluss/data
remote.data.dir: s3://YOUR-BUCKET/fluss-data

datalake.format: iceberg
datalake.iceberg.type: glue
datalake.iceberg.warehouse: s3://YOUR-BUCKET/iceberg-warehouse
datalake.iceberg.client.region: YOUR-REGION
datalake.iceberg.io-impl: org.apache.iceberg.aws.s3.S3FileIO

# S3 credentials (only needed on ECS Fargate / IMDSv2 environments)
# s3.access.key: ...
# s3.secret.key: ...
# s3.session.token: ...
# s3.endpoint: s3.YOUR-REGION.amazonaws.com
EOF

# ─── 6. Start services ───
/opt/zookeeper/bin/zkServer.sh start
sleep 3
/opt/fluss/bin/coordinator-server.sh start
sleep 5
/opt/fluss/bin/tablet-server.sh start
sleep 5

# ─── 7. Install Flink + tiering JARs ───
wget -q https://archive.apache.org/dist/flink/flink-1.20.1/flink-1.20.1-bin-scala_2.12.tgz
tar -xzf flink-1.20.1-bin-scala_2.12.tgz -C /opt/flink --strip-components=1

# Copy/download all required JARs to Flink lib
cp /opt/fluss/plugins/iceberg/iceberg-aws-bundle-1.10.1.jar /opt/flink/lib/
cp /opt/fluss/plugins/iceberg/iceberg-aws-1.10.1.jar /opt/flink/lib/
cp /opt/fluss/plugins/iceberg/failsafe-3.3.2.jar /opt/flink/lib/
wget -O /opt/flink/lib/fluss-flink-1.20-0.9.1-incubating.jar \
  https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/0.9.1-incubating/fluss-flink-1.20-0.9.1-incubating.jar
wget -O /opt/flink/lib/fluss-lake-iceberg-0.9.1-incubating.jar \
  https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-iceberg/0.9.1-incubating/fluss-lake-iceberg-0.9.1-incubating.jar
wget -O /opt/flink/lib/fluss-flink-tiering-0.9.1-incubating.jar \
  https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-tiering/0.9.1-incubating/fluss-flink-tiering-0.9.1-incubating.jar
wget -O /opt/flink/lib/hadoop-client-api-3.3.6.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar
wget -O /opt/flink/lib/hadoop-client-runtime-3.3.6.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar

# Start Flink
/opt/flink/bin/start-cluster.sh
sleep 5

# ─── 8. Create table + insert data (single Flink SQL session) ───
cat > /tmp/setup.sql <<EOF
CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'localhost:9123');
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS my_database;
USE my_database;
CREATE TABLE test_table (id BIGINT, name STRING, PRIMARY KEY (id) NOT ENFORCED)
  WITH ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');
INSERT INTO test_table VALUES (1, 'hello'), (2, 'world');
EOF
/opt/flink/bin/sql-client.sh -f /tmp/setup.sql

# ─── 9. Start tiering service ───
/opt/flink/bin/flink run -d /opt/flink/lib/fluss-flink-tiering-0.9.1-incubating.jar \
  --fluss.bootstrap.servers localhost:9123 \
  --datalake.format iceberg \
  --datalake.iceberg.type glue \
  --datalake.iceberg.warehouse s3://YOUR-BUCKET/iceberg-warehouse \
  --datalake.iceberg.client.region YOUR-REGION

# ─── 10. Query in Athena ───
# Wait ~30s for tiering to flush, then:
#   SELECT * FROM my_database.test_table;
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
