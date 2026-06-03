---
title: AWS Glue
sidebar_position: 2
---

# AWS Glue

## Introduction

[AWS Glue](https://aws.amazon.com/glue/) is a serverless data integration service that makes it easy to discover, prepare, and combine data for analytics, machine learning, and application development. AWS Glue includes a central metadata repository, known as the AWS Glue Data Catalog, which is fully compatible with Apache Iceberg.

This guide explains how to configure Fluss to use AWS Glue as its Iceberg catalog. For general Iceberg integration details (table mapping, data types, limitations), see [Iceberg](../formats/iceberg.md).

## How It Works

When Fluss is configured with AWS Glue as its Iceberg catalog:

1. Fluss creates and manages Iceberg database and table metadata directly within the AWS Glue Data Catalog.
2. The [tiering service](maintenance/tiered-storage/lakehouse-storage.md#start-the-datalake-tiering-service) writes data files to Amazon S3 and commits snapshots to the Glue Data Catalog.
3. Any AWS native or external query engine (such as Amazon Athena, Amazon EMR, AWS Glue Jobs, Snowflake, Trino, Flink, or Spark) can discover and query the tiered tables through AWS Glue.

## Prerequisites

### AWS IAM Permissions

Fluss and the tiering service require appropriate IAM permissions to interact with AWS Glue and S3. Below is a minimal IAM policy template:

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
        "glue:UpdateDatabase",
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

You must place the required Iceberg AWS integration JARs into the classpath of both Fluss and the Flink tiering service.

#### For Fluss Servers (Coordinator & Tablet Servers)

Place the following JARs in the `FLUSS_HOME/plugins/iceberg/` directory:

1. **Iceberg AWS Integration**: [iceberg-aws-1.10.1.jar](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.10.1/iceberg-aws-1.10.1.jar)
2. **AWS SDK Bundle**: [iceberg-aws-bundle-1.10.1.jar](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar)
3. **Failsafe**: [failsafe-3.3.2.jar](https://repo1.maven.org/maven2/dev/failsafe/failsafe/3.3.2/failsafe-3.3.2.jar)

#### For the Flink Tiering Service

Place the same three JARs into the `${FLINK_HOME}/lib` directory.

## Configure Fluss with AWS Glue

### Cluster Configuration

Add the following configuration parameters to your `server.yaml`:

```yaml
datalake.format: iceberg
datalake.iceberg.type: glue
datalake.iceberg.warehouse: s3://<your-bucket>/<warehouse-path>
datalake.iceberg.client.region: <your-aws-region>
datalake.iceberg.io-impl: org.apache.iceberg.aws.s3.S3FileIO
```

:::tip
Fluss strips the `datalake.iceberg.` prefix and passes the remaining properties to the Iceberg Glue catalog client. You can configure any additional [Iceberg AWS integration properties](https://iceberg.apache.org/docs/1.10.1/aws/) (such as S3 endpoint overrides or credentials provider configurations) using this prefix.
:::

#### Authentication Methods

**1. IAM Role / Default Credentials Provider Chain (Recommended)**

If Fluss and Flink are running in an AWS environment (e.g., EKS, ECS, or EC2) with attached IAM roles, you do not need to configure credentials in `server.yaml`. The catalog will automatically resolve credentials using the default provider chain.

**2. Static Credentials (Not Recommended for Production)**

If you need to supply static credentials for testing, add them to `server.yaml`:

```yaml
datalake.iceberg.s3.access-key-id: <your-access-key-id>
datalake.iceberg.s3.secret-access-key: <your-secret-access-key>
```

### Start Tiering Service

Follow the [Iceberg tiering service setup](../formats/iceberg.md#start-tiering-service-to-iceberg) instructions to start the tiering service. Provide the Glue catalog parameters when launching the Flink tiering job:

```bash
${FLINK_HOME}/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers <coordinator-host>:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type glue \
    --datalake.iceberg.warehouse s3://<your-bucket>/<warehouse-path> \
    --datalake.iceberg.client.region <your-aws-region> \
    --datalake.iceberg.io-impl org.apache.iceberg.aws.s3.S3FileIO
```

## Usage Example

### Create a Datalake-Enabled Table

Connect to Fluss via Flink SQL and create a table with data lake enabled:

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

Fluss will automatically provision the database (if it does not exist) and create the corresponding Iceberg table within the AWS Glue Data Catalog. Once data is ingested and the tiering service runs, the parquet files are stored in S3.

### Query Data with Athena

Because the table metadata is stored in the Glue Data Catalog, you can query your tiered data directly in AWS Athena:

```sql title="Athena SQL"
SELECT * FROM fluss_database.customer_orders;
```

## Further Reading

- [Iceberg Integration](../formats/iceberg.md) - Table mapping, data types, and configurations.
- [Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md) - General tiered storage overview.
- [Iceberg AWS Integration Docs](https://iceberg.apache.org/docs/latest/aws/) - Detailed properties for Glue and S3 configs.
