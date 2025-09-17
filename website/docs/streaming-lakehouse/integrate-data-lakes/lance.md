---
title: Lance
sidebar_position: 3
---

# Lance

## Introduction

[Lance](https://lancedb.github.io/lance/) is a modern table format optimized for machine learning and AI applications. 
To integrate Fluss with Lance, you must enable lakehouse storage and configure Lance as the lakehouse storage. For more details, see [Enable Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md#enable-lakehouse-storage).

## Configure Lance as LakeHouse Storage

### Configure Lance in Cluster Configurations

To configure Lance as the lakehouse storage, you must configure the following configurations in `server.yaml`:
```yaml
# Lance configuration
datalake.format: lance

# Currently only local file system and object stores such as AWS S3 (and compatible stores) are supported as storage backends for Lance
# To use S3 as Lance storage backend, you need to specify the following properties
datalake.lance.warehouse: s3://<bucket>
datalake.lance.endpoint: <endpoint>
datalake.lance.allow_http: true
datalake.lance.access_key_id: <access_key_id>
datalake.lance.secret_access_key: <secret_access_key>

# Use local file system as Lance storage backend, you only need to specify the following property
# datalake.lance.warehouse: /tmp/lance
```

When a table is created or altered with the option `'table.datalake.enabled' = 'true'`, Fluss will automatically create a corresponding Lance table with path `<warehouse_path>/<database_name>/<table_name>.lance`.
The schema of the Lance table matches that of the Fluss table.

```sql title="Flink SQL"
USE CATALOG fluss_catalog;

CREATE TABLE fluss_order_with_lake (
    `order_id` BIGINT,
    `item_id` BIGINT,
    `amount` INT,
    `address` STRING
) WITH (
     'table.datalake.enabled' = 'true',
     'table.datalake.freshness' = '30s'
);
```

### Start Tiering Service to Lance
Then, you must start the datalake tiering service to tier Fluss's data to Lance. For guidance, you can refer to [Start The Datalake Tiering Service
](maintenance/tiered-storage/lakehouse-storage.md#start-the-datalake-tiering-service). Although the example uses Paimon, the process is also applicable to Lance. 

But in [Prepare required jars](maintenance/tiered-storage/lakehouse-storage.md#prepare-required-jars) step, you should follow this guidance:
- Put [fluss-flink connector jar](/downloads) into `${FLINK_HOME}/lib`, you should choose a connector version matching your Flink version. If you're using Flink 1.20, please use [fluss-flink-1.20-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar)
- If you are using [Amazon S3](http://aws.amazon.com/s3/), [Aliyun OSS](https://www.aliyun.com/product/oss) or [HDFS(Hadoop Distributed File System)](https://hadoop.apache.org/docs/stable/) as Fluss's [remote storage](maintenance/tiered-storage/remote-storage.md),
  you should download the corresponding [Fluss filesystem jar](/downloads#filesystem-jars) and also put it into `${FLINK_HOME}/lib`
- Put [fluss-lake-lance jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-lance/$FLUSS_VERSION$/fluss-lake-lance-$FLUSS_VERSION$.jar) into `${FLINK_HOME}/lib`

Additionally, when following the [Start Datalake Tiering Service](maintenance/tiered-storage/lakehouse-storage.md#start-datalake-tiering-service) guide, make sure to use Lance-specific configurations as parameters when starting the Flink tiering job:
```shell
<FLINK_HOME>/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format lance \
    --datalake.lance.warehouse s3://<bucket> \
    --datalake.lance.endpoint <endpoint> \
    --datalake.lance.allow_http true \
    --datalake.lance.secret_access_key <secret_access_key> \
    --datalake.lance.access_key_id <access_key_id>
```

> **NOTE**: Fluss v0.8 only supports tiering log tables to Lance.

Then, the datalake tiering service continuously tiers data from Fluss to Lance. The parameter `table.datalake.freshness` controls the frequency that Fluss writes data to Lance tables. By default, the data freshness is 3 minutes.

You can also specify Lance table properties when creating a datalake-enabled Fluss table by using the `lance.` prefix within the Fluss table properties clause.

```sql title="Flink SQL"
CREATE TABLE fluss_order_with_lake (
    `order_id` BIGINT,
    `item_id` BIGINT,
    `amount` INT,
    `address` STRING
 ) WITH (
     'table.datalake.enabled' = 'true',
     'table.datalake.freshness' = '30s',
     'lance.max_row_per_file' = '512'
);
```

For example, you can specify the property `max_row_per_file` to control the writing behavior when Fluss tiers data to Lance.

## Reading with Lance ecosystem tools

Since the data tiered to Lance from Fluss is stored as a standard Lance table, you can use any tool that supports Lance to read it. Below is an example using [pylance](https://pypi.org/project/pylance/):

```python title="Lance Python"
import lance
ds = lance.dataset("<warehouse_path>/<database_name>/<table_name>.lance")
```

## Data Type Mapping

Lance internally stores data in Arrow format.
When integrating with Lance, Fluss automatically converts between Fluss data types and Lance data types.  
The following table shows the mapping between [Fluss data types](table-design/data-types.md) and Lance data types:

| Fluss Data Type               | Lance Data Type |
|-------------------------------|-----------------|
| BOOLEAN                       | Bool            |
| TINYINT                       | Int8            |
| SMALLINT                      | Int16           |
| INT                           | Int32           |
| BIGINT                        | Int64           |
| FLOAT                         | Float32         |
| DOUBLE                        | Float64         |
| DECIMAL                       | Decimal128      |
| STRING                        | Utf8            |
| CHAR                          | Utf8            |
| DATE                          | Date            |
| TIME                          | Time            |
| TIMESTAMP                     | Timestamp       |
| TIMESTAMP WITH LOCAL TIMEZONE | Timestamp       |
| BINARY                        | FixedSizeBinary |
| BYTES                         | Binary          |