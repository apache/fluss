---
title: Lance
sidebar_position: 3
---

# Lance

## Introduction

[Lance](https://lancedb.github.io/lance/) is a modern table format optimized for machine learning and AI applications. 
To integrate Fluss with Lance, you must enable lakehouse storage and configure Lance as the lakehouse storage. For more details, see [Deploying Streaming Lakehouse](../../install-deploy/deploying-streaming-lakehouse.md).

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
Then, you must start the datalake tiering service to tier Fluss's data to Lance. For guidance, you can refer to [Deploying Streaming Lakehouse](../../install-deploy/deploying-streaming-lakehouse.md). Although the example uses Paimon, the process is also applicable to Lance.

For required JARs, you should follow this guidance:
- Put [fluss-flink connector jar](/downloads) into `${FLINK_HOME}/lib`, you should choose a connector version matching your Flink version. If you're using Flink 1.20, please use [fluss-flink-1.20-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar)
- If you are using [Amazon S3](http://aws.amazon.com/s3/), [Aliyun OSS](https://www.aliyun.com/product/oss) or [HDFS(Hadoop Distributed File System)](https://hadoop.apache.org/docs/stable/) as Fluss's [remote storage](../../maintenance/tiered-storage/remote-storage.md),
  you should download the corresponding [Fluss filesystem jar](/downloads#filesystem-jars) and also put it into `${FLINK_HOME}/lib`
- Put [fluss-lake-lance jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-lance/$FLUSS_VERSION$/fluss-lake-lance-$FLUSS_VERSION$.jar) into `${FLINK_HOME}/lib`

Make sure to use Lance-specific configurations as parameters when starting the Flink tiering job:
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

> **NOTE**: The Lance connector leverages Arrow Java library, which operates on off-heap memory. To prevent `java.lang.OutOfMemoryError: Direct buffer memory` error in Flink Task Manager, please increase the value of `taskmanager.memory.task.off-heap.size` in `<FLINK_HOME>/conf/config.yaml` to at least `'512m'` (e.g., `taskmanager.memory.task.off-heap.size: 512m`). You may need to adjust this value higher (such as `'1g'`) depending on your workload and data size.

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

| Fluss Data Type               | Lance Data Type           |
|-------------------------------|---------------------------|
| BOOLEAN                       | Bool                      |
| TINYINT                       | Int8                      |
| SMALLINT                      | Int16                     |
| INT                           | Int32                     |
| BIGINT                        | Int64                     |
| FLOAT                         | Float32                   |
| DOUBLE                        | Float64                   |
| DECIMAL                       | Decimal128                |
| STRING                        | Utf8                      |
| CHAR                          | Utf8                      |
| DATE                          | Date                      |
| TIME                          | Time                      |
| TIMESTAMP                     | Timestamp                 |
| TIMESTAMP WITH LOCAL TIMEZONE | Timestamp                 |
| BINARY                        | FixedSizeBinary           |
| BYTES                         | Binary                    |
| ARRAY\<t\>                    | List (or FixedSizeList)   |

## Array Type Support

Fluss supports the `ARRAY<t>` data type, which is particularly useful for machine learning and AI applications, such as storing vector embeddings.
When tiering data to Lance, Fluss automatically converts `ARRAY` columns to Lance's List type (or FixedSizeList for fixed-dimension arrays).

### Use Cases for Array Type

The array type is especially valuable for:
- **Vector Embeddings**: Store embeddings generated by machine learning models (e.g., text embeddings, image embeddings)
- **Multi-dimensional Features**: Store feature vectors for recommendation systems or analytics
- **Time Series Data**: Store sequences of numerical data points
- **Batch Processing**: Store collections of related values together

### Creating Tables with Array Columns

You can create a Fluss table with array columns that will be tiered to Lance:

```sql title="Flink SQL"
CREATE TABLE product_embeddings (
    product_id BIGINT,
    product_name STRING,
    embedding ARRAY<FLOAT>,
    tags ARRAY<STRING>,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

### Vector Embedding Example

Here's a complete example of using Lance with Fluss for vector embeddings in a recommendation system:

```sql title="Flink SQL"
USE CATALOG fluss_catalog;

-- Create a table to store product embeddings
CREATE TABLE product_vectors (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    embedding ARRAY<FLOAT>,
    created_time TIMESTAMP
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '1min',
    'lance.max_row_per_file' = '1024'
);

-- Insert sample data with embeddings
INSERT INTO product_vectors VALUES
(1, 'Laptop', 'Electronics', ARRAY[0.23, 0.45, 0.67, 0.12, 0.89], CURRENT_TIMESTAMP),
(2, 'Phone', 'Electronics', ARRAY[0.21, 0.43, 0.65, 0.15, 0.87], CURRENT_TIMESTAMP),
(3, 'Book', 'Media', ARRAY[0.78, 0.32, 0.11, 0.54, 0.23], CURRENT_TIMESTAMP);
```

### Reading Array Data from Lance

Once data is tiered to Lance, you can use Lance's vector search capabilities to perform similarity searches:

```python title="Lance Python with Vector Search"
import lance
import numpy as np

# Connect to the Lance dataset
ds = lance.dataset("s3://my-bucket/my_database/product_vectors.lance")

# Read all data
df = ds.to_table().to_pandas()
print(df)

# Perform vector similarity search
# Query vector (e.g., embedding of a search query)
query_vector = np.array([0.22, 0.44, 0.66, 0.13, 0.88])

# Search for similar products using Lance's vector index
results = ds.to_table(
    nearest={
        "column": "embedding",
        "q": query_vector,
        "k": 5
    }
).to_pandas()

print("Top 5 similar products:")
print(results[['product_id', 'product_name', 'category']])
```

### Advanced Usage with Fixed-Size Arrays

For optimal performance with vector embeddings, you can use fixed-size arrays:

```sql title="Flink SQL"
CREATE TABLE image_embeddings (
    image_id BIGINT,
    image_url STRING,
    embedding ARRAY<FLOAT>,
    metadata ROW<width INT, height INT, format STRING>
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

### Best Practices

When working with array columns in Lance:

1. **Fixed Dimensions**: Keep embedding dimensions consistent across all rows for better performance
2. **Data Type**: Use `FLOAT` or `DOUBLE` for numerical embeddings, depending on precision requirements
3. **Batch Size**: Adjust `lance.max_row_per_file` based on your embedding size and query patterns
4. **Freshness**: Set `table.datalake.freshness` appropriately for your use case (balance between latency and write efficiency)

### Performance Considerations

- Lance stores arrays efficiently using Arrow's columnar format
- Fixed-size arrays (when all elements have the same length) can be stored more efficiently
- Lance supports vector indexes for fast similarity search on array columns
- For large embeddings (e.g., 1024+ dimensions), consider increasing Flink Task Manager's off-heap memory