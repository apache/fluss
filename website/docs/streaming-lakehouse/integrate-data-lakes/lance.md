---
title: Lance
sidebar_position: 1
---

# Lance

[Lance](https://lancedb.github.io/lance/) is a modern table format optimized for machine learning and AI applications. 
To integrate Fluss with Lance, you must enable lakehouse storage and configure Lance as the lakehouse storage. For more details, see [Enable Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md#enable-lakehouse-storage).

## Introduction

To configure Lance as the lakehouse storage, you must configure the following configurations in `server.yaml`:
```yaml
# Lance configuration
datalake.format: lance

datalake.lance.warehouse: /tmp/lance

# To use S3 as Lance storage backend, you need to specify the following properties
# datalake.lance.warehouse: s3://<bucket>
# datalake.lance.endpoint: <endpoint>
# datalake.lance.allow_http: true
# datalake.lance.access_key_id: <access_key_id>
# datalake.lance.secret_access_key: <secret_access_key>
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

### Reading with Lance ecosystem tools

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
| BOOLEAN                       | BOOLEAN         |
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
| TIME                          | TIME            |
| TIMESTAMP                     | TIMESTAMP       |
| TIMESTAMP WITH LOCAL TIMEZONE | TIMESTAMP       |
| BINARY                        | FixedSizeBinary |
| BYTES                         | BINARY          |