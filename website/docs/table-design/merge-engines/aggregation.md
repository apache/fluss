---
sidebar_label: Aggregation
title: Aggregation Merge Engine
sidebar_position: 5
---

# Aggregation Merge Engine

## Overview

The **Aggregation Merge Engine** is designed for scenarios where users only care about aggregated results rather than individual records. It aggregates each value field with the latest data one by one under the same primary key according to the specified aggregate function.

Each field not part of the primary keys can be assigned an aggregate function. The recommended way depends on the client you are working with:
- For **Flink SQL** or **Spark SQL**, use DDL and connector options (`'fields.<field-name>.agg'`)
- For **Java clients**, use the Schema API

If no function is specified for a field, it will use `last_value_ignore_nulls` aggregation as the default behavior.

This merge engine is useful for real-time aggregation scenarios such as:
- Computing running totals and statistics
- Maintaining counters and metrics
- Tracking maximum/minimum values over time
- Building real-time dashboards and analytics

## Configuration

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

To enable the aggregation merge engine, set the following table property:

```sql
CREATE TABLE product_stats (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT,
    last_update_time TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.price.agg' = 'max',
    'fields.sales.agg' = 'sum'
    -- last_update_time defaults to 'last_value_ignore_nulls'
);
```

Specify the aggregate function for each non-primary key field using connector options:

```sql
'fields.<field-name>.agg' = '<function-name>'
```

For functions that require parameters (e.g., `listagg` with custom delimiter):

```sql
'fields.<field-name>.agg' = '<function-name>',
'fields.<field-name>.<function-name>.<param-name>' = '<param-value>'
```

</TabItem>
<TabItem value="java-client" label="Java Client">

To enable the aggregation merge engine, set the following table property:

```java
TableDescriptor tableDescriptor = TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();
```

Specify the aggregate function for each non-primary key field using the Schema API:

```java
Schema schema = Schema.newBuilder()
    .column("product_id", DataTypes.BIGINT())
    .column("price", DataTypes.DOUBLE(), AggFunctions.MAX())
    .column("sales", DataTypes.BIGINT(), AggFunctions.SUM())
    .column("last_update_time", DataTypes.TIMESTAMP(3))  // Defaults to LAST_VALUE_IGNORE_NULLS
    .primaryKey("product_id")
    .build();
```

</TabItem>
</Tabs>

## Usage Examples

<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

### Creating a Table with Aggregation

```sql
CREATE TABLE product_stats (
    product_id BIGINT,
    price DOUBLE,
    sales BIGINT,
    last_update_time TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.price.agg' = 'max',
    'fields.sales.agg' = 'sum'
    -- last_update_time defaults to 'last_value_ignore_nulls'
);
```

### Writing Data

```sql
-- Insert data - these will be aggregated
INSERT INTO product_stats VALUES
    (1, 23.0, 15, TIMESTAMP '2024-01-01 10:00:00'),
    (1, 30.2, 20, TIMESTAMP '2024-01-01 11:00:00');  -- Same primary key - triggers aggregation
```

### Querying Results

```sql
SELECT * FROM product_stats;
```

**Result after aggregation:**
```
+------------+-------+-------+---------------------+
| product_id | price | sales | last_update_time     |
+------------+-------+-------+---------------------+
|          1 |  30.2 |    35 | 2024-01-01 11:00:00 |
+------------+-------+-------+---------------------+
```

- `product_id`: 1
- `price`: 30.2 (max of 23.0 and 30.2)
- `sales`: 35 (sum of 15 and 20)
- `last_update_time`: 2024-01-01 11:00:00 (last non-null value)

</TabItem>
<TabItem value="java-client" label="Java Client">

### Creating a Table with Aggregation

```java
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.metadata.AggFunction;

// Create connection
Connection conn = Connection.create(config);
Admin admin = conn.getAdmin();

// Define schema with aggregation functions
Schema schema = Schema.newBuilder()
    .column("product_id", DataTypes.BIGINT())
    .column("price", DataTypes.DOUBLE(), AggFunctions.MAX())
    .column("sales", DataTypes.BIGINT(), AggFunctions.SUM())
    .column("last_update_time", DataTypes.TIMESTAMP(3))  // Defaults to LAST_VALUE_IGNORE_NULLS
    .primaryKey("product_id")
    .build();

// Create table with aggregation merge engine
TableDescriptor tableDescriptor = TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

TablePath tablePath = TablePath.of("my_database", "product_stats");
admin.createTable(tablePath, tableDescriptor, false).get();
```

### Writing Data

```java
// Get table
Table table = conn.getTable(tablePath);

// Create upsert writer
UpsertWriter writer = table.newUpsert().createWriter();

// Write data - these will be aggregated
writer.upsert(row(1L, 23.0, 15L, timestamp1));
writer.upsert(row(1L, 30.2, 20L, timestamp2)); // Same primary key - triggers aggregation

writer.flush();
```

**Result after aggregation:**
- `product_id`: 1
- `price`: 30.2 (max of 23.0 and 30.2)
- `sales`: 35 (sum of 15 and 20)
- `last_update_time`: timestamp2 (last non-null value)

</TabItem>
</Tabs>

## Supported Aggregate Functions

Fluss currently supports the following aggregate functions:

### sum

Aggregates values by computing the sum across multiple rows.

- **Supported Data Types**: `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`
- **Behavior**: Adds incoming values to the accumulator
- **Null Handling**: Null values are ignored

**Example:**

<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_sum (
    id BIGINT,
    amount DECIMAL(10, 2),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.amount.agg' = 'sum'
);

INSERT INTO test_sum VALUES
    (1, 100.50),
    (1, 200.75);

SELECT * FROM test_sum;
+------------+---------+
| id         | amount  |
+------------+---------+
|          1 | 301.25  |
+------------+---------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("amount", DataTypes.DECIMAL(10, 2), AggFunctions.SUM())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 100.50), (1, 200.75)
// Result: (1, 301.25)
```
</TabItem>
</Tabs>

### product

Computes the product of values across multiple rows.

- **Supported Data Types**: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL
- **Behavior**: Multiplies incoming values with the accumulator
- **Null Handling**: Null values are ignored

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_product (
    id BIGINT,
    discount_factor DOUBLE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.discount_factor.agg' = 'product'
);

INSERT INTO test_product VALUES
    (1, 0.9),
    (1, 0.8);

SELECT * FROM test_product;
+------------+---------+
| id         | amount  |
+------------+---------+
|          1 | 0.72    |
+------------+---------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("discount_factor", DataTypes.DOUBLE(), AggFunctions.PRODUCT())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 0.9), (1, 0.8)
// Result: (1, 0.72) -- 90% * 80% = 72%
```

</TabItem>
</Tabs>

### max

Identifies and retains the maximum value.

- **Supported Data Types**: CHAR, STRING, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ
- **Behavior**: Keeps the larger value between accumulator and incoming value
- **Null Handling**: Null values are ignored

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_max (
    id BIGINT,
    temperature DOUBLE,
    reading_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.temperature.agg' = 'max',
    'fields.reading_time.agg' = 'max'
);

INSERT INTO test_max VALUES
    (1, 25.5, TIMESTAMP '2024-01-01 10:00:00'),
    (1, 28.3, TIMESTAMP '2024-01-01 11:00:00');

SELECT * FROM test_max;
+------------+----------------+---------------------+
| id         | temperature    | reading_time        |
+------------+----------------+---------------------+
|          1 | 28.3           | 2024-01-01 11:00:00 |
+------------+----------------+---------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("max_temperature", DataTypes.DOUBLE(), AggFunctions.MAX())
    .column("max_reading_time", DataTypes.TIMESTAMP(3), AggFunctions.MAX())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 25.5, '2024-01-01 10:00:00'), (1, 28.3, '2024-01-01 11:00:00')
// Result: (1, 28.3, '2024-01-01 11:00:00')
```
</TabItem>
</Tabs>

### min

Identifies and retains the minimum value.

- **Supported Data Types**: CHAR, STRING, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ
- **Behavior**: Keeps the smaller value between accumulator and incoming value
- **Null Handling**: Null values are ignored

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_min  (
    id BIGINT,
    lowest_price DECIMAL(10, 2),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.lowest_price.agg' = 'min'
);

INSERT INTO test_min VALUES
    (1, 99.99),
    (1, 79.99),
    (1, 89.99);

SELECT * FROM test_min;
+------------+--------------+
| id         | lowest_price |
+------------+--------------+
|          1 | 79.99        |
+------------+--------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("lowest_price", DataTypes.DECIMAL(10, 2), AggFunctions.MIN())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 99.99), (1, 79.99), (1, 89.99)
// Result: (1, 79.99)
```

</TabItem>
</Tabs>

### last_value

Replaces the previous value with the most recently received value.

- **Supported Data Types**: All data types
- **Behavior**: Always uses the latest incoming value
- **Null Handling**: Null values will overwrite previous values

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_last_value  (
    id BIGINT,
    status STRING,
    last_login TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.status.agg' = 'last_value',
    'fields.last_login.agg' = 'last_value'
);


INSERT INTO test_last_value VALUES
    (1, 'online', TIMESTAMP '2024-01-01 10:00:00'),
    (1, 'offline', TIMESTAMP '2024-01-01 11:00:00'),
    (1, null, TIMESTAMP '2024-01-01 12:00:00');  -- Null overwrites previous 'offline' value

SELECT * FROM test_last_value;
+------------+---------+---------------------+
| id         | status  | last_login          |
+------------+---------+---------------------+
|          1 | NULL    | 2024-01-01 12:00:00 |
+------------+---------+---------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("status", DataTypes.STRING(), AggFunctions.LAST_VALUE())
    .column("last_login", DataTypes.TIMESTAMP(3), AggFunctions.LAST_VALUE())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Step 1: Insert initial values
// Input:  (1, 'online', '2024-01-01 10:00:00')
// Result: (1, 'online', '2024-01-01 10:00:00')

// Step 2: Upsert with new values
// Input:  (1, 'offline', '2024-01-01 11:00:00')
// Result: (1, 'offline', '2024-01-01 11:00:00')

// Step 3: Upsert with null status - null overwrites the previous 'offline' value
// Input:  (1, null, '2024-01-01 12:00:00')
// Result: (1, null, '2024-01-01 12:00:00')
// Note: status becomes null (null overwrites previous value), last_login updated
```
</TabItem>
</Tabs>

**Key behavior:** Null values overwrite existing values, treating null as a valid value to be stored.

### last_value_ignore_nulls

Replaces the previous value with the latest non-null value. This is the **default aggregate function** when no function is specified.

- **Supported Data Types**: All data types
- **Behavior**: Uses the latest incoming value only if it's not null
- **Null Handling**: Null values are ignored, previous value is retained

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_last_value_ignore_nulls  (
    id BIGINT,
    email STRING,
    phone STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.email.agg' = 'last_value_ignore_nulls',
    'fields.phone.agg' = 'last_value_ignore_nulls'
);


INSERT INTO test_last_value_ignore_nulls VALUES
    (1, 'user@example.com', '123-456'),
    (1, null, '789-012'),  -- Null is ignored, email retains previous value
    (1, 'new@example.com', null);

SELECT * FROM test_last_value_ignore_nulls;
+------------+-------------------+---------+
| id         | email             | phone   |
+------------+-------------------+---------+
|          1 | new@example.com   | 789-012 |
+------------+-------------------+---------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("email", DataTypes.STRING(), AggFunctions.LAST_VALUE_IGNORE_NULLS())
    .column("phone", DataTypes.STRING(), AggFunctions.LAST_VALUE_IGNORE_NULLS())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Step 1: Insert initial values
// Input:  (1, 'user@example.com', '123-456')
// Result: (1, 'user@example.com', '123-456')

// Step 2: Upsert with null email - null is ignored, email retains previous value
// Input:  (1, null, '789-012')
// Result: (1, 'user@example.com', '789-012')
// Note: email remains 'user@example.com' (null was ignored), phone updated to '789-012'

// Step 3: Upsert with null phone - null is ignored, phone retains previous value
// Input:  (1, 'new@example.com', null)
// Result: (1, 'new@example.com', '789-012')
// Note: email updated to 'new@example.com', phone remains '789-012' (null was ignored)
```

</TabItem>
</Tabs>

**Key behavior:** Null values do not overwrite existing non-null values, making this function ideal for maintaining the most recent valid data.

### first_value

Retrieves and retains the first value seen for a field.

- **Supported Data Types**: All data types
- **Behavior**: Keeps the first received value, ignores all subsequent values
- **Null Handling**: Null values are retained if received first

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_first_value  (
    id BIGINT,
    first_purchase_date DATE,
    first_product STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.first_purchase_date.agg' = 'first_value',
    'fields.first_product.agg' = 'first_value'
);

INSERT INTO test_first_value VALUES
    (1, '2024-01-01', 'ProductA'),
    (1, '2024-02-01', 'ProductB');  -- Ignored, first value retained

SELECT * FROM test_first_value;
+------------+---------------------+---------------+
| id         | first_purchase_date | first_product |
+------------+---------------------+---------------+
|          1 | 2024-01-01          | ProductA      |
+------------+---------------------+---------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">


```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("first_purchase_date", DataTypes.DATE(), AggFunctions.FIRST_VALUE())
    .column("first_product", DataTypes.STRING(), AggFunctions.FIRST_VALUE())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, '2024-01-01', 'ProductA'), (1, '2024-02-01', 'ProductB')
// Result: (1, '2024-01-01', 'ProductA')
```

</TabItem>
</Tabs>

### first_value_ignore_nulls

Selects the first non-null value in a data set.

- **Supported Data Types**: All data types
- **Behavior**: Keeps the first received non-null value, ignores all subsequent values
- **Null Handling**: Null values are ignored until a non-null value is received

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_first_value_ignore_nulls  (
    id BIGINT,
    email STRING,
    verified_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.email.agg' = 'first_value_ignore_nulls',
    'fields.verified_at.agg' = 'first_value_ignore_nulls'
);

INSERT INTO test_first_value_ignore_nulls VALUES
    (1, null, null),
    (1, 'user@example.com', '2024-01-01 10:00:00'),
    (1, 'other@example.com', '2024-01-02 10:00:00'); -- Only the first non-null value is retained

SELECT * FROM test_first_value_ignore_nulls;
+------------+-------------------+---------------------+
| id         | email             | verified_at         |
+------------+-------------------+---------------------+
|          1 | user@example.com  | 2024-01-01 10:00:00 |
+------------+-------------------+---------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("email", DataTypes.STRING(), AggFunctions.FIRST_VALUE_IGNORE_NULLS())
    .column("verified_at", DataTypes.TIMESTAMP(3), AggFunctions.FIRST_VALUE_IGNORE_NULLS())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, null, null), (1, 'user@example.com', '2024-01-01 10:00:00'), (1, 'other@example.com', '2024-01-02 10:00:00')
// Result: (1, 'user@example.com', '2024-01-01 10:00:00')
```

</TabItem>
</Tabs>

### listagg

Concatenates multiple string values into a single string with a delimiter.

- **Supported Data Types**: STRING, CHAR
- **Behavior**: Concatenates values using the specified delimiter
- **Null Handling**: Null values are skipped
- **Delimiter**: Specify delimiter directly in the aggregation function (default is comma `,`)

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_listagg  (
    id BIGINT,
    tags1 STRING,
    tags2 STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.tags1.agg' = 'listagg',
    'fields.tags2.agg' = 'listagg',
    'fields.tags2.listagg.delimiter' = ';'   -- Specify delimiter as parameter
);

INSERT INTO test_listagg VALUES
    (1, 'developer', 'developer'),
    (1, 'java', 'java'),
    (1, 'flink', 'flink');

SELECT * FROM test_listagg;
+------------+-----------------------+-----------------------+
| id         | tags1                 | tags2                 |
+------------+-----------------------+-----------------------+
|          1 | developer,java,flink  | developer;java;flink  |
+------------+-----------------------+-----------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("tags1", DataTypes.STRING(), AggFunctions.LISTAGG())
    .column("tags2", DataTypes.STRING(), AggFunctions.LISTAGG(";"))  // Specify delimiter inline
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 'developer', 'developer'), (1, 'java', 'java'), (1, 'flink', 'flink')
// Result: (1, 'developer,java,flink', 'developer;java;flink')
```

</TabItem>
</Tabs>

### string_agg

Alias for `listagg`. Concatenates multiple string values into a single string with a delimiter.

- **Supported Data Types**: STRING, CHAR
- **Behavior**: Same as `listagg` - concatenates values using the specified delimiter
- **Null Handling**: Null values are skipped
- **Delimiter**: Specify delimiter directly in the aggregation function (default is comma `,`)

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_string_agg  (
    id BIGINT,
    tags1 STRING,
    tags2 STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.tags1.agg' = 'string_agg',
    'fields.tags2.agg' = 'string_agg',
    'fields.tags2.string_agg.delimiter' = ';'   -- Specify delimiter as parameter
);

INSERT INTO test_string_agg VALUES
    (1, 'developer', 'developer'),
    (1, 'java', 'java'),
    (1, 'flink', 'flink');

SELECT * FROM test_string_agg;
+------------+-----------------------+-----------------------+
| id         | tags1                 | tags2                 |
+------------+-----------------------+-----------------------+
|          1 | developer,java,flink  | developer;java;flink  |
+------------+-----------------------+-----------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("tags", DataTypes.STRING(), AggFunctions.STRING_AGG(";"))  // Specify delimiter inline
    .primaryKey("id")
    .build();
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("tags1", DataTypes.STRING(), AggFunctions.STRING_AGG())
    .column("tags2", DataTypes.STRING(), AggFunctions.STRING_AGG(";"))  // Specify delimiter inline
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 'developer', 'developer'), (1, 'java', 'java'), (1, 'flink', 'flink')
// Result: (1, 'developer,java,flink', 'developer;java;flink')
```

</TabItem>
</Tabs>

### rbm32

Aggregates serialized 32-bit RoaringBitmap values by union.

- **Supported Data Types**: BYTES
- **Behavior**: ORs incoming bitmaps with the accumulator
- **Null Handling**: Null values are ignored

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE user_visits (
    user_id BIGINT,
    visit_bitmap BYTES,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.visit_bitmap.agg' = 'rbm32'
);
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("user_id", DataTypes.BIGINT())
    .column("visit_bitmap", DataTypes.BYTES(), AggFunctions.RBM32())
    .primaryKey("user_id")
    .build();
```

</TabItem>
</Tabs>

### rbm64

Aggregates serialized 64-bit RoaringBitmap values by union.

- **Supported Data Types**: BYTES
- **Behavior**: ORs incoming bitmaps with the accumulator
- **Null Handling**: Null values are ignored

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE session_interactions (
    session_id BIGINT,
    interaction_bitmap BYTES,
    PRIMARY KEY (session_id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.interaction_bitmap.agg' = 'rbm64'
);
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("session_id", DataTypes.BIGINT())
    .column("interaction_bitmap", DataTypes.BYTES(), AggFunctions.RBM64())
    .primaryKey("session_id")
    .build();
```

</TabItem>
</Tabs>

### bool_and

Evaluates whether all boolean values in a set are true (logical AND).

- **Supported Data Types**: BOOLEAN
- **Behavior**: Returns true only if all values are true
- **Null Handling**: Null values are ignored

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_bool_and  (
    id BIGINT,
    has_all_permissions BOOLEAN,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.has_all_permissions.agg' = 'bool_and'
);

INSERT INTO test_bool_and VALUES
    (1, true),
    (1, true),
    (1, false);

SELECT * FROM test_bool_and;
+------------+----------------------+
| id         | has_all_permissions  |
+------------+----------------------+
|          1 | false                |
+------------+----------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("has_all_permissions", DataTypes.BOOLEAN(), AggFunctions.BOOL_AND())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, true), (1, true), (1, false)
// Result: (1, false) -- Not all values are true
```

</TabItem>
</Tabs>

### bool_or

Checks if at least one boolean value in a set is true (logical OR).

- **Supported Data Types**: BOOLEAN
- **Behavior**: Returns true if any value is true
- **Null Handling**: Null values are ignored

**Example:**
<Tabs>
<TabItem value="flink-sql" label="Flink SQL" default>

```sql
CREATE TABLE test_bool_or  (
    id BIGINT,
    has_any_alert BOOLEAN,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'fields.has_any_alert.agg' = 'bool_or'
);

INSERT INTO test_bool_or VALUES
    (1, false),
    (1, false),
    (1, true);

SELECT * FROM test_bool_or;
+------------+------------------+
| id         | has_any_alert    |
+------------+------------------+
|          1 | true             |
+------------+------------------+
```

</TabItem>
<TabItem value="java-client" label="Java Client">

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("has_any_alert", DataTypes.BOOLEAN(), AggFunctions.BOOL_OR())
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, false), (1, false), (1, true)
// Result: (1, true) -- At least one value is true
```

</TabItem>
</Tabs>

## Delete Behavior

The aggregation merge engine provides limited support for delete operations. You can configure the behavior using the `'table.delete.behavior'` option:

```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.delete.behavior", "allow")  // Enable delete operations
    .build();
```

**Configuration options**:
- **`'table.delete.behavior' = 'ignore'`** (default): Delete operations will be silently ignored without error
- **`'table.delete.behavior' = 'disable'`**: Delete operations will be rejected with a clear error message
- **`'table.delete.behavior' = 'allow'`**: Delete operations will remove records based on the update mode (see details below)

### Delete Behavior with Different Update Modes

When `'table.delete.behavior' = 'allow'`, the actual delete behavior depends on whether you are using **full update** or **partial update**:

**Full Update (Default Write Mode)**:
- Delete operations remove the **entire record** from the table
- All aggregated values for that primary key are permanently lost

**Example**:
```java
// Full update mode (default)
UpsertWriter writer = table.newUpsert().createWriter();
writer.delete(primaryKeyRow);  // Removes the entire record
```

**Partial Update Mode**:
- Delete operations perform a **partial delete** on target columns only
- **Target columns** (except primary key): Set to null
- **Non-target columns**: Remain unchanged
- **Special case**: If all non-target columns are null after the delete, the entire record is removed

**Example**:
```java
// Partial update mode - only targeting specific columns
UpsertWriter partialWriter = table.newUpsert()
    .partialUpdate("id", "count1", "sum1")  // Target columns
    .createWriter();

// Delete will:
// - Set count1 and sum1 to null
// - Keep count2 and sum2 unchanged (non-target columns)
// - Remove entire record only if count2 and sum2 are both null
partialWriter.delete(primaryKeyRow);
```

:::note
**Current Limitation**: The aggregation merge engine does not support retraction semantics (e.g., subtracting from a sum, reverting a max). 

- **Full update mode**: Delete operations can only remove the entire record
- **Partial update mode**: Delete operations can only null out target columns, not retract aggregated values

Future versions may support fine-grained retraction by enhancing the protocol to carry row data with delete operations.
:::

## Limitations

:::warning Critical Limitations
When using the `aggregation` merge engine, be aware of the following critical limitations:

### Exactly-Once Semantics

When writing to an aggregate merge engine table using the Flink engine, Fluss does provide exactly-once guarantees. Thanks to Flink's checkpointing mechanism, in the event of a failure and recovery, the Flink connector automatically performs an undo operation to roll back the table state to what it was at the last successful checkpoint. This ensures no over-counting or under-counting: data remains consistent and accurate.

However, when using the Fluss client API directly (outside of Flink), exactly-once is not provided out of the box. In such cases, users must implement their own recovery logic (similar to what the Flink connector does) by explicitly resetting the table state to a previous version by performing undo operations.

For detailed information about Exactly-Once implementation, please refer to: [FIP-21: Aggregation Merge Engine](https://cwiki.apache.org/confluence/display/FLUSS/FIP-21%3A+Aggregation+Merge+Engine)

:::

## See Also

- [Default Merge Engine](table-design/merge-engines/default.md)
- [FirstRow Merge Engine](table-design/merge-engines/first-row.md)
- [Versioned Merge Engine](table-design/merge-engines/versioned.md)
- [Primary Key Tables](table-design/table-types/pk-table.md)
- [Fluss Client API](apis/java-client.md)
