---
sidebar_label: Writes
title: Flink Writes
sidebar_position: 3
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Flink Writes

You can directly insert or update data into a Fluss table using the `INSERT INTO` statement.
Fluss primary key tables can accept all types of messages (`INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE`), while Fluss log table can only accept `INSERT` type messages.


## INSERT INTO
`INSERT INTO` statements are used to write data to Fluss tables. 
They support both streaming and batch modes and are compatible with primary-key tables (for upserting data) as well as log tables (for appending data).

### Appending Data to the Log Table
#### Create a Log table.
```sql title="Flink SQL"
CREATE TABLE log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
);
```

#### Insert data into the Log table.
```sql title="Flink SQL"
CREATE TEMPORARY TABLE source (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
) WITH ('connector' = 'datagen');
```

```sql title="Flink SQL"
INSERT INTO log_table
SELECT * FROM source;
```


### Perform Data Upserts to the PrimaryKey Table.

#### Create a primary key table.
```sql title="Flink SQL"
CREATE TABLE pk_table (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT,
  PRIMARY KEY (shop_id, user_id) NOT ENFORCED
);
```

#### Updates All Columns
```sql title="Flink SQL"
CREATE TEMPORARY TABLE source (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT
) WITH ('connector' = 'datagen');
```

```sql title="Flink SQL"
INSERT INTO pk_table
SELECT * FROM source;
```


#### Partial Updates

```sql title="Flink SQL"
CREATE TEMPORARY TABLE source (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT
) WITH ('connector' = 'datagen');
```

```sql title="Flink SQL"
-- only partial-update the num_orders column
INSERT INTO pk_table (shop_id, user_id, num_orders)
SELECT shop_id, user_id, num_orders FROM source;
```

## DELETE FROM

Fluss supports deleting data for primary-key tables in batch mode via `DELETE FROM` statement. Currently, only single data deletions based on the primary key are supported.

* the primary key table
```sql title="Flink SQL"
-- DELETE statement requires batch mode
SET 'execution.runtime-mode' = 'batch';
```

```sql title="Flink SQL"
-- The condition must include all primary key equality conditions.
DELETE FROM pk_table WHERE shop_id = 10000 and user_id = 123456;
```

## UPDATE
Fluss enables data updates for primary-key tables in batch mode using the `UPDATE` statement. Currently, only single-row updates based on the primary key are supported.

```sql title="Flink SQL"
-- Execute the flink job in batch mode for current session context
SET execution.runtime-mode = batch;
```

```sql title="Flink SQL"
-- The condition must include all primary key equality conditions.
UPDATE pk_table SET total_amount = 2 WHERE shop_id = 10000 and user_id = 123456;
```