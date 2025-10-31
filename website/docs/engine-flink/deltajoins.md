---
sidebar_label: Delta Joins
title: Flink Delta Joins
sidebar_position: 6
---

# The Delta Join
Beginning with **Apache Flink 2.1**, a new operator called **Delta Join** was introduced.  
Compared to traditional streaming joins, the delta join operator significantly reduces the amount of state that needs to be maintained during execution. This improvement helps mitigate several common issues associated with large state sizes, including:

- Excessive computing resource and storage consumption
- Long checkpointing durations
- Extended recovery times after failures or restarts

Starting with **Apache Fluss 0.8**, streaming join jobs running on **Flink 2.1 or later** will be automatically optimized into **delta joins** whenever applicable. This optimization happens transparently at query planning time, requiring no manual configuration.

## How Delta Join Works
Traditional streaming joins in Flink require maintaining both input sides entirely in state to match records across streams. Delta join, by contrast, uses a **prefix-based lookup mechanism** to transform the behavior of querying data from the state into querying data from the Fluss source table, thereby avoiding redundant storage of the same data in both the Fluss source table and the state. This drastically reduces state size and improves performance for many streaming analytics and enrichment workloads.

## Example: Delta Join in Flink 2.1

Below is an example demonstrating a delta join query supported by Flink 2.1.

#### Create Source and Sink Tables

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

```sql title="Flink SQL"
CREATE DATABASE my_db;
```

```sql title="Flink SQL"
USE my_db;
```

#### Create Left Source Table
```sql title="Flink SQL"
CREATE TABLE `fluss_catalog`.`my_db`.`left_src` (
  `city_id` INT NOT NULL,
  `order_id` INT NOT NULL,
  `content` VARCHAR NOT NULL,
  PRIMARY KEY (city_id, order_id) NOT ENFORCED
) WITH (
    -- prefix key
    'bucket.key' = 'city_id',
    -- in Flink 2.1, delta join only support append-only source
    'table.merge-engine' = 'first_row'
);
```

#### Create Right Source Table
```sql title="Flink SQL"
CREATE TABLE `fluss_catalog`.`my_db`.`right_src` (
  `city_id` INT NOT NULL,
  `city_name` VARCHAR NOT NULL,
  PRIMARY KEY (city_id) NOT ENFORCED
) WITH (
    -- in Flink 2.1, delta join only support append-only source
    'table.merge-engine' = 'first_row'
);
```

#### Create Sink Table
```sql title="Flink SQL"
CREATE TABLE `fluss_catalog`.`my_db`.`snk` (
    `city_id` INT NOT NULL,
    `order_id` INT NOT NULL,
    `content` VARCHAR NOT NULL,
    `city_name` VARCHAR NOT NULL,
    PRIMARY KEY (city_id, order_id) NOT ENFORCED
) WITH (...);
```

#### Explain the Join Query
```sql title="Flink SQL"
EXPLAIN 
INSERT INTO `fluss_catalog`.`my_db`.`snk`
SELECT T1.`city_id`, T1.`order_id`, T1.`content`, T2.`city_name` 
FROM `fluss_catalog`.`my_db`.`left_src` T1
Join `fluss_catalog`.`my_db`.`right_src` T2
ON T1.`city_id` = T2.`city_id`;
```

If the printed plan includes `DeltaJoin` as shown below, it indicates that the optimizer has successfully transformed the traditional streaming join into a delta join.

```title="Flink Plan"
== Abstract Syntax Tree ==
LogicalSink(table=[fluss_catalog.my_db.snk], fields=[city_id, order_id, content, city_name])
+- LogicalProject(city_id=[$0], order_id=[$1], content=[$2], city_name=[$4])
   +- LogicalJoin(condition=[=($0, $3)], joinType=[inner])
      :- LogicalTableScan(table=[[fluss_catalog, my_db, left_src]])
      +- LogicalTableScan(table=[[fluss_catalog, my_db, right_src]])

== Optimized Physical Plan ==
Sink(table=[fluss_catalog.my_db.snk], fields=[city_id, order_id, content, city_name])
+- Calc(select=[city_id, order_id, content, city_name])
   +- DeltaJoin(joinType=[InnerJoin], where=[=(city_id, city_id0)], select=[city_id, order_id, content, city_id0, city_name])
      :- Exchange(distribution=[hash[city_id]])
      :  +- TableSourceScan(table=[[fluss_catalog, my_db, left_src]], fields=[city_id, order_id, content])
      +- Exchange(distribution=[hash[city_id]])
         +- TableSourceScan(table=[[fluss_catalog, my_db, right_src]], fields=[city_id, city_name])

== Optimized Execution Plan ==
Sink(table=[fluss_catalog.my_db.snk], fields=[city_id, order_id, content, city_name])
+- Calc(select=[city_id, order_id, content, city_name])
   +- DeltaJoin(joinType=[InnerJoin], where=[(city_id = city_id0)], select=[city_id, order_id, content, city_id0, city_name])
      :- Exchange(distribution=[hash[city_id]])
      :  +- TableSourceScan(table=[[fluss_catalog, my_db, left_src]], fields=[city_id, order_id, content])
      +- Exchange(distribution=[hash[city_id]])
         +- TableSourceScan(table=[[fluss_catalog, my_db, right_src]], fields=[city_id, city_name])
```

## Understanding Prefix Keys
A prefix key defines the portion of a table’s primary key that can be used for efficient key-based lookups or index pruning.

In Fluss, the option `'bucket.key' = 'city_id'` specifies that data is organized (or bucketed) by `city_id`. When performing a delta join, this allows Flink to quickly locate and read only the subset of records corresponding to the specific prefix key value, rather than scanning or caching the entire table state.

For example:
- Full primary key: `(city_id, order_id)`
- Prefix key: `city_id`

In this setup:
* The delta join operator uses the prefix key (`city_id`) to retrieve only relevant right-side records matching each left-side event. 
* This eliminates the need to hold all records for every city in Flink state, significantly reducing state size.

Prefix keys thus form the foundation for efficient lookups in delta joins, enabling Flink to scale join workloads efficiently even under high throughput.

## Flink Version Support

The delta join feature is still evolving, and its optimization capabilities vary across Flink versions.

Refer to the [Delta Join](https://issues.apache.org/jira/browse/FLINK-37836) for the most up-to-date information.


### Flink 2.1

#### Supported Features

- Support for optimizing a dual-stream join in the straightforward pattern of "insert only source -> join -> upsert sink" into a delta join.

#### Limitations

- The primary key or the prefix key of the tables must be included as part of the equivalence conditions in the join.
- The join must be a INNER join.
- The downstream nodes of the join can accept duplicate changes, such as a sink that provides UPSERT mode without `upsertMaterialize`.
- All join inputs should be INSERT-ONLY streams.
  - This is why the option `'table.merge-engine' = 'first_row'` is added to the source table DDL.
- All upstream nodes of the join should be `TableSourceScan` or `Exchange`.
