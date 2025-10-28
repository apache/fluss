---
sidebar_label: DeltaJoins
title: Flink Delta Joins
sidebar_position: 6
---

# Delta Join
Begin with Flink 2.1, a new delta join operator was introduced. Compared to traditional streaming join, delta join significantly reduces the required state, effectively alleviating issues associated with large state, such as resource bottlenecks, lengthy checkpoint execution times, and long recovery times during job restarts.

Starting from Fluss version 0.8, streaming join jobs running on Flink 2.1 or higher will be automatically optimized to delta join in applicable scenarios.

## Examples

Here is an example of delta join currently supported in Flink 2.1.

1. Create two source tables and one sink tables

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

```sql title="Flink SQL"
CREATE DATABASE my_db;
```

```sql title="Flink SQL"
USE my_db;
```

```sql title="Flink SQL"
CREATE TABLE `fluss_catalog`.`my_db`.`left_src` (
  `city_id` INT NOT NULL,
  `order_id` INT NOT NULL,
  `content` VARCHAR NOT NULL,
  PRIMARY KEY (city_id, order_id) NOT ENFORCED
) WITH (
    -- prefix lookup key
    'bucket.key' = 'city_id',
    -- in Flink 2.1, delta join only support append-only source
    'table.merge-engine' = 'first_row'
    ...)
;
```

```sql title="Flink SQL"
CREATE TABLE `fluss_catalog`.`my_db`.`right_src` (
  `city_id` INT NOT NULL,
  `city_name` VARCHAR NOT NULL,
  PRIMARY KEY (city_id) NOT ENFORCED
) WITH (
    -- in Flink 2.1, delta join only support append-only source
    'table.merge-engine' = 'first_row'
    ...)
;
```

```sql title="Flink SQL"
CREATE TABLE `fluss_catalog`.`my_db`.`snk` (
    `city_id` INT NOT NULL,
    `order_id` INT NOT NULL,
    `content` VARCHAR NOT NULL,
    `city_name` VARCHAR NOT NULL,
    PRIMARY KEY (city_id, order_id) NOT ENFORCED
) WITH (...)
;
```

2. Explain DML about streaming join

```sql title="Flink SQL"
EXPLAIN 
INSERT INTO `fluss_catalog`.`my_db`.`snk`
SELECT T1.`city_id`, T1.`order_id`, T1.`content`, T2.`city_name` 
FROM `fluss_catalog`.`my_db`.`left_src` T1
Join `fluss_catalog`.`my_db`.`right_src` T2
ON T1.`city_id` = T2.`city_id`
;
```

If you see the plan that includes DeltaJoin as following, it indicates that the optimization has been effective, and the streaming join has been successfully optimized into a delta join.

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

## Flink Version Support

The work on Delta Join is still ongoing, so the support for more sql patterns that can be optimized into delta join varies across different versions of Flink. More details can be found at [Delta Join](https://issues.apache.org/jira/browse/FLINK-37836).

### Flink 2.1

#### Supported Features

- Support for optimizing a dual-stream join in the straightforward pattern of "insert only source -> join -> upsert sink" into a delta join.

#### Limitations

- The primary key or the prefix lookup key of the tables must be included as part of the equivalence conditions in the join.
- The join must be a INNER join.
- The downstream nodes of the join can accept duplicate changes, such as a sink that provides UPSERT mode.
- All join inputs should be INSERT-ONLY streams.
  - This is why the option `'table.merge-engine' = 'first_row'` is added to the source table DDL.
- All upstream nodes of the join should be `TableSourceScan` or `Exchange`.
