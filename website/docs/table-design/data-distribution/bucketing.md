---
title: "Bucketing"
sidebar_position: 1
---

# Bucketing

A bucketing strategy is a data distribution technique that divides table data into small pieces 
and distributes the data to multiple hosts and services.

When creating a Fluss table, you can specify the number of buckets by setting `'bucket.num' = '<num>'` property for the table, see more details in [DDL](engine-flink/ddl.md).
Currently, Fluss supports 3 bucketing strategies: **Hash Bucketing**, **Sticky Bucketing** and **Round-Robin Bucketing**.
Primary-Key Tables only allow to use **Hash Bucketing**. Log Tables use **Sticky Bucketing** by default but can use other two bucketing strategies.

## Hash Bucketing
**Hash Bucketing** is common in OLAP scenarios.
The advantage is that it can be very evenly distributed to multiple nodes, making full use of the capabilities of distributed computing, and has excellent
scalability (rescale buckets or clusters) to cope with massive data.

**Usage**: setting `'bucket.key' = 'col1, col2'` property for the table to specify the bucket key for hash bucketing.
Primary-Key Tables use primary key (excluding partition key) as the bucket key by default.

## Sticky Bucketing

**Sticky Bucketing** enables larger batches and reduces latency when writing records into Log Tables. After sending a batch, the sticky bucket changes. Over time, the records are spread out evenly among all the buckets.
Sticky Bucketing is the default bucketing strategy for Log Tables. This is quite important because Log Tables uses Apache Arrow as the underling data format which is efficient for large batches.

**Usage**: setting `'client.writer.bucket.no-key-assigner'='sticky'` property for the table to enable this strategy. PrimaryKey Tables do not support this strategy.

## Round-Robin Bucketing

**Round-Robin Bucketing** is a simple strategy that randomly selects a bucket for each record before writing it in. This strategy is suitable for scenarios where the data distribution is relatively uniform and the data is not skewed.

**Usage**: setting `'client.writer.bucket.no-key-assigner'='round_robin'` property for the table to enable this strategy. PrimaryKey Tables do not support this strategy.
