---
sidebar_position: 1
---

# Bucketing

A bucketing strategy is a data distribution technique that divides table data into small pieces 
and distributes the data to multiple hosts and services. Fluss, uses Hash bucketing as the default bucket splitting strategy.

## Hash Bucketing
**Hash Bucketing** is common in OLAP scenarios. Typical systems include Kafka, StarRocks and Paimon.
The advantage is that it can be very evenly distributed to multiple nodes, making full use of the capabilities of distributed computing, and has excellent
scalability (rescale buckets or clusters) to cope with massive data. The disadvantage is that that range queries are not efficient, because they have to be reordered at the calculation layer.

**Usage** We use in DDL `DISTRIBUTED BY HASH (<bucket_key>...) INTO n BUCKETS` to describe the bucket key and bucket number.
The bucket key can be omitted, and the primary key is used by default (provided that the primary key is defined).

## Hash Algorithm
To reduce the amount of data that needs to be moved during rescale buckets, we introduce a concept similar to Flink KeyGroup, that is 
**key -> key_group_id -> bucket_id**. The `bucket_id` is calculated with the following formula:
> key_group_id = hash(key) % max_buckets, bucket_id = key_group_id / buckets. 

The capacity scaling process is similar to Flink rescale keyed state. 