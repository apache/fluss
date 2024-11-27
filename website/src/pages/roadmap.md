# Fluss Roadmap

This roadmap means to provide users and contributors with a high-level summary of ongoing efforts in the Fluss community.
The roadmap contains both efforts working in process as well as completed efforts, so that users may get a better impression of the overall status and direction of those developments.

## Kafka Protocol Compatibility

Fluss will support the Kafka network protocol to enable users to use Fluss as a drop-in replacement for Kafka. This will allow users to leverage Fluss's real-time storage capabilities while maintaining compatibility with existing Kafka applications.

## Flink Integration

Fluss will provide deep integration with Apache Flink, enabling users a single engine experience for building real-time analytics applications.
The integration will include:
- More pushdown optimizations: Filter Pushdown, Partition Pruning, Aggregation Pushdown, etc.
- Upgrade the Rule-Based Optimization into Cost-Based Optimization in Flink SQL streaming planner with leveraging statistics in Fluss tables.
- Support new [Delta Join](https://cwiki.apache.org/confluence/display/FLINK/FLIP-486%3A+Introduce+A+New+DeltaJoin) to address the pain-points of Stream-Stream Join.
- Support for Flink DataStream API to read/write data from/to Fluss.

## Streaming Lakehouse

- Support for Iceberg as Lakehouse Storage. And DeltaLake, Hudi as well.
- Support Union Read for Spark, Trino, StarRocks.
- Avoid data shuffle in compaction service to directly compact Arrow files of Fluss into Parquet files of data lakes.

## ZooKeeper Removal

Fluss currently utilizes **ZooKeeper** for cluster coordination, metadata storage, and cluster configuration management.
In upcoming releases, **ZooKeeper will be replaced** by **KvStore** for metadata storage and **Raft** for cluster coordination and ensuring consistency.
This transition aims to streamline operations and enhance system reliability.

## Storage Engine

- Support for complex data types: Array, Map, Struct, Variant/JSON.
- Support for schema evolution.
- Support for secondary index for Delta Join with Flink.
- Support for buckets rescale.

## Maintenance

- Re-balance Cluster
- Gray Upgrade

## Miscellaneous

- Upgrade programing language to Java 17.
- Support for more connectors: Spark, Presto, DuckDB, etc.
