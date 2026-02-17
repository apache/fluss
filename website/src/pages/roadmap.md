# Fluss Roadmap
This roadmap means to provide users and contributors with a high-level summary of ongoing efforts in the Fluss community. The roadmap contains both efforts working in process as well as completed efforts, so that users may get a better impression of the overall status and direction of those developments.

Fluss is positioned as the Streaming Storage for Real-Time Analytics and AI.

## Real-Time AI and ML
- Real-Time Feature Store with aggregation merge engines, schema evolution, and point-in-time correctness.
- Multimodal Streaming Data support for rows, columns, vectors, variant, and images.
- High-performance Rust/Python SDK integrating PyTorch, Ray, Pandas, and PyArrow.

## Real-Time Lakehouse
- Iceberg V3 support
- Schema evolution for Lakehouse tables tiering
- In-Place Lakehouse: Define Fluss tables on existing Lake tables
- Native Union Read support for Spark, Trino, and StarRocks.
- Deletion Vectors to accelerate updates and deletes.
- Integration with Hudi and Delta Lake.

## Streaming Analytics
- Global Secondary Index for fast lookups on non-primary key columns.
- Delta Join with multi-stream and left/right/full join support.
- RoaringBitmap aggregation and auto-increment keys for User/Item Profiles.
- Cost-Based Optimizer in Flink SQL leveraging Fluss table statistics.
- Full Spark Engine support with Structured Streaming Real-Time Mode integration.

## Storage Engine
- Columnar Streaming with Filter Pushdown and Aggregation Pushdown.
- Full Schema Evolution: add, alter, drop, and rename columns.
- Table renaming and column default values.

## Cloud-Native Architecture
- ZooKeeper Removal to simplify deployment and reduce operational complexity.
- Zero Disks: Direct write to S3 for diskless, elastic storage nodes.

## Connectivity and Ingestion
- Log agent integration with Logstash, Vector, Filebeat, Fluentd, or Kafka compatibility.
- Client SDK expansion: Rust, C++, and Python.
- Postgres API compatibility.

## Operational Excellence
- Automated cluster rebalancing.
- Bucket rescaling for Log Tables and Primary Key Tables.
- Coordinator high-availability with multi-AZ support.
- Cross-cluster geo-replication.

## Security
- Support (m)TLS for intra-cluster, ZooKeeper, and external clients communication.

For a more exhaustive list and to track recent changes, refer to the [Fluss 2026 Roadmap Discussion](https://github.com/apache/fluss/discussions/2342).

*This roadmap is subject to change based on community feedback, technical discoveries, and evolving requirements. For the most up-to-date information, please refer to the GitHub milestone boards and project issues.*