# Fluss Roadmap
This roadmap provides a high-level summary of ongoing efforts in the Fluss community. Fluss is positioned as the **Streaming Storage for Real-Time Analytics and AI**.

For detailed tracking, see the [Fluss 2026 Roadmap](https://github.com/apache/fluss/discussions/2342).

## Real-Time AI and ML
- Real-Time Feature Store with aggregation merge engines, schema evolution, and point-in-time correctness.
- Multimodal Streaming Data support for rows, columns, vectors, variant, and images.
- High-performance Rust/Python SDK integrating PyTorch, Ray, Pandas, and PyArrow.

## Real-Time Lakehouse
- Iceberg V3, Hudi, and Delta Lake integration
- In-Place Lakehouse: Define Fluss tables on existing Lake tables
- Native Union Read for Spark, Trino, and StarRocks
- Deletion Vectors to accelerate updates and deletes

## Streaming Analytics
- Global Secondary Index for non-primary key lookups
- Delta Join with multi-stream and left/right/full join support
- Cost-Based Optimizer in Flink SQL with Fluss table statistics
- Full Spark Engine support with Structured Streaming integration

## Storage Engine
- Columnar Streaming with Filter and Aggregation Pushdown
- Full Schema Evolution with table renaming and column defaults

## Cloud-Native Architecture
- ZooKeeper Removal for simpler deployment
- Zero Disks: Direct S3 writes for elastic, diskless storage

## Connectivity and Ingestion
- Log agent integration 
- Client SDKs: Rust, C++, Python

## Operational Excellence
- Automated cluster rebalancing and bucket rescaling
- Coordinator HA with multi-AZ and cross-cluster geo-replication

## Security
- (m)TLS for intra-cluster, ZooKeeper, and external clients

*This roadmap is subject to change based on community feedback and evolving requirements.*