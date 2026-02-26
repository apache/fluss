# Apache Fluss Rust, Python, and C++ Client 0.1.0 Release

The Apache Fluss (Incubating) community is pleased to announce the release of fluss-rust 0.1.0, the first official release of the Rust, Python, and C++ clients for Apache Fluss.

## What is fluss-rust?

[fluss-rust](https://github.com/apache/fluss-rust) is the official [Apache Fluss (Incubating)](https://fluss.apache.org/) client library, written in Rust with bindings for [Python](TODO: Add the link once website is setup) and [C++](TODO: Add the link once website is setup). All three clients share
the same Rust core, gaining native Rust performance while retaining language ergonomics. Built on [Apache Arrow](https://arrow.apache.org/), fluss-rust enables table management, log streaming, and key-value operations with zero-copy efficiency and async-first design.

Fluss is a streaming storage built for real-time analytics, serving as the real-time data layer for Lakehouse architectures. It bridges the gap between streaming data and the data Lakehouse by enabling low-latency, high-throughput data ingestion and processing while seamlessly integrating with popular compute engines.

This 0.1.0 release represents the culmination of 130+ commits from the community, delivering a feature-rich multi-language client from the ground up.

## Highlights

### Table Operation Support

fluss-rust supports Fluss's core table types:

- Log Tables (append-only) — Time-series data ingestion with subscription-based polling, offset tracking, and configurable batch sizes. See [log table examples](TODO: Add the link once website is setup).
- Primary Key Tables (KV) — Upsert, delete, and point lookup operations with compacted row encoding, including partial updates. See the [primary key table examples](TODO: Add the link once website is setup).
- Partitioned Tables — Full support for partition-aware reads and writes, dynamic partition discovery, and partition-specific subscribe/unsubscribe. See the [partitioned table](TODO: Add the link once website is setup).

### Apache Arrow Native

Records are represented as Apache Arrow RecordBatch objects throughout the stack, enabling zero-copy data sharing between Rust, Python, and C++, columnar access patterns for analytical workloads, and direct integration with the broader Arrow ecosystem. The client supports 18+ data types including decimals, timestamps, and nested structures. See [supported Arrow data types](TODO: Add the link once website is setup).

### Remote Storage Support

fluss-rust supports reading log segments from remote storage backends, with pluggable backends enabled through feature flags:

- storage-memory — In-memory (default)
- storage-fs — Local filesystem (default)
- storage-s3 — [Amazon S3](https://aws.amazon.com/s3/)
- storage-oss — [Alibaba Object Storage Service](https://www.alibabacloud.com/product/object-storage-service)

Remote log fetching includes a priority-queue-based prefetching system with configurable concurrent downloads for optimized read performance.

### Other Features

The release also includes SASL/PLAIN authentication across all three clients, a comprehensive admin API for database and table management, fire-and-forget write batching with configurable bucket assignment strategies, column projection, and more. See [fluss-rust documentation](TODO: Add the link once website is setup).

### Getting Started

- Rust: fluss-rs [installation guide](TODO: Add the link once website is setup).
- Python: pyfluss [installation guide](TODO: Add the link once website is setup).
- C++: fluss-cpp [installation guide](TODO: Add the link once website is setup).

### What's Next

This is the first release of fluss-rust, and the community is actively working on expanding capabilities. Areas of future development include additional language bindings, additional storage backends, enhanced compression support, and expanded ecosystem integrations.

### Getting Involved

The Apache Fluss community welcomes contributions!

- GitHub: https://github.com/apache/fluss-rust
- Documentation: TODO: Add the link once website is setup
- Website: https://fluss.apache.org/
- Mailing List: dev@fluss.apache.org
- Slack:https://apache-fluss.slack.com