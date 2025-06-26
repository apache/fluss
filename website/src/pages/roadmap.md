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

# Fluss Roadmap

This roadmap means to provide users and contributors with a high-level summary of ongoing efforts in the Fluss community.
The roadmap contains both efforts working in process as well as completed efforts, so that users may get a better impression of the overall status and direction of those developments.

**Milestone Board:** https://github.com/alibaba/fluss/milestone/4

## Fluss Server

### High Availability and Reliability
- **Coordinator HA** ([#188](https://github.com/alibaba/fluss/issues/188)): CoordinatorServer supports high availability configuration.
- **Rolling Upgrade** ([#1142](https://github.com/alibaba/fluss/issues/1142)): Support ControlledShutdown for TabletServer enabling zero-downtime upgrades.

### Cluster Operations
- **Replica Rebalancing** ([#1144](https://github.com/alibaba/fluss/issues/1144)): Automatic cluster rebalancing capabilities.
- **Throttling Support** ([#1145](https://github.com/alibaba/fluss/issues/1145)): Comprehensive cluster throttling capabilities for resource management.

### Server-Side Optimizations
- **Metadata Cache Refactoring** ([#483](https://github.com/alibaba/fluss/issues/483)): Refactor metadata cache and async updating mechanism in client side.
- **Leader Epoch Implementation** ([#673](https://github.com/alibaba/fluss/issues/673)): Use Leader Epoch rather than High Watermark for truncation operations.
- **Metadata Optimization** ([#982](https://github.com/alibaba/fluss/issues/982)): Remove TableInfo from server MetadataCache for better performance.

### Compatibility and Testing
- **Backward/Forward Compatibility** ([#1161](https://github.com/alibaba/fluss/issues/1161)): Add backward and forward compatibility E2E tests.

## Streaming Lakehouse

### Production-Ready Features
- **Union Read Streaming Support** ([#1140](https://github.com/alibaba/fluss/issues/1140)): Enable union read operations in streaming mode for real-time data processing.
- **Tiered Log Scanner** ([#41](https://github.com/alibaba/fluss/issues/41)): Support fetching logs that have been tiered into lake storage.
- **Paimon Decoupling** ([#1141](https://github.com/alibaba/fluss/issues/1141)): Decouple union read functionality from Paimon dependency.

### Lakehouse Format Support
- **Iceberg Integration** ([#452](https://github.com/alibaba/fluss/issues/452)): Support for Iceberg as Lakehouse Storage with full compatibility.
- **Lance Table Format** ([#1155](https://github.com/alibaba/fluss/issues/1155)): Complete support for Lance table format in streaming lakehouse architecture.
- **Multi-Format Support**: Future support for DeltaLake and Hudi as lakehouse storage options.

### Engine Integration
- **Union Read for Multiple Engines**: Support Union Read for Spark, Trino, and StarRocks.
- **Direct Compaction Optimization** ([#107](https://github.com/alibaba/fluss/issues/107)): Avoid data shuffle in compaction service to directly compact Arrow files into Parquet files.

## Data Types and Schema Management

### Complex Data Types ([#816](https://github.com/alibaba/fluss/issues/816))
- Support for Array ([#168](https://github.com/alibaba/fluss/issues/168)), Map ([#169](https://github.com/alibaba/fluss/issues/169)), Struct ([#170](https://github.com/alibaba/fluss/issues/170)), and Variant/JSON data types.

### Schema Evolution ([#1154](https://github.com/alibaba/fluss/issues/1154))
- Comprehensive schema evolution support for backward and forward compatibility.
- **Compatibility Testing** ([#1161](https://github.com/alibaba/fluss/issues/1161)): Add backward and forward compatibility E2E tests.

## Flink Integration

### Version Support
- **Flink 2.0 & 2.1 Support** ([#651](https://github.com/alibaba/fluss/issues/651)): Full compatibility with latest Flink versions.
- **Delta Join Support** ([#1143](https://github.com/alibaba/fluss/issues/1143)): Support new Delta Join functionality on Flink 2.1 to address Stream-Stream Join pain points.

### Advanced Features
- **Materialized Table Support** ([#860](https://github.com/alibaba/fluss/issues/860)): Support for Flink materialized table functionality.
- **System Tables**:
  - **$changelog table** ([#356](https://github.com/alibaba/fluss/issues/356)): Support changelog auxiliary table for Flink connector.
  - **$binlog table** ([#978](https://github.com/alibaba/fluss/issues/978)): Add binlog system table support for Fluss tables.

### Performance Optimizations
- **Filter Pushdown** ([#197](https://github.com/alibaba/fluss/issues/197)): Support filter pushdown for Log Tables.
- **Partition Pruning** ([#196](https://github.com/alibaba/fluss/issues/196)): Optimize query performance through partition pruning.
- **Cost-Based Optimization**: Upgrade Rule-Based Optimization to Cost-Based Optimization in Flink SQL streaming planner leveraging Fluss table statistics.
- **Auto Drop Partition Optimization** ([#864](https://github.com/alibaba/fluss/issues/864)): Optimize automatic partition dropping performance.

## Engine Connectors

### Spark Connector ([#155](https://github.com/alibaba/fluss/issues/155))
Native support for Spark as a computation engine, enabling efficient batch, streaming, and interactive query processing.

### Additional Connectors
- Future support for StarRocks, DuckDB, and other popular engines.

## Storage Engine Enhancements

### Advanced Features
- **Secondary Index Support**: Support for secondary index for Delta Join with Flink (~~[#65](https://github.com/alibaba/fluss/issues/65)~~).
- **Bucket Rescaling**: Dynamic bucket rescaling capabilities for better resource utilization.

## Infrastructure and Deployment

### ZooKeeper Removal
Fluss currently utilizes **ZooKeeper** for cluster coordination, metadata storage, and cluster configuration management.
In upcoming releases, **ZooKeeper will be replaced** by **KvStore** for metadata storage and **Raft** for cluster coordination and ensuring consistency.
This transition aims to streamline operations and enhance system reliability.

### Zero Disks Architecture
Fluss currently utilizes a tiered storage architecture to significantly reduce storage costs and operational complexities.
The community is actively investing in the Zero Disk architecture, which aims to completely replace local disks with S3 storage.
This transition will enable Fluss to achieve a serverless, stateless, and elastic design, significantly minimizing operational overhead while eliminating inter-zone networking costs.

### Deployment
- **Helm Charts** ([#779](https://github.com/alibaba/fluss/issues/779)): Add Helm chart for easier Kubernetes deployment and increased adoption.

## Filesystems Support

### Cloud Storage
- **Google Cloud Storage** ([#993](https://github.com/alibaba/fluss/issues/993)): Implement obtainSecurityToken for Google filesystem.
- **MinIO Support** ([#621](https://github.com/alibaba/fluss/issues/621)): Add AssumeRoleWithWebIdentity STS functionality for temporary credentials to S3 filesystem.

## Client SDKs and Language Support

### Python Client
Support Python SDK to connect with Python ecosystems, including PyArrow, Pandas, Lance, and DuckDB.
This may be developed in a separate repository: `apache/fluss-python`.

### Java Version Upgrade
- **Java 11 Support** ([#1156](https://github.com/alibaba/fluss/issues/1156)): Upgrade default compile language to Java 11 (updated from Java 17 plan).

## Kafka Protocol Compatibility

Fluss will support the Kafka network protocol to enable users to use Fluss as a drop-in replacement for Kafka.
This will allow users to leverage Fluss's real-time storage capabilities while maintaining compatibility with existing Kafka applications.

## Monitoring and Observability

### Metrics and Monitoring
- **OpenTelemetry Integration** ([#181](https://github.com/alibaba/fluss/issues/181)): OpenTelemetry metric reporter for comprehensive observability.

## Future Considerations

### Maintenance and Operations
- **Re-balance Cluster**: Advanced cluster rebalancing strategies.
- **Gray Upgrade**: Gradual deployment and upgrade capabilities.

---

*This roadmap is subject to change based on community feedback, technical discoveries, and evolving requirements. For the most up-to-date information, please refer to the GitHub milestone boards and project issues.*