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

## Kafka Protocol Compatibility

Fluss will support the Kafka network protocol to enable users to use Fluss as a drop-in replacement for Kafka. This will allow users to leverage Fluss's real-time storage capabilities while maintaining compatibility with existing Kafka applications.

## Flink Integration

Fluss will provide deep integration with Apache Flink, enabling users a single engine experience for building real-time analytics applications.
The integration will include:
- Support for Flink **DataStream API** to read/write data from/to Fluss.
- Support new [Delta Join](https://cwiki.apache.org/confluence/display/FLINK/FLIP-486%3A+Introduce+A+New+DeltaJoin) to address the pain-points of Stream-Stream Join.
- More pushdown optimizations: Filter Pushdown ([#197](https://github.com/alibaba/fluss/issues/197)), Partition Pruning ([#196](https://github.com/alibaba/fluss/issues/196)), Aggregation Pushdown, etc.
- Upgrade the Rule-Based Optimization into Cost-Based Optimization in Flink SQL streaming planner with leveraging statistics in Fluss tables.


## Streaming Lakehouse

- Support for Iceberg ([#102](https://github.com/alibaba/fluss/issues/102)) as Lakehouse Storage. And DeltaLake, Hudi as well.
- Support Union Read for Spark, Trino, StarRocks.
- Avoid data shuffle in compaction service to directly compact Arrow files of Fluss into Parquet files of data lakes ([#107](https://github.com/alibaba/fluss/issues/107)).

## ZooKeeper Removal

Fluss currently utilizes **ZooKeeper** for cluster coordination, metadata storage, and cluster configuration management.
In upcoming releases, **ZooKeeper will be replaced** by **KvStore** for metadata storage and **Raft** for cluster coordination and ensuring consistency.
This transition aims to streamline operations and enhance system reliability.

## Storage Engine

- Support for complex data types: Array ([#168](https://github.com/alibaba/fluss/issues/168)), Map ([#169](https://github.com/alibaba/fluss/issues/169)), Struct ([#170](https://github.com/alibaba/fluss/issues/170)), Variant/JSON.
- Support for schema evolution.
- Support for secondary index for Delta Join with Flink (~~[#65](https://github.com/alibaba/fluss/issues/65)~~).
- Support for buckets rescale.

## Zero Disks

Fluss currently utilizes a tiered storage architecture to significantly reduce storage costs and operational complexities.
However, the Fluss community is actively investing in the Zero Disk architecture,
which aims to completely replace local disks with S3 storage. This transition will enable Fluss to achieve a
serverless, stateless, and elastic design, significantly minimizing operational overhead while eliminating inter-zone networking costs.

## Maintenance

- Re-balance Cluster
- Gray Upgrade

## Miscellaneous

- Upgrade programing language to Java 17 ([#203](https://github.com/alibaba/fluss/issues/203)).
- Support for more connectors: Spark ([#155](https://github.com/alibaba/fluss/issues/155)), Presto, DuckDB, etc.
