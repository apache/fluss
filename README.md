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

<p align="center">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="website/static/img/logo/svg/white_color_logo.svg">
      <source media="(prefers-color-scheme: light)" srcset="website/static/img/logo/svg/colored_logo.svg">
      <!-- Fall back to version that works for dark and light mode -->
      <img alt="Apache Fluss logo" src="website/static/img/logo/svg/white_filled.svg">
    </picture>
</p>

<p align="center">
  <a href="https://fluss.apache.org/docs/">Documentation</a> | <a href="https://fluss.apache.org/docs/quickstart/flink/">QuickStart</a> | <a href="https://fluss.apache.org/community/dev/ide-setup/">Development</a>
</p>

<p align="center">
  <a href="https://github.com/apache/fluss/actions/workflows/ci.yaml"><img src="https://github.com/apache/fluss/actions/workflows/ci.yaml/badge.svg?branch=main" alt="CI"></a>
  <a href="https://github.com/apache/fluss/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-4EB1BA.svg" alt="License"></a>
  <a href="https://join.slack.com/t/apache-fluss/shared_invite/zt-33wlna581-QAooAiCmnYboJS8D_JUcYw"><img src="https://img.shields.io/badge/slack-join_chat-brightgreen.svg?logo=slack" alt="Slack"></a>
  <a href="https://deepwiki.com/apache/fluss"><img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki"></a>
</p>

## What is Apache Fluss (Incubating)?

Apache Fluss (Incubating) is a streaming storage built for real-time analytics & AI which can serve as the real-time data layer for Lakehouse architectures.

It bridges the gap between **data streaming** and **data Lakehouse** by enabling low-latency, high-throughput data ingestion and processing while seamlessly integrating with popular compute engines like **Apache Flink**, while 
Apache Spark, and StarRocks are coming soon.

**Fluss (German: river, pronounced `/flus/`)** enables streaming data continuously converging, distributing and flowing into lakes, like a river 🌊

## Features

- **Sub-Second Data Freshness**: Continuous ingestion and immediate availability of data enable low-latency analytics and real-time decision-making at scale.
- **Streaming & Lakehouse Unification**: Streaming-native storage with low-latency access on top of the lakehouse, using tables as a single abstraction to unify real-time and historical data across engines.
- **Columnar Streaming**: Based on Apache Arrow it allows database primitives on data streams and techniques like column pruning and predicate pushdown. This ensures engines read only the data they need, minimizing I/O and network costs.
- **Compute–Storage Separation**: Stream processors focus on pure computation while Fluss manages state and storage, with features like deduplication, partial updates, delta joins, and aggregation merge engines.
- **ML & AI–Ready Storage**: A unified storage layer supporting row-based, columnar, vector, and multi-modal data, enabling real-time feature stores and a centralized data repository for ML and AI systems.
- **Changelogs & Decision Tracking**: Built-in changelog generation provides an append-only history of state and decision evolution, enabling auditing, reproducibility, and deep system observability.

## Building

Prerequisites for building Apache Fluss:

- Unix-like environment (we use Linux, Mac OS X, Cygwin, WSL)
- Git
- Maven (we require version >= 3.8.6)
- Java 11

```bash
git clone https://github.com/apache/fluss.git
cd fluss
./mvnw clean package -DskipTests
```

Apache Fluss is now installed in `build-target`. The build command uses Maven Wrapper (`mvnw`) which ensures the correct Maven version is used.

## Contributing

Apache Fluss (Incubating) is open-source, and we’d love your help to keep it growing! Join the [discussions](https://github.com/apache/fluss/discussions),
open [issues](https://github.com/apache/fluss/issues) if you find a bug or request features, contribute code and documentation,
or help us improve the project in any way. All contributions are welcome!

## License

Apache Fluss (Incubating) project is licensed under the [Apache License 2.0](https://github.com/apache/fluss/blob/main/LICENSE).
