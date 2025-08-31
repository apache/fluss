<p align="center">
  <img src="website/static/img/banner.png" alt="Fluss - Streaming Storage for Real-Time Analytics" />
</p>

<p align="center">
  <a href="https://alibaba.github.io/fluss-docs/docs/intro/">Documentation</a> | <a href="https://alibaba.github.io/fluss-docs/docs/quickstart/flink/">QuickStart</a> | <a href="https://alibaba.github.io/fluss-docs/community/dev/ide-setup/">Development</a>
</p>

<p align="center">
  <a href="https://github.com/alibaba/fluss/actions/workflows/ci.yaml"><img src="https://github.com/alibaba/fluss/actions/workflows/ci.yaml/badge.svg" alt="CI"></a>
  <a href="https://github.com/alibaba/fluss/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-4EB1BA.svg" alt="License"></a>
  <a href="https://join.slack.com/t/fluss-hq/shared_invite/zt-33wlna581-QAooAiCmnYboJS8D_JUcYw"><img src="https://img.shields.io/badge/slack-join_chat-brightgreen.svg?logo=slack" alt="Slack"></a>
</p>

## What is Fluss?

Fluss is a streaming storage built for real-time analytics which can serve as the real-time data layer for Lakehouse architectures.

It bridges the gap between **data streaming** and **data Lakehouse** by enabling low-latency, high-throughput data ingestion and processing while seamlessly integrating with popular compute engines like **Apache Flink**, while Apache Spark, and StarRocks are coming soon.

**Fluss (German: river, pronounced `/flus/`)** enables streaming data continuously converging, distributing and flowing into lakes, like a river 🌊

## Building

Prerequisites for building Fluss:

- Unix-like environment (we use Linux, Mac OS X, Cygwin, WSL)
- Git
- Maven (we require version >= 3.8.6)
- Java 8 or 11

```bash
git clone https://github.com/alibaba/fluss.git
cd fluss
./mvnw clean package -DskipTests
```

Fluss is now installed in `build-target`. The build command uses Maven Wrapper (`mvnw`) which ensures the correct Maven version is used.

## Contributing

Fluss is open-source, and we’d love your help to keep it growing! Join the [discussions](https://github.com/alibaba/fluss/discussions),
open [issues](https://github.com/alibaba/fluss/issues) if you find a bug or request features, contribute code and documentation,
 or help us improve the project in any way. All contributions are welcome!

## License

Fluss project is licensed under the [Apache License 2.0](https://github.com/alibaba/fluss/blob/main/LICENSE).
