<p align="center">
  <img src="website/static/img/banner.png" />
</p>

<p align="center">
  <a href="https://alibaba.github.io/fluss-docs/docs/intro/">Documentation</a> | <a href="https://alibaba.github.io/fluss-docs/docs/quickstart/flink/">QuickStart</a> | <a href="https://alibaba.github.io/fluss-docs/docs/dev/ide-setup/">Development</a>
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
