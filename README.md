Fluss
----

Fluss is a streaming storage built for real-time analytics which can serve as the real-time data layer
for Lakehouse architectures. With its columnar stream and real-time update capabilities,
Fluss integrates seamlessly with Apache Flink to enable high-throughput, low-latency, cost-effective
streaming data warehouses tailored for real-time applications.

The Fluss project, named after the German word for 'river' and pronounced `/flus/`,
symbolizes the essence of streaming data continuously flowing, converging, and distributing like a river.

## Documentation

Learn more about Fluss at [https://alibaba.github.io/fluss-docs](https://alibaba.github.io/fluss-docs/).

[QuickStart](https://alibaba.github.io/fluss-docs/docs/quickstart/flink/) | [Architecture](https://alibaba.github.io/fluss-docs/docs/concepts/architecture/) | [Development](https://alibaba.github.io/fluss-docs/docs/dev/ide-setup/)

## Build

Prerequisites for building Fluss:

- Unix-like environment (we use Linux, Mac OS X, Cygwin, WSL)
- Git
- Maven (we require version >= 3.8.6)
- Java 8 or 11

```bash
git clone https://github.com/alibaba/fluss.git
cd fluss
mvn clean package -DskipTests
```

Fluss is now installed in `build-target`.

## Contributing

Fluss is open-source, and we’d love your help to keep it growing! Join the [discussions](https://github.com/alibaba/fluss/discussions),
open [issues](https://github.com/alibaba/fluss/issues) if you find a bug or request features, contribute code and documentation,
 or help us improve the project in any way. All contributions are welcome!

## License

Fluss project is licensed under the [Apache License 2.0](https://github.com/alibaba/fluss/blob/main/LICENSE).
