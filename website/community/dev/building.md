---
sidebar_position: 1
title: Building Fluss
---

# Building Fluss from Source

This page covers how to build Fluss from sources.

In order to build Fluss you need to get the source code by [cloning the git repository](https://github.com/apache/fluss).

In addition, you need **Maven 3.8.6** and a **JDK** (Java Development Kit). Fluss requires **Java 11** to build.

To clone from git, enter:

```bash
git clone git@github.com:apache/fluss.git
```

If you want to build a specific release or release candidate, have a look at the existing tags using

```bash
git tag -n
```

and checkout the corresponding branch using

```bash
git checkout <tag>
```

The simplest way of building Fluss is by running:

```bash
mvn clean install -DskipTests
```

This instructs [Maven](http://maven.apache.org) (`mvn`) to first remove all existing builds (`clean`) and then create a new Fluss binary (`install`).

:::tip
Using the included [Maven Wrapper](https://maven.apache.org/wrapper/) by replacing `mvn` with `./mvnw` ensures that the correct Maven version is used.
:::

To speed up the build you can:
- skip tests by using ` -DskipTests`
- use Maven's parallel build feature, e.g., `mvn package -T 1C` will attempt to build 1 module for each CPU core in parallel.

The build script will be:
```bash
mvn clean install -DskipTests -T 1C
```

If you need to compile artifacts that target Java 8 bytecode, enable the
`java8` Maven profile:

```bash
mvn clean install -DskipTests -Pjava8
```

**NOTE**:
- For local testing, it's recommend to use directory `${project}/build-target` in project.
- For deploying distributed cluster, it's recommend to use binary file named `fluss-xxx-bin.tgz`, the file is in directory `${project}/fluss-dist/target`.

## Building the Rust client (fluss-rust)

The Rust client, language bindings, and examples live under `fluss-rust/` and build with Cargo. You need **Rust** (the toolchain pinned in `fluss-rust/rust-toolchain.toml`, currently 1.85+) and **protoc**, the Protobuf compiler — `build.rs` compiles the canonical `fluss-rpc/src/main/proto/FlussApi.proto`.

```bash
# protoc (pick one)
brew install protobuf                    # macOS
sudo apt-get install protobuf-compiler   # Debian/Ubuntu

cd fluss-rust
cargo build --workspace --all-targets    # build everything
cargo test --workspace                    # unit tests
```

Integration tests start a Fluss cluster via Docker:

```bash
RUST_TEST_THREADS=1 cargo test --features integration_tests --workspace
```

The Python and C++ bindings build on top of the Rust crate:

```bash
cd fluss-rust/bindings/python && uv sync --extra dev && uv run maturin develop   # Python
cd fluss-rust/bindings/cpp && cmake -B build && cmake --build build              # C++
```

Before pushing, run the same checks CI does:

```bash
cd fluss-rust
cargo fmt --all -- --check
cargo clippy --all-targets --workspace -- -D warnings
cargo deny check licenses
```
