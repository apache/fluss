# Contributing

Welcome to the development guide for `fluss-rust`! This project builds the Fluss Rust client and language-specific bindings (Python, C++).

## Prerequisites

- Rust 1.85+ (see [rust-toolchain.toml](https://github.com/apache/fluss-rust/blob/main/rust-toolchain.toml))
- Protobuf compiler (`protoc`)

Install using your preferred package/version manager:

```bash
# Using mise
mise install protobuf
mise install rust

# Using Homebrew (macOS)
brew install protobuf

# Using apt (Ubuntu/Debian)
sudo apt-get install protobuf-compiler
```

## IDE Setup

We recommend [RustRover](https://www.jetbrains.com/rust/) IDE.

### Importing the Project

1. Clone the repository:
   ```bash
   git clone https://github.com/apache/fluss-rust.git
   ```
2. Open RustRover, go to the `Projects` tab, click `Open`, and navigate to the root directory.
3. Click `Open`.

### Copyright Profile

Fluss is an Apache project, every file needs an Apache licence header. To automate this in RustRover:

1. Go to `Settings` > `Editor` > `Copyright` > `Copyright Profiles`.
2. Add a new profile named `Apache` with this text:
   ```
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
   ```
3. Go to `Editor` > `Copyright` and set `Apache` as the default profile.
4. Go to `Editor` > `Copyright` > `Formatting` > `Rust`, choose `Use custom formatting`, then `Use line comment`.
5. Click `Apply`.

## Project Structure

```
crates/fluss        (Fluss Rust client crate)
crates/examples     (Rust client examples)
bindings/cpp        (C++ bindings)
bindings/python     (Python bindings - PyO3)
```

## Building and Testing

### Rust Client

```bash
# Build everything
cargo build --workspace --all-targets

# Run unit tests
cargo test --workspace

# Run integration tests (requires Docker)
RUST_TEST_THREADS=1 cargo test --features integration_tests --workspace

# Run a single test
cargo test test_name
```

### Python Bindings

```bash
cd bindings/python

# Install dev dependencies and build the extension
uv sync --extra dev && uv run maturin develop

# Run integration tests (requires Docker)
uv run pytest test/ -v

# To run against an existing cluster instead
FLUSS_BOOTSTRAP_SERVERS=127.0.0.1:9123 uv run pytest test/ -v
```

### C++ Bindings

```bash
cd bindings/cpp
mkdir -p build && cd build
cmake ..
cmake --build .
```

## License Check (cargo-deny)

We use [cargo-deny](https://embarkstudios.github.io/cargo-deny/) to ensure all dependency licenses are Apache-compatible:

```bash
cargo install cargo-deny --locked
cargo deny check licenses
```

## Formatting and Clippy

CI runs formatting and clippy checks. Run these before submitting a PR:

```bash
cargo fmt --all
cargo clippy --all-targets --fix --allow-dirty --allow-staged
```
