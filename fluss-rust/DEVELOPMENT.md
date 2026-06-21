# Development Guide

Welcome to the development guide of `fluss-rust`! This project builds `fluss-rust` client and language specific bindings.  

## Pre-requisites

- protobuf
- rust

You can install these using your favourite package / version manager. Example installation using mise:

```bash
mise install protobuf
mise install rust
```

## IDE Setup

We recommend [RustRover](https://www.jetbrains.com/rust/) IDE to work with fluss-rust code base.

### Importing fluss-rust

1. On your terminal, clone fluss-rust project from GitHub
   ```bash
   git clone https://github.com/apache/fluss-rust.git
   ```
1. Open RustRover, on `Projects` tab, click `Open` and navigate to the root directory of fluss-rust
1. Click `Open`

### Copyright Profile

Fluss and Fluss-rust are Apache projects and as such every files need to have Apache licence header. This can be automated in RustRover by adding a Copyright profile:

1. Go to `Settings` -> `Editor` -> `Copyright` -> `Copyright Profiles`.
1. Add a new profile and name it `Apache`.
1. Add the following text as the license text:
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
1. Go to `Editor` -> `Copyright` and choose the `Apache` profile as the default profile for this project.
1. Click `Apply`

We also use line comment formatting for licence headers. 
1. Go to `Editor` -> `Copyright` -> `Formatting` -> `Rust`
1. Choose `Use custom formatting`  
1. Choose `Use line comment`

## Project directories

Source files are organized in the following manner

1. `crates/fluss` - fluss rust client crate source
1. `crates/examples` - fluss rust client examples
1. `bindings` - bindings to other languages e.g. C++ under `bindings/cpp` and Python under `bindings/python`
1. Click `Apply`
2. 
## Building & Testing

See [quickstart](README.md#quick-start) for steps to run example code.

Running all unit tests for fluss rust client: 

```bash
cargo test --workspace
```

Running all integration test cases:

```bash
cargo test --features integration_tests --workspace
```


### License check (cargo-deny)

We use [cargo-deny](https://embarkstudios.github.io/cargo-deny/) to ensure all dependency licenses are Apache-compatible. When present, configuration lives in a `deny.toml` file at the repo root and should enforce an Apache-compatible license policy.

```bash
cargo install cargo-deny --locked
cargo deny check licenses
```

### Formatting and Clippy

Our CI runs cargo formatting and clippy to help keep the code base styling tidy and readable. Run the following commands and address any errors or warnings to ensure that your PR can complete CI successfully.

```bash
cargo fmt --all
cargo clippy --all-targets --fix --allow-dirty --allow-staged
```

