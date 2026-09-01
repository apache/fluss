<!--
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
-->

# Apache Fluss Gateway

A stateless REST gateway for Apache Fluss. It exposes REST APIs for writing to
Fluss tables and performing DDL operations, while keeping no session, cursor,
or replay state: any instance can serve any request behind a plain load
balancer.

The gateway is an executable, not a library on crates.io, and it is its own
Cargo workspace so its dependencies never touch the `fluss-rust` workspace's
lock file or its generated dependency inventories.

For the 1.0 preview scope, known limitations, and deployment security model,
see the [Fluss Gateway documentation](../website/docs/gateway/index.md).

## Status

The gateway provides configuration validation, lifecycle management, request-id,
body-size and deadline middleware, and a Prometheus listener. Its REST API
currently supports:

- health, readiness, the generated OpenAPI document, and cluster discovery;
- paginated metadata reads for databases, tables, table definitions, and
  partitions;
- DDL operations to create and drop databases, create, alter, and drop tables,
  and add and drop partitions; and
- schema-aware batched append, upsert, and delete records through
  `POST /v1/clusters/{cluster}/databases/{database}/tables/{table}/records`,
  with per-entry outcomes.

The backend runtime owns one shared service connection per configured cluster. A
connection is opened lazily on the first request that needs it, shared by every
request to that cluster, released after the configured
`connection.idle-timeout`, and drained during shutdown. Concurrent cold requests
serialize behind one connection attempt. Cancelling that request cancels its
attempt and lets the next waiter retry; bootstrap timeout and retry behavior
remain owned by `fluss-rust`.
Connections use Fluss's default plaintext protocol unless
`connection.security.protocol: sasl` selects the configured service account. A
broken transport is left to the native client, which reconnects the affected
server on its own.
`connection.identity-mode: user` is refused at startup until Fluss supports
act-as. Lookup and prefix-lookup APIs, HTTP caller authentication, and user
identity propagation remain follow-up work.

## Distribution and container

The Linux convenience distribution uses the same layout as the Java Fluss
distribution:

```text
fluss-gateway-<version>-bin-linux-<arch>/
├── bin/
├── conf/
├── openapi.yaml
├── DEPENDENCIES.rust.tsv
├── LICENSE
└── NOTICE
```

Create it from the repository `tools/` directory:

```bash
RELEASE_VERSION=1.0.0 SKIP_GPG=true releasing/create_gateway_release.sh
```

The release script uses `docker/fluss-gateway/Dockerfile.build` to pin Rust
1.88 and Debian Bookworm as the Linux build environment. It builds the host
architecture by default; set `GATEWAY_ARCH=amd64` or
`GATEWAY_ARCH=arm64` only when the selected buildx node is native for that
platform.

After extracting the archive, edit `conf/gateway.yaml` and start the foreground
process:

```bash
bin/fluss-gateway.sh
```

The wrapper resolves `FLUSS_HOME` from its own location, uses
`conf/gateway.yaml` by default, and forwards additional CLI options to the
binary. The convenience distribution follows the Java distribution and binds
listeners to loopback by default. Set `RUST_LOG=debug` when temporary diagnostic
logging, including per-request access logs, is needed.

The container image also installs into `/opt/fluss`, uses the `fluss` user with
UID/GID 9999, and reads `/opt/fluss/conf/gateway.yaml`. The image is assembled
from the prepared binary distribution, matching the Java image's
`build-target` flow. Typed environment defaults bind its REST and Prometheus
listeners to `0.0.0.0` without modifying the packaged configuration.

Build and run the local image:

```bash
just image
docker run --rm \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --stop-timeout 35 \
  -p 127.0.0.1:8080:8080 \
  -p 127.0.0.1:9095:9095 \
  -e FLUSS_GATEWAY__CLUSTER__DEFAULT__BOOTSTRAP__SERVERS=host.docker.internal:9123 \
  fluss-gateway:dev
```

The REST health and readiness endpoints are available at `/health` and
`/ready`; Prometheus metrics are served on port `9095`. Use a cluster DNS name
or container-network alias instead of `host.docker.internal` on Linux when that
hostname is unavailable. Production deployments should terminate TLS at a
trusted ingress and must not expose `trust` authentication outside a protected
network boundary.

## Prerequisites

- Rust toolchain managed by [rustup](https://rustup.rs); the workspace pins the
  channel in `rust-toolchain.toml` (stable with `rustfmt` and `clippy`)
- The declared minimum supported Rust version is 1.88, enforced by the
  `gateway-msrv` CI job
- [`just`](https://github.com/casey/just) for the recipes below

## Build and test

Run everything from this directory, or use the `just` recipes:

```bash
just build        # cargo build --all-targets
just test         # cargo test --all-targets
just test-e2e     # real Gateway + Dockerized Fluss cluster
just fmt-check    # cargo fmt --all -- --check
just clippy       # cargo clippy --all-targets -- -D warnings
just doc          # RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
just licenses     # cargo deny check licenses
just image        # build the Gateway container image
just image-smoke  # run container health and shutdown smoke tests
```

The E2E test requires Docker. It builds and invokes the `fluss-test-cluster`
helper from the `fluss-rust` workspace, creates catalog objects in a real Fluss
cluster, then verifies the cluster, database, and table REST APIs through both
its plaintext and SASL endpoints. Set `FLUSS_IMAGE` and `FLUSS_VERSION` to
select the Fluss image; CI builds and uses `fluss:dev` from the same source
revision.

Plaintext is the default and must not carry service credentials. To use
SASL/PLAIN, configure all three options explicitly:

```yaml
gateway.cluster.default.connection.security.protocol: sasl
gateway.cluster.default.connection.service.account: gateway_svc
gateway.cluster.default.connection.service.secret: change-me
gateway.cluster.default.connection.idle-timeout: 10m
```

The MSRV can be verified locally with `cargo +1.88.0 check --all-targets`.

## CI

Gateway changes are gated by a dedicated workflow, `.github/workflows/gateway-ci.yml`
(build and tests on Linux/macOS, MSRV 1.88, license headers, dependency
licenses via `cargo-deny`, formatting, clippy, rustdoc). Gateway-only changes
are excluded from the Java CI, mirroring `fluss-rust`, and the gateway workflow
never builds the `fluss-rust` workspace. The container job builds the Gateway
against the in-tree client on native `amd64` and `arm64` runners, packages the
binary distribution, assembles the image from that distribution, and exercises
startup, health, readiness, non-root execution, configuration errors, and
SIGTERM draining.

## License enforcement

Like `fluss-rust`, the gateway carries its own license enforcement: source
headers are checked by `skywalking-eyes` (`.licenserc.yaml`) and dependency
licenses by `cargo-deny` (`deny.toml`). `LICENSE-bin` and `NOTICE-bin` are
generated from the normal Linux runtime dependency closure with
`scripts/generate_binary_license.py`; build, development, and procedural-macro
tooling is excluded because it is not linked into the distributed executable.
CI rejects drift in all three dependency records.
