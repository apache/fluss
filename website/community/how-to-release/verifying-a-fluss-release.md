---
title: Verifying a Fluss Release
sidebar_position: 3
---

# Verifying a Fluss Release

## Validating distributions

Release vote email includes links to:

- Distribution archives (source, Java server, Gateway) on dist.apache.org
- Signature files (.asc)
- Checksum files (.sha512)
- KEYS file

After downloading the distributions archives, signatures, checksums, and KEYS file, here are the instructions on how to verify signatures, checksums.

## Verifying signatures

First, import the keys in your local keyring:

```bash
curl https://downloads.apache.org/fluss/KEYS -o KEYS
gpg --import KEYS
```

Next, verify all `.asc` files:

```bash
for i in *.tgz; do echo $i; gpg --verify $i.asc $i; done
```
If the verification is successful, you will see a message like this:

```
fluss-1.0.0-src.tgz
gpg: Signature made Mon 01 Jan 2024 12:00:00 PM UTC
gpg:                using RSA key E2C45417BED5C104154F341085BACB5AEFAE3202
gpg: Good signature from "Jark Wu (CODE SIGNING KEY) <jark@apache.org>"
```

## Verifying checksums

Next, verify all the checksums:

```bash
shasum *.sha512 > checklist.chk; shasum -c checklist.chk
```

If the verification is successful, you will see a message like this:

```
fluss-1.0.0-bin.tgz.sha512: OK
fluss-gateway-1.0.0-bin-linux-amd64.tgz.sha512: OK
fluss-1.0.0-src.tgz.sha512: OK
```

## Verifying build

Unzip the source release archive (`fluss-1.0.0-src.tgz`), and verify that the source release builds correctly (may with different Java version and Maven version), you can run the following commands:

```bash
mvn clean package -DskipTests
```

## Verifying LICENSE/NOTICE

Unzip the source release archive, and verify that:

1. Check the LICENSE and NOTICE files are correct.
2. All files have ASF license headers if necessary.
3. All dependencies must be checked for their license and the license must be ASL 2.0 compatible (http://www.apache.org/legal/resolved.html#category-x)
4. Compatible non-ASL 2.0 licenses should be contained in the `META-INF/licenses` directory of the respective module
5. The LICENSE and NOTICE files in the root directory refer to dependencies in the source release, i.e., files in the git repository (such as fonts, css, JavaScript, images)


## Verifying the clients (Rust / Python / C++)

The Rust, Python, and C++ clients ship in the same source release under `fluss-rust/`. Build them from the extracted source archive — you need **Rust** (see `fluss-rust/rust-toolchain.toml` for the expected version), plus **protobuf** and, for the Python binding, **Python 3.9+**:

```bash
cd fluss-rust
cargo build --workspace --release
```

Per-language verification:

- **Rust:** build from the source release (above), or depend on the RC tag in a throwaway project (`fluss-rs = { git = "https://github.com/apache/fluss", tag = "v${RELEASE_VERSION}-rc${RC_NUM}" }`), then write a few test cases (connect, create table, read/write). Installation: https://fluss.apache.org/docs/apis/rust/installation/
- **Python:** for an RC, install from **TestPyPI** (`pip install -i https://test.pypi.org/simple/ pyfluss==${RELEASE_VERSION}`) and write test cases. Installation: https://fluss.apache.org/docs/apis/python/installation/
- **C++:** build and link the C++ client from `fluss-rust/bindings/cpp/`, then verify. Installation: https://fluss.apache.org/docs/apis/cpp/installation/

The Rust workspace's dependency licenses are checked with [cargo-deny](https://embarkstudios.github.io/cargo-deny/); the release manager regenerates the dependency audit before the release.

## Verifying the Gateway distribution

Extract the Gateway archive on the matching Linux architecture and check its
version, configuration, health endpoint, and graceful shutdown:

```bash
tar -xzf fluss-gateway-${RELEASE_VERSION}-bin-linux-amd64.tgz
cd fluss-gateway-${RELEASE_VERSION}-bin-linux-amd64

bin/fluss-gateway --version
bin/fluss-gateway.sh --bind-address 127.0.0.1:8080 &
GATEWAY_PID=$!

curl --fail --silent --show-error http://127.0.0.1:8080/health

kill -TERM ${GATEWAY_PID}
wait ${GATEWAY_PID}
```

Confirm that the process exits with status `0`, and review `LICENSE`, `NOTICE`,
and `DEPENDENCIES.rust.tsv` against the packaged contents. Repeat the same
verification for the `arm64` archive on an `arm64` Linux host.

The convenience binaries are built with the pinned Rust 1.88 Debian Bookworm
builder and support Debian Bookworm's glibc 2.36 baseline or newer.

Confirm that the release-candidate image contains both architectures:

```bash
docker buildx imagetools inspect \
  apache/fluss-gateway:${RELEASE_VERSION}-rc${RC_NUM}
```

Then verify the image on matching `amd64` and `arm64` Docker hosts:

```bash
docker pull apache/fluss-gateway:${RELEASE_VERSION}-rc${RC_NUM}

# Run the checked-in smoke test from the root of the extracted Fluss source
# release, not from the Gateway binary distribution used above.
cd /path/to/fluss-${RELEASE_VERSION}
GATEWAY_IMAGE=apache/fluss-gateway:${RELEASE_VERSION}-rc${RC_NUM} \
  docker/fluss-gateway/smoke-test.sh
```

## Release artifacts and publish targets

A release publishes to several registries; confirm each one carries the release version:

| Component | Target | Identifier |
|-----------|--------|------------|
| Java / Scala | Maven Central (via Apache Nexus staging) | `org.apache.fluss:fluss-*` |
| Rust | [crates.io](https://crates.io/crates/fluss-rs) | `fluss-rs` |
| Python | [PyPI](https://pypi.org/project/pyfluss/) (RC → [TestPyPI](https://test.pypi.org/project/pyfluss/)) | `pyfluss` |
| C++ | source archive only (no registry) | — |
| Elixir | Hex.pm (post-1.0; not yet published) | `fluss` |
| Gateway | dist.apache.org | `fluss-gateway-<version>-bin-linux-<arch>.tgz` |
| Docker | Docker Hub | `apache/fluss`, `apache/fluss-gateway`, `apache/fluss-quickstart-flink` |

Source archives, signatures, and checksums are on [dist.apache.org](https://dist.apache.org/repos/dist/dev/fluss/) (dev) and, after the vote, on [downloads.apache.org](https://downloads.apache.org/fluss/).

## Testing Against Staged Maven Artifacts

Update the root `pom.xml` of the maven project (like the apache/fluss project) to include the staged repository in the `<repositories>` section. You can do this by adding a new repository entry like this:

```xml
<repositories>
    <repository>
        <id>fluss-staging</id>
        <name>Temporary Staging Repo</name>
        <url>https://repository.apache.org/content/repositories/orgapachefluss-${STAGED_REPO_ID}/</url>
    </repository>
</repositories>
```

And then you can use the staged maven artifacts as dependencies in the project and verify the new dependencies work.


## Testing Features

For any user-facing feature included in a release, we aim to ensure it is functional, usable, and well-documented for the Fluss community.

To support this, release managers can create and assign cross-team testing issues that outline key scenarios to validate. These issues are open to, and encouraged for, all community members to pick up and help verify.

A great way to get started is by walking through the official Quickstart Guide: https://fluss.apache.org/docs/quickstart/flink/ (please switch to the documentation version currently under release).


## Voting

Votes are cast by replying on the vote email on the dev mailing list, with either +1, 0, -1.

In addition to your vote, it’s customary to specify if your vote is binding or non-binding. Only members of the PMC have formally binding votes. If you’re unsure, you can specify that your vote is non-binding. You can find more details on https://www.apache.org/foundation/voting.html.

Besides, it is recommended to include a list of checklist you have verified for your vote. This helps the community to understand what you have checked and what is still missing.
