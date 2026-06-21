# How to Verify a Release Candidate

This document describes how to verify a release candidate (RC) of the **Fluss clients** (fluss-rust, fluss-python, fluss-cpp) from the [fluss-rust](https://github.com/apache/fluss-rust) repository. It is intended for anyone participating in the release vote (binding or non-binding) and is based on [Verifying a Fluss Release](https://fluss.apache.org/community/how-to-release/verifying-a-fluss-release/) of the Apache Fluss project, adapted for the fluss-rust source distribution and tooling (Rust, Python, C++).

## Validating distributions

The release vote email includes links to:

- **Distribution archive:** source tarball (`fluss-rust-${RELEASE_VERSION}-incubating.tgz`) on [dist.apache.org dev](https://dist.apache.org/repos/dist/dev/incubator/fluss/)
- **Signature file:** `fluss-rust-${RELEASE_VERSION}-incubating.tgz.asc`
- **Checksum file:** `fluss-rust-${RELEASE_VERSION}-incubating.tgz.sha512`
- **KEYS file:** [https://downloads.apache.org/incubator/fluss/KEYS](https://downloads.apache.org/incubator/fluss/KEYS)

Download the archive (`.tgz`), `.asc`, and `.sha512` from the RC directory (e.g. `fluss-rust-0.1.0-rc1/`) and the KEYS file. Then follow the steps below to verify signatures and checksums.

## Verifying signatures

First, import the keys into your local keyring:

```bash
curl https://downloads.apache.org/incubator/fluss/KEYS -o KEYS
gpg --import KEYS
```

Next, verify all `.asc` files:

```bash
for i in *.tgz; do echo $i; gpg --verify $i.asc $i; done
```

If verification succeeds, you will see a message like:

```text
gpg: Signature made ...
gpg: using RSA key ...
gpg: Good signature from "Release Manager Name (CODE SIGNING KEY) <...@apache.org>"
```

## Verifying checksums

Next, verify the tarball(s) using the provided `.sha512` file(s). Each `.sha512` file lists the expected SHA-512 hash for the corresponding archive; `-c` reads that file and checks the archive.

**On macOS (shasum):**

```bash
shasum -a 512 -c fluss-rust-${RELEASE_VERSION}-incubating.tgz.sha512
```

**On Linux (sha512sum):**

```bash
sha512sum -c fluss-rust-${RELEASE_VERSION}-incubating.tgz.sha512
```

If you have multiple archives, run `-c` on each `.sha512` file (or use `shasum -a 512 -c *.sha512` / `sha512sum -c *.sha512`).

If the verification is successful, you will see a message like this:

```text
fluss-rust-0.1.0-incubating.tgz: OK
```

## Verifying build

Extract the source release archive and verify that it builds (and optionally that tests pass). You need **Rust** (see [rust-toolchain.toml](https://github.com/apache/fluss-rust/blob/main/rust-toolchain.toml) for the expected version) and, for full builds, **protobuf** and **Python 3.9+** for bindings.

```bash
tar -xzf fluss-rust-${RELEASE_VERSION}-incubating.tgz
cd fluss-rust-${RELEASE_VERSION}-incubating
```

Build the workspace:

```bash
cargo build --workspace --release
```

For Python bindings, see the project [README](https://github.com/apache/fluss-rust#readme) and [Development Guide](https://github.com/apache/fluss-rust/blob/main/DEVELOPMENT.md). For C++ bindings, see `bindings/cpp/`.

## Verifying LICENSE and NOTICE

Unzip the source release archive and verify that:

1. The **LICENSE** and **NOTICE** files in the root directory are correct and refer to dependencies in the source release (e.g. files in the repository such as fonts, CSS, JavaScript, images).
2. All files that need it have ASF license headers.
3. All dependencies have been checked for their license and the license is ASL 2.0 compatible ([ASF third-party license policy](http://www.apache.org/legal/resolved.html#category-x)).
4. Compatible non-ASL 2.0 licenses are documented (e.g. in NOTICE or in dependency audit files such as `DEPENDENCIES*.tsv`).

The project uses [cargo-deny](https://embarkstudios.github.io/cargo-deny/) for license checks; see [Creating a Fluss Rust Client Release](create-release.md) for how the dependency list is generated before a release.

## Testing features

For any user-facing feature included in a release, we aim to ensure it is functional, usable, and well-documented. Release managers may create testing issues that outline key scenarios to validate; these are open to all community members.

**Per-language verification:** For **Rust** and **C++**, build from the source release and write your own test cases to verify. For **Python**, the RC is published to **TestPyPI**; install the client from TestPyPI and write your own test cases (e.g. connect, create table, read/write) to verify. Use the README in each component as the entry point:

- **Rust client:** You can depend on the RC via its git tag (e.g. in your `Cargo.toml`: `fluss-rs = { git = "https://github.com/apache/fluss-rust", tag = "v${RELEASE_VERSION}-rc${RC_NUM}" }`) and build your own test project to verify. Alternatively, build from the source release; see [Rust Installation Guide](../user-guide/rust/installation.md).
- **Python bindings:** See [Python Installation Guide](../user-guide/python/installation.md) for how to add the Python client (for an RC, install from **TestPyPI**: `pip install -i https://test.pypi.org/simple/ pyfluss==${RELEASE_VERSION}`); then write test cases to verify.
- **C++ bindings:** See [C++ Installation Guide](../user-guide/cpp/installation.md) for how to build and link the C++ client; then write test cases to verify.

## Incubator release checklist

If the project is in incubation, the ASF Incubator provides a release checklist. You can refer to it when verifying the release:

- [Incubator Release Checklist](https://cwiki.apache.org/confluence/display/INCUBATOR/Incubator+Release+Checklist)

## Voting

Votes are cast by replying to the vote email on the dev mailing list with **+1**, **0**, or **-1**.

In addition to your vote, it is customary to state whether your vote is **binding** or **non-binding**. Only members of the PPMC and mentors have formally binding votes (and the IPMC on the Incubator general list). If unsure, you can state that your vote is non-binding. See [Apache Foundation Voting](https://www.apache.org/foundation/voting.html).

It is recommended to include a short list of what you verified (e.g. signatures, checksums, build, tests, LICENSE/NOTICE). This helps the community see what has been checked and what might still be missing.

**Checklist you can reference in your vote:**

- [ ] [Validating distributions](#validating-distributions)
- [ ] [Verifying signatures](#verifying-signatures)
- [ ] [Verifying checksums](#verifying-checksums)
- [ ] [Verifying build](#verifying-build)
- [ ] [Verifying LICENSE and NOTICE](#verifying-license-and-notice)
- [ ] [Testing features](#testing-features)
- [ ] [Incubator release checklist](#incubator-release-checklist) (if applicable)

