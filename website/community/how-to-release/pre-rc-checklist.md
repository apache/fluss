---
title: Pre-RC Checklist
sidebar_position: 1.5
---

# Pre-RC Checklist

Run through this before cutting a release candidate. It catches the problems that are expensive to discover mid-vote, now that a single tag releases Java, Rust, Python, and C++ together.

## Access and secrets

- [ ] Maven Central (Apache Nexus) access for `org.apache.fluss` — see [Release Manager Preparation](release-manager-preparation.md)
- [ ] `CARGO_REGISTRY_TOKEN`, `PYPI_API_TOKEN`, and `TEST_PYPI_API_TOKEN` configured as repository secrets
- [ ] crates.io owner of `fluss-rs`; PyPI maintainer of `pyfluss`
- [ ] GPG key published to the Apache KEYS file

## Build and publish dry-runs

- [ ] `cargo publish -p fluss-rs --dry-run` succeeds (run after `fluss-rust/scripts/vendor-proto.sh` so the proto is vendored)
- [ ] Python wheels + sdist install from **TestPyPI** (the RC tag publishes there):

  ```bash
  pip install -i https://test.pypi.org/simple/ pyfluss==${RELEASE_VERSION}
  ```

- [ ] `fluss-cpp` Bazel build smoke test passes:

  ```bash
  cd fluss-rust/bindings/cpp && bazel build //...
  ```

## Audits

- [ ] `cargo deny check licenses` passes; the Rust dependency list is regenerated and committed
- [ ] Java + Rust + binding CI is green on the release branch
- [ ] `LICENSE` / `NOTICE` cover any third-party content bundled in the source release (including under `fluss-rust/`)

Once these pass, proceed to [Creating a Fluss Release](creating-a-fluss-release.mdx).
