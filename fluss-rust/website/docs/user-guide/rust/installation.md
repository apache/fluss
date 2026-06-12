---
sidebar_position: 1
---
# Installation

The Fluss Rust client is published to [crates.io](https://crates.io/crates/fluss-rs) as `fluss-rs`. The crate's library name is `fluss`, so you import it with `use fluss::...`.

```toml
[dependencies]
fluss-rs = "0.1.0"
tokio = { version = "1", features = ["full"] }
```

## Feature Flags

```toml
[dependencies]
# Default: memory and filesystem storage
fluss-rs = "0.1.0"

# With S3 storage support
fluss-rs = { version = "0.1", features = ["storage-s3"] }

# With OSS storage support
fluss-rs = { version = "0.1", features = ["storage-oss"] }

# All storage backends
fluss-rs = { version = "0.1", features = ["storage-all"] }
```

Available features:
- `storage-memory` (default: In-memory storage)
- `storage-fs` (default: Local filesystem storage)
- `storage-s3` (Amazon S3 storage)
- `storage-oss` (Alibaba OSS storage)
- `storage-all` (All storage backends)

## Git or Path Dependency

For development against unreleased changes:

```toml
[dependencies]
# From Git
fluss = { git = "https://github.com/apache/fluss-rust.git", package = "fluss-rs" }

# From local path
fluss = { path = "/path/to/fluss-rust/crates/fluss", package = "fluss-rs" }
```

> **Note:** When using `git` or `path` dependencies, the `package = "fluss-rs"` field is required so that Cargo resolves the correct package while still allowing `use fluss::...` imports.

## Building from Source

**Prerequisites:** Rust 1.85+, Protobuf compiler (`protoc`)

```bash
git clone https://github.com/apache/fluss-rust.git
cd fluss-rust
```

Install `protoc`:

```bash
# macOS
brew install protobuf

# Ubuntu/Debian
sudo apt-get install protobuf-compiler
```

Build:

```bash
cargo build --workspace --all-targets
```
