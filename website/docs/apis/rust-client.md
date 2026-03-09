---
title: "Rust Client"
sidebar_position: 4
---

# Fluss Rust Client

The Fluss Rust Client is a high-performance, asynchronous library powered by the
[Tokio](https://tokio.rs/) runtime. It provides a native interface for interacting
with Fluss clusters with minimal overhead.

The client provides two main APIs:

- **Admin API**: For managing databases, tables, and partitions.
- **Table API**: For high-throughput reading (Scanners/Lookups) and writing (Writers).

## Installation

The Fluss Rust client is published to [crates.io](https://crates.io/crates/fluss-rs)
as `fluss-rs`. The crate's library name is `fluss`, so you import it with `use fluss::...`.

Add the following to your `Cargo.toml`:
```toml
[dependencies]
fluss-rs = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Quick Example
```rust
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::default();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();

    let conn = FlussConnection::new(config).await?;
    let admin = conn.get_admin().await?;

    Ok(())
}
```

## Full Documentation

For the complete Rust client reference including all configuration options,
API methods, data types, error handling, and worked examples — see the
**[Fluss Rust Client documentation](https://clients.fluss.apache.org/user-guide/rust/installation)**.