---
sidebar_position: 2
---
# Configuration

## Connection Setup

```rust
use fluss::client::FlussConnection;
use fluss::config::Config;

let mut config = Config::default();
config.bootstrap_servers = "127.0.0.1:9123".to_string();

let conn = FlussConnection::new(config).await?;
```

## Connection Configurations

See the [`Config`](../api-reference.md#config) section in the API Reference for the full list of configuration options, types, and defaults.

## SASL Authentication

To connect to a Fluss cluster with SASL/PLAIN authentication enabled:

```rust
let mut config = Config::default();
config.bootstrap_servers = "127.0.0.1:9123".to_string();
config.security_protocol = "sasl".to_string();
config.security_sasl_mechanism = "PLAIN".to_string();
config.security_sasl_username = "admin".to_string();
config.security_sasl_password = "admin-secret".to_string();

let conn = FlussConnection::new(config).await?;
```
