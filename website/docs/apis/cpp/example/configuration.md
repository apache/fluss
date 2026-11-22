---
sidebar_position: 2
---
# Configuration

## Connection Setup

```cpp
#include "fluss.hpp"

fluss::Configuration config;
config.bootstrap_servers = "127.0.0.1:9123";

fluss::Connection conn;
fluss::Result result = fluss::Connection::Create(config, conn);

if (!result.Ok()) {
    std::cerr << "Connection failed: " << result.error_message << std::endl;
}
```

## Connection Configurations

All fields have sensible defaults. Only `bootstrap_servers` typically needs to be set.

See the [`Configuration`](../api-reference.md#configuration) section in the API Reference for the full list of configuration fields, types, and defaults.

## SASL Authentication

To connect to a Fluss cluster with SASL/PLAIN authentication enabled:

```cpp
fluss::Configuration config;
config.bootstrap_servers = "127.0.0.1:9123";
config.security_protocol = "sasl";
config.security_sasl_mechanism = "PLAIN";
config.security_sasl_username = "admin";
config.security_sasl_password = "admin-secret";

fluss::Connection conn;
fluss::Result result = fluss::Connection::Create(config, conn);
```
