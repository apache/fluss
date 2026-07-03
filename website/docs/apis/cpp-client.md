---
title: "C++ Client"
sidebar_position: 4
---

# Fluss C++ Client

The Fluss C++ Client provides a high-performance, synchronous interface for
interacting with Fluss clusters. It manages an internal Tokio runtime and
supports Apache Arrow for efficient data interchange.

The client provides two main APIs:

- **[Admin API](https://clients.fluss.apache.org/user-guide/cpp/api-reference#admin)**: For managing databases, tables, and partitions.
- **[Table API](https://clients.fluss.apache.org/user-guide/cpp/api-reference#table)**: For reading and writing to Log and Primary Key tables.

## Installation

The C++ client is not yet published as a package and must be built from source.

**Prerequisites:** CMake 3.22+, C++17 compiler, Rust 1.85+, Apache Arrow C++ library

Install dependencies:
```bash
# macOS
brew install cmake arrow

# Ubuntu/Debian
sudo apt-get install cmake libarrow-dev
```
```bash
git clone https://github.com/apache/fluss-rust.git
cd fluss-rust/bindings/cpp
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
```

For full build options including CMake integration into your own project, see the
[C++ client installation guide](https://clients.fluss.apache.org/user-guide/cpp/installation).

## Quick Example
```cpp
#include "fluss.hpp"

int main() {
    fluss::Configuration config;
    config.bootstrap_servers = "127.0.0.1:9123";

    fluss::Connection conn;
    fluss::Result result = fluss::Connection::Create(config, conn);
    if (!result.Ok()) {
        std::cerr << "Connection failed: " << result.error_message << std::endl;
        return 1;
    }

    fluss::Admin admin;
    conn.GetAdmin(admin);

    return 0;
}
```

For more examples, see the [Fluss C++ Client documentation](https://clients.fluss.apache.org/user-guide/cpp/example/).

## Full Documentation

For the complete C++ client reference including all configuration options,
API methods, data types, error handling, and worked examples — see the
**[Fluss C++ Client documentation](https://clients.fluss.apache.org/user-guide/cpp/installation)**.