---
sidebar_position: 4
---
# Error Handling

The Fluss Rust client uses a unified `Error` type and a `Result<T>` alias for all fallible operations.

## Basic Usage

```rust
use fluss::error::{Error, Result};

// All operations return Result<T>
let conn = FlussConnection::new(config).await?;
let admin = conn.get_admin()?;
let table = conn.get_table(&table_path).await?;
```

Use the `?` operator to propagate errors, or `match` on specific variants for fine-grained handling.

## Matching Error Variants

```rust
use fluss::error::Error;

match result {
    Ok(val) => {
        // handle success
    }
    Err(Error::RpcError { message, .. }) => {
        eprintln!("RPC failure: {}", message);
    }
    Err(Error::UnsupportedOperation { message }) => {
        eprintln!("Unsupported: {}", message);
    }
    Err(Error::FlussAPIError { api_error }) => {
        eprintln!("Server error: {}", api_error);
    }
    Err(e) => {
        eprintln!("Unexpected error: {}", e);
    }
}
```

## Error Variants

| Variant                        | Description                                                  |
|--------------------------------|--------------------------------------------------------------|
| `UnexpectedError`              | General unexpected errors with a message and optional source |
| `IoUnexpectedError`            | I/O errors (network, file system)                            |
| `RemoteStorageUnexpectedError` | Remote storage errors (OpenDAL backend failures)             |
| `RpcError`                     | RPC communication failures (connection refused, timeout)     |
| `RowConvertError`              | Row conversion failures (type mismatch, invalid data)        |
| `ArrowError`                   | Arrow data handling errors (schema mismatch, encoding)       |
| `IllegalArgument`              | Invalid arguments passed to an API method                    |
| `UnsupportedOperation`         | Operation not supported on the table type                    |
| `FlussAPIError`                | Server-side API errors returned by the Fluss cluster         |

Server side errors are represented as `FlussAPIError` with a specific error code. Use the `api_error()` helper to match them ergonomically:

```rust
use fluss::error::FlussError;

match result {
    Err(ref e) if e.api_error() == Some(FlussError::InvalidTableException) => {
        eprintln!("Invalid table: {}", e);
    }
    Err(ref e) if e.api_error() == Some(FlussError::PartitionNotExists) => {
        eprintln!("Partition does not exist: {}", e);
    }
    Err(ref e) if e.api_error() == Some(FlussError::LeaderNotAvailableException) => {
        eprintln!("Leader not available: {}", e);
    }
    Err(ref e) if e.api_error() == Some(FlussError::AuthenticateException) => {
        eprintln!("Authentication failed: {}", e);
    }
    _ => {}
}
```

## Retry Logic

Some errors are transient, where the server may be temporarily unavailable, mid-election, or under load. `is_retriable()` can be used for deciding to retry an operation rather than treating the error as permanent.

`Error::is_retriable()` is available directly on any `Error` value. `RpcError` is always retriable; `FlussAPIError` delegates to the server error code; all other variants return `false`.

```rust
use fluss::error::Error;

match writer.append(&row) {
    Ok(_) => {}
    Err(ref e) if e.is_retriable() => {
        // Transient failure — safe to retry
    }
    Err(e) => {
        // Permanent failure — log and abort
        eprintln!("Fatal error: {}", e);
    }
}
```

### Retriable Variants

| Variant / Error                              | Code | Reason                                    |
|----------------------------------------------|------|-------------------------------------------|
| `Error::RpcError`                            | —    | Network-level failure, always retriable   |
| `FlussError::NetworkException`               | 1    | Server disconnected                       |
| `FlussError::CorruptMessage`                 | 3    | CRC or size error                         |
| `FlussError::SchemaNotExist`                 | 9    | Schema may not exist                      |
| `FlussError::LogStorageException`            | 10   | Transient log storage error               |
| `FlussError::KvStorageException`             | 11   | Transient KV storage error                |
| `FlussError::NotLeaderOrFollower`            | 12   | Leader election in progress               |
| `FlussError::CorruptRecordException`         | 14   | Corrupt record                            |
| `FlussError::UnknownTableOrBucketException`  | 21   | Metadata not yet available                |
| `FlussError::RequestTimeOut`                 | 25   | Request timed out                         |
| `FlussError::StorageException`               | 26   | Transient storage error                   |
| `FlussError::NotEnoughReplicasAfterAppendException` | 28 | Wrote to server but with low ISR size |
| `FlussError::NotEnoughReplicasException`     | 29   | Low ISR size at write time                |
| `FlussError::LeaderNotAvailableException`    | 44   | No leader available for partition         |

All other `Error` variants (e.g. `RowConvertError`, `IllegalArgument`, `UnsupportedOperation`) always return `false` from `is_retriable()`.

## Common Error Scenarios

### Connection Refused

The Fluss cluster is not running or the address is incorrect.

```rust
let result = FlussConnection::new(config).await;
match result {
    Err(Error::RpcError { message, .. }) => {
        eprintln!("Cannot connect to cluster: {}", message);
    }
    _ => {}
}
```

### Table Not Found

The table does not exist or has been dropped.

```rust
use fluss::error::{Error, FlussError};

// Admin operations return FlussError::TableNotExist (code 7)
let result = admin.drop_table(&table_path, false).await;
match result {
    Err(ref e) if e.api_error() == Some(FlussError::TableNotExist) => {
        eprintln!("Table not found: {}", e);
    }
    _ => {}
}

// conn.get_table() wraps the error differently, match on FlussAPIError directly
let result = conn.get_table(&table_path).await;
match result {
    Err(Error::FlussAPIError { ref api_error }) => {
        eprintln!("Server error (code {}): {}", api_error.code, api_error.message);
    }
    _ => {}
}
```

### Partition Not Found

The partition does not exist on a partitioned table.

```rust
use fluss::error::FlussError;

let result = admin.drop_partition(&table_path, &spec, false).await;
match result {
    Err(ref e) if e.api_error() == Some(FlussError::PartitionNotExists) => {
        eprintln!("Partition does not exist: {}", e);
    }
    _ => {}
}
```

### Authentication Failed

SASL credentials are incorrect or the user does not exist.

```rust
use fluss::error::{Error, FlussError};

let result = FlussConnection::new(config).await;
match result {
    Err(ref e) if e.api_error() == Some(FlussError::AuthenticateException) => {
        eprintln!("Authentication failed: {}", e);
    }
    _ => {}
}
```

### Schema Mismatch

Row data does not match the expected table schema.

```rust
let result = writer.append(&row);
match result {
    Err(Error::RowConvertError { .. }) => {
        eprintln!("Row does not match table schema");
    }
    _ => {}
}
```

## Using `Result<T>` in Application Code

The `fluss::error::Result<T>` type alias makes it easy to use Fluss errors with the `?` operator in your application functions:

```rust
use fluss::error::Result;

async fn my_pipeline() -> Result<()> {
    let conn = FlussConnection::new(config).await?;
    let admin = conn.get_admin()?;
    let table = conn.get_table(&table_path).await?;
    let writer = table.new_append()?.create_writer()?;
    writer.append(&row)?;
    writer.flush().await?;
    Ok(())
}
```

For applications that use other error types alongside Fluss errors, you can convert with standard `From` / `Into` traits or use crates like `anyhow`:

```rust
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let conn = FlussConnection::new(config).await?;
    // fluss::error::Error implements std::error::Error,
    // so it converts into anyhow::Error automatically
    Ok(())
}
```
