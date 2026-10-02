---
sidebar_position: 4
---
# Error Handling

All C++ client operations return a `fluss::Result` struct instead of throwing exceptions. This gives you explicit control over error handling.

## The `Result` Struct

```cpp
#include "fluss.hpp"

// All operations return fluss::Result
fluss::Result result = admin.CreateTable(path, descriptor);
if (!result.Ok()) {
    std::cerr << "Error code: " << result.error_code << std::endl;
    std::cerr << "Error message: " << result.error_message << std::endl;
}
```

| Field / Method   | Type          | Description                               |
|------------------|---------------|-------------------------------------------|
| `error_code`     | `int32_t`     | 0 for success, non-zero for errors        |
| `error_message`  | `std::string` | Human-readable error description          |
| `Ok()`           | `bool`        | Returns `true` if the operation succeeded |

## Handling Errors

Check the `Result` after each operation and decide how to respond, e.g. log and continue, retry, or abort:

```cpp
fluss::Connection conn;
fluss::Result result = fluss::Connection::Create(config, conn);
if (!result.Ok()) {
    // Log, retry, or propagate the error as appropriate
    std::cerr << "Connection failed (code " << result.error_code
              << "): " << result.error_message << std::endl;
    return 1;
}
```

## Connection State Checking

Use `Available()` to verify that a connection or object is valid before using it:

```cpp
fluss::Connection conn;
if (!conn.Available()) {
    // Connection not initialized or already moved
}

fluss::Configuration config;
config.bootstrap_servers = "127.0.0.1:9123";
fluss::Result result = fluss::Connection::Create(config, conn);
if (result.Ok() && conn.Available()) {
    // Connection is ready to use
}
```

## Error Codes

Server-side errors carry a specific error code (>0 or -1). Client-side errors (connection failures, type mismatches, etc.) use `ErrorCode::CLIENT_ERROR` (-2). Use `fluss::ErrorCode` to match on specific codes:

```cpp
fluss::Result result = admin.DropTable(table_path);
if (!result.Ok()) {
    if (result.error_code == fluss::ErrorCode::TABLE_NOT_EXIST) {
        std::cerr << "Table does not exist" << std::endl;
    } else if (result.error_code == fluss::ErrorCode::PARTITION_NOT_EXISTS) {
        std::cerr << "Partition does not exist" << std::endl;
    } else if (result.error_code == fluss::ErrorCode::CLIENT_ERROR) {
        std::cerr << "Client-side error: " << result.error_message << std::endl;
    } else {
        std::cerr << "Server error (code " << result.error_code
                  << "): " << result.error_message << std::endl;
    }
}
```

### Common Error Codes

| Constant                                      | Code | Description                         |
|-----------------------------------------------|------|-------------------------------------|
| `ErrorCode::CLIENT_ERROR`                     | -2   | Client-side error (not from server) |
| `ErrorCode::UNKNOWN_SERVER_ERROR`             | -1   | Unexpected server error             |
| `ErrorCode::NETWORK_EXCEPTION`                | 1    | Server disconnected before response |
| `ErrorCode::DATABASE_NOT_EXIST`               | 4    | Database does not exist             |
| `ErrorCode::DATABASE_ALREADY_EXIST`           | 6    | Database already exists             |
| `ErrorCode::TABLE_NOT_EXIST`                  | 7    | Table does not exist                |
| `ErrorCode::TABLE_ALREADY_EXIST`              | 8    | Table already exists                |
| `ErrorCode::INVALID_TABLE_EXCEPTION`          | 15   | Invalid table operation             |
| `ErrorCode::REQUEST_TIME_OUT`                 | 25   | Request timed out                   |
| `ErrorCode::PARTITION_NOT_EXISTS`             | 36   | Partition does not exist            |
| `ErrorCode::PARTITION_ALREADY_EXISTS`         | 42   | Partition already exists            |
| `ErrorCode::PARTITION_SPEC_INVALID_EXCEPTION` | 43   | Invalid partition spec              |
| `ErrorCode::LEADER_NOT_AVAILABLE_EXCEPTION`   | 44   | No leader available for partition   |
| `ErrorCode::AUTHENTICATE_EXCEPTION`           | 46   | Authentication failed (bad credentials) |

See `fluss::ErrorCode` in `fluss.hpp` for the full list of named constants.

## Retry Logic

Some errors are transient, where the server may be temporarily unavailable, mid-election, or under load. `IsRetriable()` can be used for deciding to to retry an operation rather than treating the error as permanent.

`ErrorCode::IsRetriable(int32_t code)` is a static helper available directly on the error code:

```cpp
fluss::Result result = writer.Append(row);
if (!result.Ok()) {
    if (result.IsRetriable()) {
        // Transient failure — safe to retry 
    } else {
        // Permanent failure — log and abort
        std::cerr << "Fatal error (code " << result.error_code
                  << "): " << result.error_message << std::endl;
    }
}
```

`Result::IsRetriable()` delegates to `ErrorCode::IsRetriable()`, so you can also call it directly on the code:

```cpp
if (fluss::ErrorCode::IsRetriable(result.error_code)) {
    // retry
}
```

### Retriable Error Codes

| Constant                                                    | Code | Reason                                    |
|-------------------------------------------------------------|------|-------------------------------------------|
| `ErrorCode::NETWORK_EXCEPTION`                          | 1    | Server disconnected                       |
| `ErrorCode::CORRUPT_MESSAGE`                            | 3    | CRC or size error                         |
| `ErrorCode::SCHEMA_NOT_EXIST`                           | 9    | Schema may not exist                      |
| `ErrorCode::LOG_STORAGE_EXCEPTION`                      | 10   | Transient log storage error               |
| `ErrorCode::KV_STORAGE_EXCEPTION`                       | 11   | Transient KV storage error                |
| `ErrorCode::NOT_LEADER_OR_FOLLOWER`                     | 12   | Leader election in progress               |
| `ErrorCode::CORRUPT_RECORD_EXCEPTION`                   | 14   | Corrupt record                            |
| `ErrorCode::UNKNOWN_TABLE_OR_BUCKET_EXCEPTION`          | 21   | Metadata not yet available                |
| `ErrorCode::REQUEST_TIME_OUT`                           | 25   | Request timed out                         |
| `ErrorCode::STORAGE_EXCEPTION`                          | 26   | Transient storage error                   |
| `ErrorCode::NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION` | 28   | Wrote to server but with low ISR size     |
| `ErrorCode::NOT_ENOUGH_REPLICAS_EXCEPTION`              | 29   | Low ISR size at write time                |
| `ErrorCode::LEADER_NOT_AVAILABLE_EXCEPTION`             | 44   | No leader available for partition         |

Client-side errors (`ErrorCode::CLIENT_ERROR`, code -2) always return `false` from `IsRetriable()`.

## Common Error Scenarios

### Connection Refused

The cluster is not running or the address is incorrect:

```cpp
fluss::Configuration config;
config.bootstrap_servers = "127.0.0.1:9123";
fluss::Connection conn;
fluss::Result result = fluss::Connection::Create(config, conn);
if (!result.Ok()) {
    // "Connection refused" or timeout error
    std::cerr << "Cannot connect to cluster: " << result.error_message << std::endl;
}
```

### Table Not Found

Attempting to access a table that does not exist:

```cpp
fluss::Table table;
fluss::Result result = conn.GetTable(fluss::TablePath("fluss", "nonexistent"), table);
if (!result.Ok()) {
    if (result.error_code == fluss::ErrorCode::TABLE_NOT_EXIST) {
        std::cerr << "Table not found" << std::endl;
    }
}
```

### Partition Not Found

Writing to a partitioned primary key table before creating partitions:

```cpp
// This will fail if partitions are not created first
auto row = table.NewRow();
row.Set("user_id", 1);
row.Set("region", "US");
row.Set("score", static_cast<int64_t>(100));
fluss::WriteResult wr;
fluss::Result result = writer.Upsert(row, wr);
if (!result.Ok()) {
    if (result.error_code == fluss::ErrorCode::PARTITION_NOT_EXISTS) {
        std::cerr << "Partition not found, create partitions before writing" << std::endl;
    }
}
```

### Authentication Failed

SASL credentials are incorrect or the user does not exist:

```cpp
fluss::Configuration config;
config.bootstrap_servers = "127.0.0.1:9123";
config.security_protocol = "sasl";
config.security_sasl_username = "admin";
config.security_sasl_password = "wrong-password";

fluss::Connection conn;
fluss::Result result = fluss::Connection::Create(config, conn);
if (!result.Ok()) {
    if (result.error_code == fluss::ErrorCode::AUTHENTICATE_EXCEPTION) {
        std::cerr << "Authentication failed: " << result.error_message << std::endl;
    }
}
```

### Schema Mismatch

Using incorrect types or column indices when writing:

```cpp
fluss::GenericRow row;
// Setting wrong type for a column will result in an error
// when the row is sent to the server
row.SetString(0, "not_an_integer");  // Column 0 expects Int
fluss::Result result = writer.Append(row);
if (!result.Ok()) {
    std::cerr << "Schema mismatch: " << result.error_message << std::endl;
}
```

## Best Practices

1. **Always check `Result`**: Never ignore the return value of operations that return `Result`.
2. **Handle errors gracefully**: Log errors and retry or fail gracefully rather than crashing.
3. **Verify connection state**: Use `Available()` to check connection validity before operations.
4. **Create partitions before writing**: For partitioned primary key tables, always create partitions before attempting upserts.
