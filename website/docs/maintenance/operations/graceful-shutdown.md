# Graceful Shutdown

Fluss provides comprehensive graceful shutdown mechanisms to ensure data integrity and proper resource cleanup when stopping servers or services. This document covers the various shutdown procedures and best practices for different Fluss components.

## Overview

Graceful shutdown in Fluss ensures that:
- All ongoing operations complete safely
- Resources are properly released
- Data consistency is maintained
- Network connections are cleanly closed
- Background tasks are terminated properly

## Server Shutdown

### Coordinator Server Shutdown

The Coordinator Server implements a multi-stage shutdown process:

1. **Shutdown Hook Registration**: The server registers a JVM shutdown hook that triggers graceful shutdown on process termination
2. **Service Termination**: All services are stopped in a specific order to maintain consistency:

   **Coordinator Server Shutdown Order:**
   1. Server Metric Group → Metric Registry (async)
   2. Auto Partition Manager → IO Executor (5s timeout)
   3. Coordinator Event Processor → Coordinator Channel Manager
   4. RPC Server (async) → Coordinator Service
   5. Coordinator Context → Lake Table Tiering Manager
   6. ZooKeeper Client → Authorizer
   7. Dynamic Config Manager → Lake Catalog Dynamic Loader
   8. RPC Client → Client Metric Group

3. **Resource Cleanup**: Executors, connections, and other resources are properly closed

```bash
# Graceful shutdown via SIGTERM
kill -TERM <coordinator-pid>

# Or using the shutdown script (if available)
./bin/stop-coordinator.sh
```

### Tablet Server Shutdown

The Tablet Server supports **controlled shutdown** to minimize data unavailability. The shutdown process ensures that all services are stopped in a specific order to maintain consistency:

   **Tablet Server Shutdown Order:**
   1. Tablet Server Metric Group → Metric Registry (async)
   2. RPC Server (async) → Tablet Service
   3. ZooKeeper Client → RPC Client → Client Metric Group
   4. Scheduler → KV Manager → Remote Log Manager
   5. Log Manager → Replica Manager
   6. Authorizer → Dynamic Config Manager → Lake Catalog Dynamic Loader

#### Controlled Shutdown Process

1. **Leadership Transfer**: The server attempts to transfer leadership of all buckets it leads to other replicas
2. **Retry Logic**: If leadership transfer fails, the server retries with configurable intervals
3. **Timeout Handling**: After maximum retries, the server proceeds with unclean shutdown if necessary

```bash
# Initiate controlled shutdown
kill -TERM <tablet-server-pid>
```

#### Configuration Options

- **Controlled Shutdown Retries**: Number of attempts to transfer leadership
- **Retry Interval**: Time between retry attempts (default: configurable via `CONTROLLED_SHUTDOWN_RETRY_INTERVAL_MS`)

## Monitoring Shutdown

### Logging

Fluss provides detailed logging during shutdown processes:

- **INFO**: Normal shutdown progress
- **WARN**: Retry attempts or timeout warnings
- **ERROR**: Shutdown failures or exceptions

### Metrics

Monitor shutdown-related metrics:

- Shutdown duration
- Failed shutdown attempts
- Resource cleanup status

## Troubleshooting

### Common Issues

1. **Hanging Shutdown**: 
   - Check for blocking operations without timeouts
   - Verify thread pool configurations
   - Look for deadlocks in application code

2. **Resource Leaks**:
   - Ensure all `AutoCloseable` resources are properly closed
   - Check for unclosed network connections
   - Verify file handle cleanup

3. **Data Loss**:
   - Use controlled shutdown for Tablet Servers
   - Ensure proper leadership transfer
   - Verify replication factor settings

### Debug Steps

1. Enable debug logging for shutdown components
2. Monitor JVM thread dumps during shutdown
3. Check system resource usage
4. Verify network connection states

## Configuration Reference

| Configuration | Description | Default |
|---------------|-------------|---------|
| `controlled.shutdown.max.retries` | Maximum retries for controlled shutdown | 3 |
| `controlled.shutdown.retry.interval.ms` | Interval between retry attempts | 5000 |
| `shutdown.timeout.ms` | General shutdown timeout | 30000 |

## See Also

- [Configuration](../configuration.md)
- [Monitoring and Observability](../observability/monitor-metrics.md)
- [Upgrading Fluss](upgrading.md)