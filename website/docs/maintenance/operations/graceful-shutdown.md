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

   **Tablet Server Shutdown Order:**
   1. Tablet Server Metric Group → Metric Registry (async)
   2. RPC Server (async) → Tablet Service
   3. ZooKeeper Client → RPC Client → Client Metric Group
   4. Scheduler → KV Manager → Remote Log Manager
   5. Log Manager → Replica Manager
   6. Authorizer → Dynamic Config Manager → Lake Catalog Dynamic Loader
3. **Resource Cleanup**: Executors, connections, and other resources are properly closed

```bash
# Graceful shutdown via SIGTERM
kill -TERM <coordinator-pid>

# Or using the shutdown script (if available)
./bin/stop-coordinator.sh
```

### Tablet Server Shutdown

The Tablet Server supports **controlled shutdown** to minimize data unavailability:

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

## Component-Specific Shutdown

### Executor Services

Fluss uses the `ExecutorUtils` class for graceful executor shutdown:

```java
// Graceful shutdown with timeout
ExecutorUtils.gracefulShutdown(timeout, TimeUnit.SECONDS, executorService);

// Non-blocking shutdown
CompletableFuture<Void> shutdownFuture = 
    ExecutorUtils.nonBlockingShutdown(timeout, TimeUnit.SECONDS, executorService);
```

**Shutdown Process**:
1. Call `shutdown()` to stop accepting new tasks
2. Wait for existing tasks to complete within timeout
3. Force termination with `shutdownNow()` if timeout exceeded

### Network Components

#### RPC Server Shutdown

The Netty-based RPC server implements asynchronous shutdown:

```java
CompletableFuture<Void> shutdownFuture = rpcServer.closeAsync();
```

**Shutdown Steps**:
1. Stop accepting new connections
2. Close existing channels gracefully
3. Shutdown event loop groups
4. Release worker pools

#### Event Loop Groups

Network event loops are shut down using Netty's graceful shutdown:

```java
group.shutdownGracefully()
    .addListener(finished -> {
        // Handle completion
    });
```

### Remote Log Manager

For components with thread pools, Fluss follows the standard Java pattern:

1. **Disable New Tasks**: Call `shutdown()` to prevent new task submission
2. **Wait for Completion**: Use `awaitTermination()` with timeout
3. **Force Cancellation**: Call `shutdownNow()` if tasks don't complete
4. **Handle Interruption**: Properly handle `InterruptedException`

## Best Practices

### 1. Use Shutdown Hooks

Register shutdown hooks for critical services to ensure cleanup on JVM termination:

```java
Thread shutdownHook = ShutdownHookUtil.addShutdownHook(
    service, 
    "ServiceName", 
    logger
);
```

### 2. Implement Timeout Handling

Always specify timeouts for shutdown operations to prevent indefinite blocking:

```java
// Good: With timeout
ExecutorUtils.gracefulShutdown(30, TimeUnit.SECONDS, executor);

// Avoid: Without timeout (may block indefinitely)
executor.shutdown();
executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
```

### 3. Order of Shutdown

Shut down components in reverse order of their dependencies:

1. Stop accepting new requests
2. Complete ongoing operations
3. Close client connections
4. Shutdown background services
5. Release system resources

### 4. Handle Exceptions

Properly handle exceptions during shutdown to ensure all cleanup steps execute:

```java
Throwable exception = null;
try {
    // Shutdown component 1
} catch (Throwable t) {
    exception = ExceptionUtils.firstOrSuppressed(t, exception);
}
try {
    // Shutdown component 2
} catch (Throwable t) {
    exception = ExceptionUtils.firstOrSuppressed(t, exception);
}
// Continue for all components...
```

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