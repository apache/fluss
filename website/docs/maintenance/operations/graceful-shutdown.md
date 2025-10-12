# Graceful Shutdown

Apache Fluss provides a **comprehensive graceful shutdown mechanism** to ensure data integrity and proper resource cleanup when stopping servers or services.

This guide describes the shutdown procedures, configuration options, and best practices for each Fluss component.

## Overview

Graceful shutdown in Fluss ensures that:
- All ongoing operations complete safely
- Resources are properly released
- Data consistency is maintained
- Network connections are cleanly closed
- Background tasks are terminated properly

These guarantees prevent data corruption and ensure smooth restarts of the system.

## Server Shutdown

### Coordinator Server Shutdown

The **Coordinator Server** uses a multi-stage shutdown process to safely terminate all services in the correct order.
#### Shutdown Process
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

The **Tablet Server** supports a **controlled shutdown process** designed to minimize data unavailability and ensure leadership handover before termination.

**Shutdown Order:**
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

- **Controlled Shutdown Retries**: Number of attempts to transfer leadership (`default:` 3 retries)
- **Retry Interval**: Time between retry attempts (`default`: 1000L)

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
| Issue                | Possible Causes                                                 | Recommended Actions                                                             |
| -------------------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| **Hanging shutdown** | Blocking operations, thread pool misconfiguration, or deadlocks | Check for blocking calls without timeouts, inspect thread dumps                 |
| **Resource leaks**   | Unclosed resources or connections                               | Verify all `AutoCloseable` resources and file handles are closed                |
| **Data loss**        | Unclean shutdown or failed leadership transfer                  | Always use controlled shutdown for Tablet Servers and verify replication factor |

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