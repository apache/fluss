---
title: Upgrade Notes
sidebar_position: 4
---

# Upgrade Notes from v0.8 to v0.9

These upgrade notes discuss important aspects, such as configuration, behavior, or dependencies, that changed between Fluss 0.8 and Fluss 0.9. Please read these notes carefully if you are planning to upgrade your Fluss version to 0.9.


## Deprecation / End of Support

### Configuration Options Deprecated

Several configuration options have been deprecated in Fluss 0.9 and replaced with a unified `server.io-pool.size` option. This change simplifies configuration management by consolidating IO thread pool settings across different components.

🔧 **Action Required**: Update your configuration files to use the new option.

#### Deprecated Options

The following options are deprecated and will be removed in a future version:

| Deprecated Option                     | Replacement           | Description                                                      |
|---------------------------------------|-----------------------|------------------------------------------------------------------|
| `coordinator.io-pool.size`            | `server.io-pool.size` | The size of the IO thread pool for coordinator server operations |
| `remote.log.data-transfer-thread-num` | `server.io-pool.size` | The number of threads for transferring remote log files          |
| `kv.snapshot.transfer-thread-num`     | `server.io-pool.size` | The number of threads for transferring KV snapshot files         |

#### Migration Steps

1. **Identify deprecated options in your configuration**:
   - Check your `server.yaml` configuration file for any of the deprecated options listed above

2. **Replace with the unified option**:
   - Remove the deprecated options from your configuration
   - Add or update `server.io-pool.size` with an appropriate value
   - The default value is `10`, which should work for most use cases

#### Benefits of the Change

- **Simplified Configuration**: One option instead of multiple options for IO thread pool management
- **Better Resource Management**: Unified thread pool allows better resource sharing across different IO operations
- **Consistent Behavior**: All IO operations (remote log, KV snapshot, etc.) now use the same thread pool configuration
