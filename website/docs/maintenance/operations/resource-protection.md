---
title: Resource Protection
sidebar_position: 4
---

# Resource Protection

Fluss includes coordinator- and TabletServer-side guards that protect a cluster from resource exhaustion —
running local disks out of space, or overcommitting memory across KV leader replicas. Each mechanism is
independent, has its own configuration, and can be tuned or disabled separately.

## Data Disk Write Protection

Each TabletServer periodically samples the usage of its local data disk(s) and rejects client writes once usage
crosses a configured threshold, to avoid running a disk out of space. Writes automatically resume once usage drops
back down to a lower recovery threshold.

Only client-driven writes (`appendLog` / `putKv`) are rejected — follower replication always bypasses this check to
preserve replica consistency.

### Enabling and Disabling

Disk write protection is controlled by three options, all set per TabletServer:

```yaml title="conf/server.yaml"
# High-water mark: reject writes once usage reaches this ratio (default 0.85)
server.data-disk.write-limit-ratio: 0.85

# Low-water mark: resume writes once usage drops to or below this ratio (default 0.80)
server.data-disk.write-recover-ratio: 0.80

# How often the local disk usage is sampled (default 30s)
server.data-disk.check-interval: 30s
```

- `server.data-disk.write-limit-ratio` must be strictly greater than `server.data-disk.write-recover-ratio`, and no
  greater than `1.0`. Set it to `1.0` to disable disk-usage protection entirely.
- The gap between the limit and recover ratios provides hysteresis, so a disk usage ratio oscillating right at the
  limit doesn't cause writes to rapidly flip between rejected and accepted.
- `server.data-disk.check-interval` trades off responsiveness against sampling overhead — a shorter interval
  narrows the window during which writes can still land after the disk crosses the limit ratio, at the cost of
  more frequent (in-memory, cheap) disk usage checks.

:::tip
All three options can be updated dynamically without restarting the cluster. Lowering both ratios at the same time
should be done in the same request, or by lowering `server.data-disk.write-recover-ratio` first — otherwise the
intermediate state can be rejected as invalid. See [Updating Configs](updating-configs.md) for how to change
cluster configs at runtime.
:::

### What Happens When a Disk Fills Up

When a TabletServer's disk usage reaches `server.data-disk.write-limit-ratio`, client writes are rejected with a
retriable `DiskWriteLockedException`:

```text
TabletServer <id> has rejected writes because the data disk usage reached <usage>% (limit: <limit>%). Free up space or scale the cluster.
```

Clients can safely retry — writes resume automatically once usage drops back to `server.data-disk.write-recover-ratio`.

While a TabletServer is under disk write protection, it's also a less attractive candidate for new leadership:
`coordinator.offline-leader.retry-delay` governs how long the coordinator waits before retrying leader election on
tablet servers that were recently rejected for temporary conditions like this one, giving the disk time to recover
before the server is considered for leadership again.

### Metrics

Each TabletServer exposes:

| Metric | Description |
|---|---|
| `diskUsageRatio` | The most recently sampled local data disk usage ratio, in `[0.0, 1.0]`. |
| `diskWriteLocked` | `1` if the TabletServer is currently rejecting writes due to disk pressure, `0` otherwise. |

## KV Leader Replica Capacity Protection

Fluss relies mainly on `max.bucket.num` and `max.partition.num` to bound table metadata size. These limits do not
directly protect a cluster from creating too many KV leader replicas, which consume resident memory on
TabletServers (each KV leader replica embeds a RocksDB instance) and can increase the risk of out-of-memory errors.

KV leader replica capacity protection is a coordinator-side admission guard that estimates the cluster's total KV
leader replica capacity from TabletServer memory and rejects new KV table or partition creation once that capacity
would be exceeded.

The feature is **disabled by default** and is considered an advanced/opt-in feature. Enable it once you have a
sense of how much memory each KV leader replica costs in your environment.

### When to Enable It

Consider enabling this feature if:
- Your workload creates KV (primary-key) tables or partitions dynamically, e.g., through auto-partitioning or
  self-service table creation, and you want a hard ceiling to protect TabletServers from OOM.
- You have observed, or want to guard against, TabletServers accumulating more KV leader replicas than their
  available memory can support.

It may be reasonable to leave it disabled if:
- Your cluster has a small, well-known, and stable set of KV tables/partitions where capacity planning is already
  done manually.
- You have not yet measured the actual per-replica memory footprint in your environment — enabling the limit with
  an inaccurate `kv.leader-replica.memory-reserved` value can reject legitimate table/partition creation.

### Enabling and Disabling

The feature is controlled entirely by a single option, `kv.leader-replica.memory-reserved`:

```yaml title="conf/server.yaml"
# Disabled (default)
kv.leader-replica.memory-reserved: 0

# Enabled: reserve an estimated 512MB per KV leader replica
kv.leader-replica.memory-reserved: 512mb
```

- `0` (the default) disables memory-based KV leader replica capacity control entirely.
- A positive value enables it, and is used as the estimated memory cost of a single KV leader replica when
  calculating the cluster-level capacity (see [How Capacity Is Calculated](#how-capacity-is-calculated) below).
- Negative values are invalid and are rejected at both static startup and dynamic reconfiguration time.

:::tip
`kv.leader-replica.memory-reserved` can be updated dynamically without restarting the cluster. See
[Updating Configs](updating-configs.md) for how to change cluster configs at runtime.
:::

### TabletServer Resource Reporting

To estimate cluster-wide capacity, the CoordinatorServer needs to know how much memory each live TabletServer has
available. By default, each TabletServer auto-detects this from cgroup or operating system information and reports
it during registration. CPU capacity is also reported (auto-detected from cgroup CPU quota or the JVM runtime),
but today it is used only for resource reporting — it does not factor into the KV leader replica capacity
calculation.

You can override the detected values explicitly:

```yaml title="conf/server.yaml"
tablet-server.advertised-resource.cpu-cores: 8
tablet-server.advertised-resource.memory-size: 32gb
```

:::note
These options only control what a TabletServer **advertises** to the CoordinatorServer for resource reporting and
KV leader replica capacity estimation. They do not configure JVM heap size, reserve memory, set a cgroup CPU quota,
or enforce any process/container resource limit. `tablet-server.advertised-resource.memory-size` represents total
usable capacity, not current or free memory.
:::

### How Capacity Is Calculated

When `kv.leader-replica.memory-reserved` is set to a positive value, the CoordinatorServer calculates capacity as
follows:

1. Look at all currently live TabletServers.
2. If more than half of them reported a memory value, the limit is **enabled**. Otherwise, the limit is treated as
   disabled — the coordinator does not have enough visibility into cluster memory to estimate capacity safely.
3. For any live TabletServer that did not report memory, the coordinator substitutes the **average memory of the
   TabletServers that did report** a value.
4. The cluster's total estimated memory is the sum of all reported memory plus the substituted average memory for
   unknown servers.
5. `kvLeaderReplicaCapacity = totalEstimatedMemory / kv.leader-replica.memory-reserved`.

For example, with `kv.leader-replica.memory-reserved: 1gb` and 4 live TabletServers where 3 report 32GB each and
one reports no memory:
- 3 out of 4 servers report memory, so the limit is enabled (more than half).
- The unknown server is assumed to have the average of the known servers: 32GB.
- Total estimated memory = 32GB × 4 = 128GB.
- Capacity = 128GB / 1GB = 128 KV leader replicas.

The capacity is recalculated on demand from the current live TabletServer set and is not persisted; it is
recomputed on CoordinatorServer leadership startup and whenever a table or partition creation request is checked.

### What Happens When Capacity Is Exceeded

When the limit is enabled and a KV table or partition creation request (including auto-partition creation) would
push the observed KV leader replica count past the calculated capacity, the request is rejected with an
`INSUFFICIENT_KV_LEADER_REPLICA_CAPACITY` error, surfaced to clients as an
`InsufficientKvLeaderReplicaCapacityException`:

```text
Not enough KV leader replica capacity. observedKvLeaderReplicaCount=<count>, requestedKvLeaderReplicaCount=<count>, kvLeaderReplicaCapacity=<capacity>.
```

If you hit this error, you can:
- Increase capacity by adding TabletServers or more memory per TabletServer.
- Raise `kv.leader-replica.memory-reserved` only if your existing estimate was too conservative — raising it
  without cause lowers the effective capacity.
- Temporarily disable the limit (set `kv.leader-replica.memory-reserved` to `0`) if you need to proceed
  immediately, then re-tune before re-enabling.

### Metrics

The CoordinatorServer exposes two gauges for monitoring:

| Metric | Description |
|---|---|
| `kvLeaderReplicaCount` | The current number of KV leader replicas observed across the cluster. |
| `kvLeaderReplicaCapacity` | The current calculated capacity, or `-1` if the limit is disabled. |

Monitor `kvLeaderReplicaCount` against `kvLeaderReplicaCapacity` to anticipate when a cluster is approaching its
limit before requests start failing.

### Example: Enabling, Tuning, and Disabling

```yaml title="conf/server.yaml"
# Step 1: Enable with an initial conservative estimate per KV leader replica
kv.leader-replica.memory-reserved: 1gb

# Step 2 (optional): Override auto-detected resource reporting on a TabletServer
# with constrained cgroup visibility
tablet-server.advertised-resource.memory-size: 64gb
tablet-server.advertised-resource.cpu-cores: 16

# Step 3: Tune down once real per-replica memory usage is measured
kv.leader-replica.memory-reserved: 256mb

# Step 4: Disable if no longer needed
kv.leader-replica.memory-reserved: 0
```