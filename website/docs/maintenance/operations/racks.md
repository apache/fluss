---
title: Racks
sidebar_position: 2
---

# Balancing Replicas Across Racks

## Overview

Rack awareness is a feature designed to distribute replicas of the same bucket across multiple physical racks. This extends Fluss's data protection guarantees beyond server failures to include rack failures, significantly reducing the risk of data loss when all TabletServers on a single rack fail simultaneously.

## Configuration

To specify that a TabletServer belongs to a particular rack, set the `tablet-server.rack` configuration option:

```yaml title="conf/server.yaml"
tablet-server.rack: RACK1
```

:::note Important
When rack awareness is enabled, the `tablet-server.rack` setting **must** be configured for each TabletServer. Failure to do so will prevent Fluss from starting and result in an exception.
:::

## Distribution Algorithm

When a table is created, the rack constraint ensures replicas are spread across as many racks as possible. A bucket will span:

`min(#racks, table.replication.factor)`

This approach maximizes replica distribution across different racks.

## Leadership Balancing

The replica assignment algorithm ensures that leader replicas are consistently distributed across TabletServers, regardless of how those servers are distributed across racks. This helps maintain balanced throughput across the system.

## Best Practices

**Recommendation:** Configure an equal number of TabletServers per rack.

If racks have different numbers of TabletServers, replica distribution will be uneven:
- Racks with fewer TabletServers will receive more replicas per server
- This leads to higher storage usage and increased resource requirements on those servers
- Workload distribution becomes unbalanced

Equal TabletServer distribution ensures optimal resource utilization and balanced workloads.