---
title: Upgrade Notes
sidebar_position: 4
---

# Upgrade Notes from v0.9 to v0.10

## Cluster Configuration Changes

### New `datalake.enabled` Cluster Configuration

Starting in v0.10, Fluss introduces the cluster-level configuration `datalake.enabled` to control whether the cluster is ready to create and manage lakehouse tables.

#### Behavior Changes

- If `datalake.enabled` is unset, Fluss preserves the legacy behavior: configuring `datalake.format` alone also enables lakehouse tables.
- If `datalake.enabled = false`, Fluss pre-binds the lake format for newly created tables but does not allow lakehouse tables yet.
- If `datalake.enabled = true`, Fluss fully enables lakehouse tables.
- If `datalake.enabled` is explicitly set to `true`, `datalake.format` must also be configured. When `datalake.enabled = false`, `datalake.format` is optional, but required if you want the pre-bind behavior described above.

#### Recommended Configuration

To enable lakehouse tables for the cluster, configure both options together:

```yaml
datalake.enabled: true
datalake.format: paimon
```

To pre-bind the lake format without enabling lakehouse tables yet, configure:

```yaml
datalake.enabled: false
datalake.format: paimon
```

This mode is useful when you want newly created tables to carry the lake format in advance, while postponing lakehouse enablement at the cluster level.
After `datalake.enabled` is later set to `true`, tables created under this configuration can still turn on `table.datalake.enabled` without being recreated.

#### Notes for Existing Deployments

If your existing deployment or internal scripts only set `datalake.format`, they will continue to work with the legacy behavior as long as `datalake.enabled` remains unset.

For new configuration examples and operational guidance, we recommend explicitly configuring `datalake.enabled` together with `datalake.format`.
