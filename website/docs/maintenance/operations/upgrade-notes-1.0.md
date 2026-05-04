---
title: Upgrade Notes
sidebar_position: 4
---

# Upgrade Notes from v0.9 to v1.0

## Cluster Configuration Changes

### New `datalake.enabled` Cluster Configuration

Starting in v1.0, Fluss introduces the cluster-level configuration `datalake.enabled` to control whether the cluster is ready to create and manage lakehouse tables.

#### Behavior Changes
The behavior of Fluss regarding lakehouse table configuration is determined by the combination of `datalake.enabled` and `datalake.format`. The specific rules are as follows:

- If `datalake.enabled` is unset, Fluss defaults to legacy behavior: In this state, configuring `datalake.format` alone automatically enables lakehouse tables.
- If `datalake.enabled` is set to `false`, lakehouse functionality remains disabled. The `datalake.format` parameter is optional in this scenario. When `datalake.format` is explicitly configured, it pre-binds the specified lake format to newly created tables, preparing them for future integration without immediately activating lakehouse tables.
- If `datalake.enabled` is set to `true`, lakehouse functionality is fully enabled. In this state, `datalake.format` is strictly required and must be provided for the configuration to take effect.

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
