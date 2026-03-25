---
title: Upgrade Notes
sidebar_position: 4
---

# Upgrade Notes from v0.9 to v0.10

## New `datalake.enabled` Cluster Configuration

Starting from v0.10, Fluss introduces the cluster-level configuration `datalake.enabled` to control whether the cluster is ready to create and manage lakehouse tables.

### Behavior Changes

- If `datalake.enabled` is unset, Fluss keeps the legacy behavior: configuring `datalake.format` alone also enables lakehouse tables.
- If `datalake.enabled = false`, Fluss only pre-binds the lake format for newly created tables and does not allow lakehouse tables yet.
- If `datalake.enabled = true`, Fluss fully enables lakehouse tables.
- If `datalake.enabled` is explicitly configured, `datalake.format` must also be configured.

### Recommended Configuration

If you want to enable lakehouse tables on the cluster, configure both options together:

```yaml
datalake.enabled: true
datalake.format: paimon
```

If you only want to pre-bind the lake format without enabling lakehouse tables yet, configure:

```yaml
datalake.enabled: false
datalake.format: paimon
```

### Documentation Updates for Existing Deployments

If your existing deployment or internal scripts only set `datalake.format`, they will continue to work with the legacy behavior as long as `datalake.enabled` is left unset.

However, for new configuration examples and operational guidance, prefer configuring `datalake.enabled` explicitly together with `datalake.format`.
