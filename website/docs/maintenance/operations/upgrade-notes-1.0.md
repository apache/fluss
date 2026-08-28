---
title: Upgrade Notes
sidebar_position: 4
---

# Upgrade Notes from v0.9 to v1.0

## Authorization Changes

### ACL Modification Requires `ALL` Permission

Starting in v1.0, creating or dropping ACLs requires `ALL` permission on the target resource. In previous versions, users with `ALTER` permission could modify ACLs.

Before upgrading, review any users, roles, scripts, or automation that call `createAcls`, `dropAcls`, `CALL sys.add_acl`, or `CALL sys.drop_acl`. Grant `ALL` permission to principals that should continue managing ACLs after the upgrade.

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

## Lake Table Schema Changes (FIP-27)

Starting from this version, Fluss creates lake tables with a **clean** physical schema that contains only the user-defined columns. Earlier versions appended three trailing system columns (`__bucket`, `__offset`, `__timestamp`) to every lake table; these are no longer added to newly created tables.

This applies to the Paimon and Iceberg lake formats. The Hudi lake storage was never exposed in a publicly released version, so it only ever uses the clean layout and the compatibility considerations below do not apply to it.

### Clean and Legacy Layouts

- **Clean layout**: newly created lake tables contain only user columns.
- **Legacy layout**: lake tables created by earlier Fluss versions still carry the three trailing system columns.

Existing legacy tables are **not** migrated and remain fully readable and writable. Fluss detects the layout directly from the physical schema — a table is treated as legacy when it carries the system columns, and clean otherwise — so both layouts are supported side by side. The tiering service keeps writing the legacy layout for a table that already has the system columns, and writes the clean layout for newly created tables.

The names `__bucket`, `__offset`, and `__timestamp` remain reserved for Fluss internal use. System columns are disabled by default; any future opt-in behavior to re-enable them is outside the scope of FIP-27.

### Compatibility Matrix

| Component | Legacy tables | Clean tables |
|-----------|---------------|--------------|
| New lake storage plugins / lake-reading Flink connectors | Readable | Readable |
| New tiering service | Writes legacy layout | Writes clean layout |
| Old tiering service | Supported | **Not supported** — must not process clean tables |
| Old Flink connectors using `FULL` startup mode | Readable | **Not readable** |

Old Flink connectors that use `FULL` startup mode assume the presence of the system columns and therefore cannot read newly created clean lake tables. Note that `FULL` is the **default** value of `scan.startup.mode`, so an old connector that does not explicitly set a startup mode is also affected — when auditing jobs before an upgrade, do not look only for jobs that explicitly configure `FULL`. A lake-reading Flink connector must be upgraded together with its matching lake storage plugin.

### Required Upgrade Order

To move to a version that creates clean lake tables safely, upgrade the components in this order:

1. **Lake-reading Flink connectors and lake storage plugins** — so that readers can handle both the legacy and clean layouts before any clean table exists.
2. **Tiering service** — so that it starts producing clean tables only after the readers can consume them.
3. **Fluss cluster**.

Upgrading in a different order can leave an old reader or an old tiering service facing a clean table it cannot handle.

### Rollback Limitations

Once a clean lake table has been created, rolling back to an older tiering service or older lake-reading connectors is **not** safe: those components assume the system columns are present and cannot correctly read or write the clean table. Plan the upgrade with this in mind, since a clean table cannot be transparently rolled back to the legacy layout.
