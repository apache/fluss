---
title: Upgrade Notes
sidebar_position: 4
sidebar_class_name: hidden
---

# Upgrade Notes from v0.9 to v1.0

These notes describe configuration, behavior, and compatibility changes to review before upgrading from Fluss v0.9 to v1.0. Previous versions can be found in the [archive of upgrade notes](upgrade-notes-archive.md).

Fluss v1.0 is the first release after graduation to an Apache Top-Level Project. Release artifacts no longer carry the `-incubating` suffix; update download scripts and dependency versions accordingly.

## Upgrade Plan

### Suggested Upgrade Order

Use the following sequence. Deployments without lakehouse tables can skip the second step.

1. **Upgrade all clients and connectors.** This prepares applications to use v1.0 features and ensures that lake readers can handle both legacy and clean lake table schemas before any clean table can be created.
2. **Upgrade the tiering service.** The new tiering service continues to write the legacy schema for existing tables and writes the clean schema for newly created tables.
3. **Upgrade the Fluss cluster.** Upgrade TabletServers one by one, waiting for the cluster to recover after each restart, then upgrade the CoordinatorServer. See [Upgrading the Fluss Server Version](upgrading.md#upgrading-the-fluss-server-version) for the rolling-upgrade procedure.

For lakehouse deployments, upgrading lake-reading connectors and storage plugins, followed by the tiering service, before the Fluss cluster is required by the lake table schema change in FIP-27. See [Lake Table Schema Changes (FIP-27)](#lake-table-schema-changes-fip-27) for the compatibility matrix and rollback limitations.

### Compatibility During the Upgrade

Clients at v0.9 can continue to run existing workloads against a v1.0 cluster, subject to the authorization and format changes described on this page. This compatibility does not extend to new features: full KV scan, multi-table subscription, log filter pushdown, column statistics, and KV snapshot leases require both v1.0 servers and v1.0 clients. In particular, keep `table.statistics.columns` unset until all servers and all clients reading the affected table have been upgraded; see [Column Statistics and the V1 Log Batch Format](#column-statistics-and-the-v1-log-batch-format).

### CoordinatorServer HA After the Upgrade

After completing the cluster upgrade, you can add v1.0 standby CoordinatorServers. Configure them with the same `zookeeper.address` and `zookeeper.path.root` as the existing coordinator. They participate in ZooKeeper leader election automatically; no additional HA feature flag is required.

During the initial upgrade of a single CoordinatorServer, admin operations such as table creation remain unavailable while that process is stopped. Adding standbys enables automatic failover for subsequent restarts, although admin operations can still be briefly unavailable during leader election. See [CoordinatorServer HA](../../install-deploy/deploying-distributed-cluster.md#fluss-coordinatorserver-high-availability-ha-setup).

## Authorization Changes

This section applies to clusters with authorization enabled.

### ACL Modification Requires `ALL` Permission

Starting in v1.0, creating or dropping ACLs requires `ALL` permission on the target resource. In previous versions, users with `ALTER` permission could modify ACLs.

Before upgrading, review any users, roles, scripts, or automation that call `createAcls`, `dropAcls`, `CALL sys.add_acl`, or `CALL sys.drop_acl`. Grant `ALL` permission to principals that should continue managing ACLs after the upgrade.

### Additional `DESCRIBE` Permission Checks

Several read-only calls now check permissions that were not checked in v0.9:

- `listOffsets` requires `DESCRIBE` permission on the table.
- `databaseExists` and `tableExists` return `false` if the principal lacks `DESCRIBE` permission on the database or table, respectively, hiding the resource's existence. The `default` database is exempt from the `databaseExists` permission check for compatibility with Flink catalog initialization.

If an application reports that an existing database or table is missing after upgrading, check its permissions before recreating the resource. Grant the appropriate `DESCRIBE` permission to restore access. See [Authorization and ACLs](../../security/authorization.md).

## Cluster Configuration Changes

### Lower Default Bucket Limit

The default of `max.bucket.num` changes from 128000 to **4096** in v1.0 to reduce the risk of assignment metadata exceeding ZooKeeper's packet size limit. The limit applies to each non-partitioned table or to **each partition** of a partitioned table, rather than the sum of buckets across all partitions.

Creating a table or partition with more buckets than the configured limit fails with `TooManyBucketsException`. Existing tables and partitions are not resized or removed, but creation of new partitions in an existing table is subject to the new limit, including automatic partition creation. If your workloads require more than 4096 buckets per table or partition, explicitly configure a suitable `max.bucket.num` before upgrading. An existing explicit setting continues to override the default. See [Server Configuration](../configuration.md).

### Remote Storage Directory Configuration

The new `remote.data.dirs` option supports multiple remote storage locations and takes precedence over `remote.data.dir` when both are configured. Existing single-directory configurations continue to work; new clusters should use `remote.data.dirs`.

When migrating an existing cluster, keep the old `remote.data.dir` value as the **first entry** in `remote.data.dirs`. Existing data without an explicit storage location is resolved against this default directory. For example, if the old value was `s3://bucket-a/fluss`, use:

```yaml
remote.data.dirs: s3://bucket-a/fluss,s3://bucket-b/fluss
```

See [Server Configuration](../configuration.md#common) for remote directory placement strategies.

### Local Multi-Disk Configuration

TabletServers can use multiple local disks through `data.dirs`. If both `data.dirs` and `data.dir` are configured, `data.dirs` takes precedence. If `data.dirs` is unset, the existing `data.dir` remains the sole local data directory. No configuration change is required unless you want to adopt multiple disks; keep existing data directories available when changing the configuration. See [Server Configuration](../configuration.md).

### AWS Credentials Providers Migrated to AWS SDK v2

Starting in v1.0, the Fluss S3 filesystem plugin uses AWS SDK for Java v2 through Hadoop 3.4.3.

If `s3.aws.credentials.provider`, `s3a.aws.credentials.provider`, or `fs.s3a.aws.credentials.provider` explicitly references an AWS SDK v1 provider, update it to the corresponding AWS SDK v2 class before upgrading.

| AWS SDK v1 provider | AWS SDK v2 provider |
|---|---|
| `com.amazonaws.auth.ContainerCredentialsProvider` | `software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider` |
| `com.amazonaws.auth.EnvironmentVariableCredentialsProvider` | `software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider` |
| `com.amazonaws.auth.InstanceProfileCredentialsProvider` | `software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider` |
| `com.amazonaws.auth.WebIdentityTokenCredentialsProvider` | `software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider` |
| `com.amazonaws.auth.profile.ProfileCredentialsProvider` | `software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider` |

Custom credentials providers must now implement `software.amazon.awssdk.auth.credentials.AwsCredentialsProvider` instead of `com.amazonaws.auth.AWSCredentialsProvider`. Implementations should provide credentials through `resolveCredentials()` rather than the SDK v1 `getCredentials()` and `refresh()` methods.

Deployments using static access keys or the default AWS credentials provider chain do not require configuration changes.

### Active Segment Retention Rollout

When upgrading a cluster from v0.9, keep
`log.retention.roll-active-segment.enabled` disabled for the entire upgrade. This is the default, so
no configuration change is required before or during the rolling upgrade.

After every CoordinatorServer and TabletServer has been upgraded to v1.0 and the upgrade is
complete, enable the option with a dynamic cluster configuration update:

```sql
CALL sys.set_cluster_configs(
  config_pairs => 'log.retention.roll-active-segment.enabled', 'true'
);
```

Enabling the option allows a non-empty active local log segment to be rolled after its effective
local cleanup TTL expires and all records are committed. For tiered logs, the effective TTL is
`table.log.local-ttl`, or `table.log.ttl` when the local option is not configured. The rolled
segment can then be uploaded to remote storage and cleaned up locally. This avoids indefinitely
retaining an expired active segment on low-traffic tables.

See [TTL](../../table-design/data-distribution/ttl.md) for the segment lifecycle,
[remote storage](../tiered-storage/remote-storage.md) for tiered-log retention, and
[updating configs](updating-configs.md#updating-cluster-configs) for other dynamic update methods.

## Flink Connector Changes

### Newly Discovered Partitions Start from Earliest

In v0.9, partitions discovered while a streaming job was running followed `scan.startup.mode`. With `latest`, this could skip records written between partition creation and discovery.

In v1.0, partitions present at the initial discovery follow `scan.startup.mode`, while partitions discovered later start from the earliest offset for log reads, including when `scan.startup.mode` is `latest`. Primary-key tables in `full` mode retain their snapshot-based initialization, falling back to earliest when no snapshot is available.

Jobs that relied on `latest` to skip records already written to newly discovered partitions will now read those records. Review any downstream assumptions about skipping backfilled data. See [Start Reading Position](../../engine-flink/reads.md#start-reading-position).

### Bucket-Level Source Reader Metrics Removed

The per-bucket `currentOffset` gauges in the `fluss.reader` metric group are no longer registered, including `fluss.reader.bucket.<n>.currentOffset` for non-partitioned tables and `fluss.reader.partition.<id>.bucket.<n>.currentOffset` for partitioned tables. Update dashboards and alerts that reference these gauges. The standard `currentFetchEventTimeLag` metric remains available for monitoring read lag; see [Flink Source Metrics](../observability/monitor-metrics.md#source-metrics).

## Client and Log Format Changes

### JAAS Configuration Restricted to `PlainLoginModule`

The compatibility option `client.security.sasl.jaas.config` now accepts only `org.apache.fluss.security.auth.sasl.plain.PlainLoginModule`. A configuration referencing another login module fails with `AuthenticationException` when the client initializes SASL authentication.

Prefer setting `client.security.sasl.username` and `client.security.sasl.password` together instead of embedding a JAAS string, and remove the old JAAS option when migrating to these dedicated options. See [SASL Client-Side Configuration](../../security/authentication.md#sasl-client-side-configuration).

### Column Statistics and the V1 Log Batch Format

Setting `table.statistics.columns` on a log table enables column statistics in newly written Arrow log batches and uses the extended **V1** log batch format. Clients at v0.9 or earlier cannot decode these batches. By default, the option is unset, no statistics are collected, and batches retain the v0.9-compatible **V0** format.

:::warning
Upgrade all Fluss servers and all clients reading the affected table to v1.0 before enabling `table.statistics.columns`. Upgrading the cluster alone is not sufficient. Disabling the option later does not convert already-written V1 batches back to V0.
:::

Only batches written with statistics can benefit from this optimization; existing batches are not rewritten. See [Filter Pushdown](../../engine-flink/reads.md#filter-pushdown) for configuration examples and supported predicates.

## Lakehouse Changes

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

### Historical Partition Lookup

Historical partition lookup through `table.datalake.historical-partition.enabled` is disabled by default and supports only **auto-partitioned Paimon primary-key tables with a single partition key**. After changing this option, restart existing lookup jobs that need access to historical partition data so their clients load the updated table configuration. See [Modifying Table Properties](../../engine-flink/ddl.md#set-properties).

### Lake Table Schema Changes (FIP-27)

Starting from this version, Fluss creates lake tables with a **clean** physical schema that contains only the user-defined columns. Earlier versions appended three trailing system columns (`__bucket`, `__offset`, `__timestamp`) to every lake table; these are no longer added to newly created tables.

This applies to the Paimon and Iceberg lake formats. The Hudi lake storage was never exposed in a publicly released version, so it only ever uses the clean layout and the compatibility considerations below do not apply to it.

#### Clean and Legacy Layouts

- **Clean layout**: newly created lake tables contain only user columns.
- **Legacy layout**: lake tables created by earlier Fluss versions still carry the three trailing system columns.

Existing legacy tables are **not** migrated and remain fully readable and writable. Fluss detects the layout directly from the physical schema — a table is treated as legacy when it carries the system columns, and clean otherwise — so both layouts are supported side by side. The tiering service keeps writing the legacy layout for a table that already has the system columns, and writes the clean layout for newly created tables.

The names `__bucket`, `__offset`, and `__timestamp` remain reserved for Fluss internal use. System columns are disabled by default; any future opt-in behavior to re-enable them is outside the scope of FIP-27.

#### Compatibility Matrix

| Component | Legacy tables | Clean tables |
|-----------|---------------|--------------|
| New lake storage plugins / lake-reading Flink connectors | Readable | Readable |
| New tiering service | Writes legacy layout | Writes clean layout |
| Old tiering service | Supported | **Not supported** — must not process clean tables |
| Old Flink connectors using `FULL` startup mode | Readable | **Not readable** |

Old Flink connectors that use `FULL` startup mode assume the presence of the system columns and therefore cannot read newly created clean lake tables. Note that `FULL` is the **default** value of `scan.startup.mode`, so an old connector that does not explicitly set a startup mode is also affected — when auditing jobs before an upgrade, do not look only for jobs that explicitly configure `FULL`. A lake-reading Flink connector must be upgraded together with its matching lake storage plugin.

This compatibility matrix is why the lake-reading connectors and storage plugins, followed by the tiering service, must be upgraded before the Fluss cluster. Follow the [suggested upgrade order](#suggested-upgrade-order) to prevent an old reader or tiering service from encountering a clean table it cannot handle.

#### Rollback Limitations

Once a clean lake table has been created, rolling back to an older tiering service or older lake-reading connectors is **not** safe: those components assume the system columns are present and cannot correctly read or write the clean table. Plan the upgrade with this in mind, since a clean table cannot be transparently rolled back to the legacy layout.
