---
title: Upgrading and Compatibility
sidebar_position: 1
---

As an online storage service, Fluss is typically designed to operate over extended periods, often spanning several years.
Like all long-running services, Fluss requires ongoing maintenance, which includes fixing bugs, implementing improvements,
and migrating applications to newer versions of the Fluss cluster. In our code design process, we place a strong emphasis
on compatibility to ensure that any updates or changes are seamlessly integrated without disrupting the existing
functionality or stability of the system.

This document provides detailed instructions on how to upgrade the Fluss server, as well as information on the
compatibility between different versions of the Fluss client and the Fluss server.

## Upgrading the Fluss Server Version

This section outlines the general process for upgrading Fluss across versions. For server upgrades, we recommend using
the rolling upgrade method. Specifically, upgrade the `TabletServers` one-by-one first, and then upgrade the `CoordinatorServer`.

:::note
1. During the server upgrade process, read and write operations in the cluster will not be affected.
2. Currently, the Fluss `CoordinatorServer` does not yet support high availability (HA). During the `CoordinatorServer` upgrade stage, the `CoordinatorServer` will be in an unavailable state, which will affect admin operations such as table creation.
:::

The following is an example of upgrading the Fluss server from 0.6 to $FLUSS_VERSION$ on
a [Distributed Cluster](docs/install-deploy/deploying-distributed-cluster.md):

### Download And Configure Fluss

1. First, download Fluss binary file for $FLUSS_VERSION$:

```shell
tar -xzf fluss-$FLUSS_VERSION$-bin.tgz
cd fluss-$FLUSS_VERSION$/
```

2. If you want to enable [Lakehouse Storage](docs/maintenance/tiered-storage/lakehouse-storage.md), you need to prepare the required JAR files for the datalake first. For more details,
   see [Add other jars required by datalake](docs/maintenance/tiered-storage/lakehouse-storage.md#add-other-jars-required-by-datalake).

3. Next, copy the configuration options from 0.6 (`fluss-0.6/conf/server.yaml`) to the new configuration
file (`fluss-$FLUSS_VERSION$/conf/server.yaml`). Adding any new options introduced in version $FLUSS_VERSION$ as
needed to experience the new features.

### Upgrade the TabletServers one-by-one

To upgrade the `TabletServers`, follow these steps one-by-one for each `TabletServer`:

**Stop a TabletServer**

```shell
./fluss-0.6/bin/tablet-server.sh stop
```

**Restart the new TabletServer**

```shell
./fluss-$FLUSS_VERSION$/bin/tablet-server.sh start
```

### Upgrade the CoordinatorServer

After all `TabletServers` have been upgraded, you can proceed to upgrade the `CoordinatorServer` by following these steps:

**Stop the CoordinatorServer**

```shell
./fluss-0.6/bin/coordinator-server.sh stop
```

**Restart the new CoordinatorServer**

```shell
./fluss-$FLUSS_VERSION$/bin/coordinator-server.sh start
```

Once this process is complete, the server upgrade will be finished.

## Compatibility between Fluss Client and Fluss Server

The compatibility between the Fluss client and the Fluss server is described in the following table:


|            | Server 0.7 | Server 0.8 | Limitations |
|------------|------------|------------|-------------|
| Client 0.7 | ✔️         | ✔️         |             |
| Client 0.8 | ✔️         | ✔️         |             |

## Upgrading Fluss datalake tiering service

### Behavior Change
Since Fluss 0.8, the auto-compaction feature during datalake tiering is **disabled** by default. Compaction will no longer be triggered automatically as part of the tiering process.

| Version                                | Behavior                                                                                                                       |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| **Previous Version(v0.7 and earlier)** | Auto-compaction enabled during tiering                                                                                         |
| **Fluss 0.8+**                         | **Auto-compaction disabled by default**. Tiering service focus solely on data movement; compaction must be explicitly enabled. |

### How to Enable Compaction
To maintain the previous behavior and enable automatic compaction, you must manually configure `table.datalake.auto-compaction = true` for each table in table option.

**Important Note**: This is a per-table setting. This design provides granular control, allowing you to manage compaction based on the specific performance and storage needs of each individual table.

### Reason for Change & Benefits
This change was implemented to significantly improve the core stability and performance of the Datalake Tiering Service.
- **Enhanced Stability & Performance**: Compaction is a resource-intensive operation (CPU/IO) that can impose significant pressure on the tiering service. By disabling it by default, the service can dedicate all resources to its primary function: reliably and efficiently moving data. This results in a more stable, predictable, and smoother tiering experience for all users.
- **Granular Control & Flexibility**: This change empowers users to make a conscious choice based on their specific needs. You can now decide the optimal balance between storage efficiency (achieved through compaction) and computational resource allocation on a per-table basis.

