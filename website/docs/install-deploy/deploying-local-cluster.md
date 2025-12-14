---
title: "Deploying Local Cluster"
sidebar_position: 2
---

# Deploying Local Cluster
:::warning
**This setup deploys Fluss to a single machine only.** The Fluss endpoint will **only be accessible locally** (e.g. via `localhost`).
If you need access your Fluss cluster from a different machine or deploy across multiple servers, please refer to the [Deploying Distributed Cluster](install-deploy/deploying-distributed-cluster.md).
That guide explains how to use `coordinator-server.sh` and `tablet-server.sh` with an externally accessible homename and port.
:::

This page provides instructions on how to deploy a *local cluster* (on one machine, but in separate processes) for Fluss.

## Requirements

Fluss runs on all *UNIX-like environments*, e.g. **Linux**, **Mac OS X**.
Before you start to set up the system, make sure you have the following software installed on your test machine:

- **Java 11** or higher (Java 8 is not supported; **Java 17 or higher is strongly recommended** for better performance)

If your cluster does not fulfill these software requirements you will need to install/upgrade it.

### `JAVA_HOME` Configuration

Fluss requires the `JAVA_HOME` environment variable to be set on your
test machine and point to the directory of your Java installation.

## Fluss Setup

Go to the [downloads page](/downloads) and download the latest Fluss release. Make sure to pick the Fluss
package **matching your Java version**. After downloading the latest release, extract it:

```shell
tar -xzf fluss-$FLUSS_VERSION$-bin.tgz
cd fluss-$FLUSS_VERSION$/
```

## Starting Fluss Local Cluster

You can start Fluss local cluster by running the following command:
```shell
./bin/local-cluster.sh start
```

After that, the Fluss local cluster is started.

## Interacting with Fluss

After Fluss local cluster is started, you can use **Fluss Client** (Currently, only support Flink SQL Client) to interact with Fluss.
The following subsections will show you how to use Flink SQL Client to interact with Fluss.

### Flink SQL Client

Using Flink SQL Client to interact with Fluss.

#### Preparation

You can start a Flink standalone cluster refer to [Flink Environment Preparation](engine-flink/getting-started.md#preparation-when-using-flink-sql-client)

**Note**: Make sure the [Fluss connector jar](/downloads/) already has copied to the `lib` directory of your Flink home.

#### Add catalog

In Flink SQL client, a catalog is created and named by executing the following query:
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'localhost:9123'
);
```

#### Do more with Fluss

After the catalog is created, you can use Flink SQL Client to do more with Fluss, for example, create a table, insert data, query data, etc.
More details please refer to [Flink Getting Started](engine-flink/getting-started.md)