---
title: Troubleshooting Plugins
sidebar_position: 6
---

# Troubleshooting Plugins

Fluss loads filesystem, lake format and metric reporter implementations as *plugins*. Most plugin
problems come down to a single question: **is the JAR in the right directory?** This page explains
how plugins are loaded and how to recognise the errors that follow when they are not.

## How plugins are loaded

A Fluss installation has two very different places to put a JAR:

```
${FLUSS_HOME}/
├── lib/                     # the main classpath, shared by everything
└── plugins/                 # one isolated sub directory per plugin
    ├── hdfs/
    │   └── fluss-fs-hdfs-<version>.jar
    ├── s3/
    │   └── fluss-fs-s3-<version>.jar
    └── paimon/
        ├── fluss-lake-paimon-<version>.jar
        ├── paimon-bundle-<version>.jar
        └── flink-shaded-hadoop-2-uber-<version>.jar
```

Every sub directory of `plugins/` is loaded in its own class loader, together with the dependencies
that sit next to it. That is what lets the S3 plugin and the Paimon plugin each carry their own,
possibly conflicting, versions of a library without interfering.

The plugins directory defaults to `plugins` and can be overridden with the `FLUSS_PLUGINS_DIR`
environment variable.

:::warning
Do not copy a plugin JAR into `${FLUSS_HOME}/lib`. Classes in `org.apache.fluss.` and
`org.apache.hadoop.` are loaded *parent-first*, so a copy on the main classpath wins over the copy
in the plugins directory — but it is loaded **without** the dependencies bundled next to the plugin.
The plugin then fails at runtime even though the JAR is clearly present.
:::

The parent-first packages are controlled by `plugin.classloader.parent-first-patterns.default`, and
can be extended with `plugin.classloader.parent-first-patterns.additional`.

## `${FLUSS_HOME}/lib` is not `${FLINK_HOME}/lib`

The [Tiering Service](../streaming-lakehouse/tiering-service.md) is a **Flink** job, and its JARs —
including `fluss-lake-<format>` — do go into `${FLINK_HOME}/lib`. That instruction is about the
Flink installation, not the Fluss one. Putting the same JAR into `${FLUSS_HOME}/lib` is what breaks
the Fluss servers.

| JAR | Goes in |
|-----|---------|
| Filesystem plugin (`fluss-fs-*`) | `${FLUSS_HOME}/plugins/<scheme>/` |
| Lake format plugin (`fluss-lake-*`) on the servers | `${FLUSS_HOME}/plugins/<format>/` |
| Lake format plugin for the Tiering Service | `${FLINK_HOME}/lib` |
| Flink connector (`fluss-flink-*`) | `${FLINK_HOME}/lib` |

## Common errors

### Found two LakeStoragePlugin for datalake format

```
Found two LakeStoragePlugin for datalake format 'paimon': one in the plugins directory
and one on the main classpath (usually <FLUSS_HOME>/lib). ...
```

The same lake format is installed twice. Delete the `fluss-lake-<format>` JAR from
`${FLUSS_HOME}/lib` and keep only the one in `${FLUSS_HOME}/plugins/<format>/`, then restart the
server. This check exists so the deployment fails immediately with an explanation, rather than later
with the `NoClassDefFoundError` below.

### NoClassDefFoundError: org/apache/hadoop/hdfs/HdfsConfiguration

```
java.lang.NoClassDefFoundError: org/apache/hadoop/hdfs/HdfsConfiguration
    at org.apache.paimon.catalog.CatalogContext.<init>(...)
Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.hdfs.HdfsConfiguration
```

The Paimon plugin was loaded from the main classpath instead of from
`${FLUSS_HOME}/plugins/paimon/`, so the `flink-shaded-hadoop-2-uber` JAR bundled next to it was
never visible. Remove the copy from `${FLUSS_HOME}/lib`.

Adding a Hadoop JAR to `${FLUSS_HOME}/lib` also makes the error disappear, but it is a workaround:
it puts Hadoop on the shared classpath for every component, which is exactly the version conflict
the plugin isolation is meant to prevent. Prefer removing the misplaced JAR.

### Hadoop is not in the classpath, or some classes are missing

```
Cannot support file system for 'hdfs' via Hadoop, because Hadoop is not in the
classpath, or some classes are missing from the classpath.
```

The `hdfs://` scheme resolved to an implementation that has no Hadoop with it. Check that
`${FLUSS_HOME}/plugins/hdfs/` contains `fluss-fs-hdfs`, which bundles Hadoop. If you deliberately
use the thin `fluss-fs-hadoop` artifact instead, Hadoop must be supplied through `HADOOP_CLASSPATH`:

```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```

Do not install both artifacts — see [Choosing an HDFS JAR](/downloads#choosing-an-hdfs-jar).

### No LakeStoragePlugin can be found for datalake format

```
No LakeStoragePlugin can be found for datalake format: paimon
```

`datalake.format` is set, but the matching plugin is not installed at all. Add the
`fluss-lake-<format>` JAR and its dependencies to `${FLUSS_HOME}/plugins/<format>/` and restart. See
[Deploying Streaming Lakehouse](../install-deploy/deploying-streaming-lakehouse.md).

## Checklist

When a plugin misbehaves, check in this order:

1. The JAR is in `${FLUSS_HOME}/plugins/<name>/`, in its own sub directory.
2. No copy of it is in `${FLUSS_HOME}/lib`.
3. The dependencies it needs sit next to it in the same sub directory.
4. Only one artifact provides each URI scheme or lake format.
5. The servers were restarted after the change — plugins are discovered at startup.
