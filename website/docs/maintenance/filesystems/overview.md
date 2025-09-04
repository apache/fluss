---
sidebar_label: Overview
title: File Systems
sidebar_position: 1
---

# File Systems

Fluss uses file systems as remote storage to store snapshots for Primary-Key Table and log segments for Log Table. The following file systems are currently supported, including *local*, *HDFS*, *Aliyun OSS*, *AWS S3*, and *HuaweiCloud OBS*.

The file system for a particular file is determined by its URI scheme. For example, `file:///home/user/text.txt` refers to a local file system file, while `hdfs://namenode:50010/data/user/text.txt` references a file in a specific HDFS cluster.

File system instances are instantiated once per process and then cached/pooled, reducing configuration overhead for each stream creation.

## Local File System

Fluss has built-in support for the local machine's file system, including any mounted NFS or SAN drives. Local files use the `file://` URI scheme. You can use the local file system for testing with this configuration in Fluss' `server.yaml`:
```yaml
remote.data.dir: file:///path/to/remote/storage
```

:::warning
Never use local file system as remote storage in production as it is not fault-tolerant. Please use distributed file systems or cloud object storage listed in [Pluggable File Systems](#pluggable-file-systems).
:::

## Pluggable File Systems
Fluss supports the following file systems:

- **[HDFS](hdfs.md)** - supported by `fluss-fs-hadoop` and registered under the `hdfs://` URI scheme.

- **[Aliyun OSS](oss.md)** - supported by `fluss-fs-oss` and registered under the `oss://` URI scheme.

- **[AWS S3](s3.md)** - supported by `fluss-fs-s3` and registered under the `s3://` URI scheme.

- **[HuaweiCloud OBS](obs.md)** - supported by `fluss-fs-obs` and registered under the `obs://` URI scheme.

The implementation leverages the [Hadoop Project](https://hadoop.apache.org/) but is self-contained with no dependency footprint.
