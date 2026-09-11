---
sidebar_label: Overview
title: File Systems
sidebar_position: 1
---

# File Systems

Fluss uses file systems as remote storage to store snapshots for Primary-Key Table and store tiered log segments for Log Table. These
are some of the file systems that Fluss supports currently, including *local*, *hadoop*, *Aliyun OSS*, *Tencent Cloud COS*.

The file system used for a particular file is determined by its URI scheme. For example, `file:///home/user/text.txt` refers to a file in the local file system,
while `hdfs://namenode:50010/data/user/text.txt` is a file in a specific HDFS cluster.

File system instances are instantiated once per process and then cached/pooled, to avoid configuration overhead per stream creation.


## Local File System

Fluss has built-in support for the file system of the local machine, including any NFS or SAN drives mounted into that local file system. Local files are referenced with the `file://` URI scheme. You 
can use local file system as remote storage for testing purposes with the following configuration in Fluss' `server.yaml`:
```yaml
remote.data.dir: file:///path/to/remote/storage
```

:::warning
Never use local file system as remote storage in production as it is not fault-tolerant. Please use distributed file systems or cloud object storage listed in [Pluggable File Systems](#pluggable-file-systems).
:::

## Pluggable File Systems
The Fluss project supports the following file systems:

- **[HDFS](hdfs.md)** is supported by `fluss-fs-hdfs` and registered under the `hdfs://` URI scheme. HDFS filesystem is included in default Fluss binary distribution, so you can use it directly without manual installation. `fluss-fs-hdfs` bundles its own Hadoop; the thin `fluss-fs-hadoop` artifact provides the same scheme for setups that supply Hadoop through `HADOOP_CLASSPATH`. Install exactly one of them, see [Choosing an HDFS JAR](/downloads#choosing-an-hdfs-jar).

- **[Aliyun OSS](oss.md)** is supported by `fluss-fs-oss` and registered under the `oss://` URI scheme. OSS filesystem is included in default Fluss binary distribution, so you can use it directly without manual installation.

- **[AWS S3](s3.md)** is supported by `fluss-fs-s3` and registered under the `s3://` URI scheme. S3 filesystem is included in default Fluss binary distribution, so you can use it directly without manual installation.

- **[Azure Blob Storage](azure.md)** is supported by `fluss-fs-azure` and registered under the `abfs://`,`abfss://`,`wasb://`,`wasbs://`, URI schemes. Please make sure to [manually install the OBS plugin](azure.md#install-azure-fs-plugin-manually).

- **[HuaweiCloud OBS](obs.md)** is supported by `fluss-fs-obs` and registered under the `obs://` URI scheme. Please make sure to [manually install the OBS plugin](obs.md#install-obs-plugin-manually).

- **[Tencent Cloud COS](cos.md)** is supported by `fluss-fs-cos` and registered under the `cosn://` URI scheme. Please make sure to [manually install the COS plugin](cos.md#install-cos-plugin-manually).

The implementation is based on [Hadoop Project](https://hadoop.apache.org/) but is self-contained with no dependency footprint.

## Installing a Filesystem Plugin

A filesystem plugin JAR goes into its own sub directory of `${FLUSS_HOME}/plugins`, named after the
URI scheme, for example `${FLUSS_HOME}/plugins/s3/`. Each sub directory is loaded in an isolated
class loader together with the dependencies next to it.

:::warning
Never copy a plugin JAR into `${FLUSS_HOME}/lib` as well. The copy on the main classpath shadows the
one in the plugins directory and is loaded without its bundled dependencies, which fails at runtime
with errors such as `NoClassDefFoundError: org/apache/hadoop/hdfs/HdfsConfiguration`. See
[Troubleshooting Plugins](../../troubleshooting-plugins.md).
:::