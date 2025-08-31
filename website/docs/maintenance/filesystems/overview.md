---
sidebar_label: Overview
title: File Systems
sidebar_position: 1
---

<!--
 Copyright (c) 2025 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# File Systems

Fluss uses file systems as remote storage to store snapshots for Primary-Key Table and store tiered log segments for Log Table. These
are some of the file systems that Fluss supports currently, including *local*, *hadoop*, *Aliyun OSS*.

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
The Fluss project supports the following file system:

- **[HDFS](hdfs.md)** is supported by `fluss-fs-hadoop` and registered under the `hdfs://` URI scheme.

- **[Aliyun OSS](oss.md)** is supported by `fluss-fs-oss` and registered under the `oss://` URI scheme.

- **[AWS S3](s3.md)** is supported by `fluss-fs-s3` and registered under the `s3://` URI scheme.

The implementation is based on [Hadoop Project](https://hadoop.apache.org/) but is self-contained with no dependency footprint.