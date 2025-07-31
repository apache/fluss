---
title: HDFS
sidebar_position: 2
---

# HDFS
[HDFS (Hadoop Distributed File System)](https://hadoop.apache.org/docs/stable/) is the primary storage system used by Hadoop applications. Fluss
supports HDFS as a remote storage.


## Configurations setup

To enabled HDFS as remote storage, you need to define the hdfs path as remote storage in Fluss' `server.yaml`:

```yaml
# The dir that used to be as the remote storage of Fluss
remote.data.dir: hdfs://namenode:50010/path/to/remote/storage
```

To allow for easy adoption, you can use the same configuration keys in Fluss' server.yaml as in Hadoop's `core-site.xml`.
You can see the configuration keys in Hadoop's [`core-site.xml`](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/core-default.xml).






