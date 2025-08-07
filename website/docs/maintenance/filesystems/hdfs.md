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

#### Hadoop Environment Configuration

Fluss includes bundled Hadoop libraries with version 3.3.4 for deploying Fluss in machine without Hadoop installed. 
For most use cases, these work perfectly. However, you should configure your machine's native Hadoop environment if:
1. Your HDFS uses kerberos security
2. You need to avoid version conflicts between Fluss's bundled hadoop libraries and your HDFS cluster

Configuration Steps are following:

**Step 1: Set Hadoop Classpath**
```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```

**Step 2: Add the following to your configuration file**
```yaml
plugin.classloader.parent-first-patterns.default: java.,com.alibaba.fluss.,javax.annotation.,org.slf4j,org.apache.log4j,org.apache.logging,org.apache.commons.logging,ch.qos.logback,hdfs-site,core-site,org.apache.hadoop.,META-INF
```






