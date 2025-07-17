---
title: HDFS
sidebar_position: 2
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# HDFS
[HDFS (Hadoop Distributed File System)](https://hadoop.apache.org/docs/stable/) is the primary storage system used by Hadoop applications. Fluss
supports HDFS as a remote storage.


## Configurations setup

To enabled HDFS as remote storage, you need to define the hdfs path as remote storage in Fluss' `server.yaml`:
```yaml title="conf/server.yaml"
# The dir that used to be as the remote storage of Fluss
remote.data.dir: hdfs://namenode:50010/path/to/remote/storage
```

### Configure Hadoop related configurations

Sometimes, you may want to configure how Fluss accesses your Hadoop filesystem, Fluss supports three methods for loading Hadoop configuration, listed in order of priority (highest to lowest):

1. **Fluss Configuration with `fluss.hadoop.*` Prefix.** Any configuration key prefixed with `fluss.hadoop.` in your `server.yaml` will be passed directly to Hadoop configuration, with the prefix stripped.
2. **Environment Variables.** The system automatically searches for Hadoop configuration files in these locations:
   - `$HADOOP_CONF_DIR` (if set)
   - `$HADOOP_HOME/conf` (if HADOOP_HOME is set)
   - `$HADOOP_HOME/etc/hadoop` (if HADOOP_HOME is set)
3. **Classpath Loading.** Configuration files (`core-site.xml`, `hdfs-site.xml`) found in the classpath are loaded automatically.

#### Configuration Examples
Here's an example of setting up the hadoop configuration in server.yaml:

```yaml title="conf/server.yaml"
# The all following hadoop related configurations is just for a demonstration of how 
# to configure hadoop related configurations in `server.yaml`, you may not need configure them

# Basic HA Hadoop configuration using fluss.hadoop.* prefix  
fluss.hadoop.fs.defaultFS: hdfs://mycluster
fluss.hadoop.dfs.nameservices: mycluster
fluss.hadoop.dfs.ha.namenodes.mycluster: nn1,nn2
fluss.hadoop.dfs.namenode.rpc-address.mycluster.nn1: namenode1:9000
fluss.hadoop.dfs.namenode.rpc-address.mycluster.nn2: namenode2:9000
fluss.hadoop.dfs.namenode.http-address.mycluster.nn1: namenode1:9870
fluss.hadoop.dfs.namenode.http-address.mycluster.nn2: namenode2:9870
fluss.hadoop.dfs.ha.automatic-failover.enabled: true
fluss.hadoop.dfs.client.failover.proxy.provider.mycluster: org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider

# Optional: Maybe need kerberos authentication  
fluss.hadoop.hadoop.security.authentication: kerberos
fluss.hadoop.hadoop.security.authorization: true
fluss.hadoop.dfs.namenode.kerberos.principal: hdfs/_HOST@REALM.COM
fluss.hadoop.dfs.datanode.kerberos.principal: hdfs/_HOST@REALM.COM
fluss.hadoop.dfs.web.authentication.kerberos.principal: HTTP/_HOST@REALM.COM
# Client principal and keytab (adjust paths as needed)  
fluss.hadoop.hadoop.security.kerberos.ticket.cache.path: /tmp/krb5cc_1000
```

#### Use Machine Hadoop Environment Configuration

To use the machine hadoop environment, instead of Fluss' embedded Hadoop, follow these steps:

**Step 1: Set Hadoop Classpath**
```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```

**Step 2: Add the following to your configuration file**
```yaml
plugin.classloader.parent-first-patterns.default: java.,com.alibaba.fluss.,javax.annotation.,org.slf4j,org.apache.log4j,org.apache.logging,org.apache.commons.logging,ch.qos.logback,hdfs-site,core-site,org.apache.hadoop.,META-INF
```
