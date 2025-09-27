---
title: HDFS
sidebar_position: 2
---

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

Fluss includes bundled Hadoop libraries with version 3.3.4 for deploying Fluss in machine without Hadoop installed. 
For most use cases, these work perfectly. However, you should configure your machine's native Hadoop environment if:
1. Your HDFS uses kerberos security
2. You need to avoid version conflicts between Fluss's bundled hadoop libraries and your HDFS cluster

Fluss automatically loads HDFS dependencies on the machine via the `HADOOP_CLASSPATH` environment variable.
Make sure that the `HADOOP_CLASSPATH` environment variable is set up (it can be checked by running `echo $HADOOP_CLASSPATH`).
If not, set it up using
```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```
