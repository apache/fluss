---
title: "Lakehouse Storage"
sidebar_position: 3
---

# Lakehouse Storage

Lakehouse represents a new, open architecture that combines the best elements of data lakes and data warehouses.
Lakehouse combines data lake scalability and cost-effectiveness with data warehouse reliability and performance.

Fluss leverages the well-known Lakehouse storage solutions like Apache Paimon, Apache Iceberg, Apache Hudi, Delta Lake as
the tiered storage layer. Currently, Apache Paimon and Apache Iceberg are supported, with more kinds of Lakehouse storage support on the way.

Fluss's datalake tiering service will tier Fluss's data to the Lakehouse storage continuously. The data in Lakehouse storage can be read both by Fluss's client in a streaming manner and accessed directly
by external systems such as Flink, Spark, StarRocks and others. With data tiered in Lakehouse storage, Fluss
can gain much storage cost reduction and analytics performance improvement.


## Enable Lakehouse Storage

Lakehouse Storage is disabled by default, you must enable it manually.

### Lakehouse Storage Cluster Configurations
#### Modify `server.yaml`
First, you must configure the lakehouse storage in `server.yaml`.

**For Paimon:**
```yaml
# Paimon configuration
datalake.format: paimon

# the catalog config about Paimon, assuming using Filesystem catalog
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon
```

Fluss processes Paimon configurations by removing the `datalake.paimon.` prefix and then use the remaining configuration (without the prefix `datalake.paimon.`) to create the Paimon catalog. Checkout the [Paimon documentation](https://paimon.apache.org/docs/1.1/maintenance/configurations/) for more details on the available configurations.

**For Iceberg:**
```yaml
# Iceberg configuration
datalake.format: iceberg

# the catalog config about Iceberg, assuming using Filesystem catalog
datalake.iceberg.warehouse: /tmp/iceberg
datalake.iceberg.catalog-impl: org.apache.iceberg.hadoop.HadoopCatalog
```

Fluss processes Iceberg configurations by removing the `datalake.iceberg.` prefix and then use the remaining configuration (without the prefix `datalake.iceberg.`) to create the Iceberg catalog. Checkout the [Iceberg documentation](https://iceberg.apache.org/docs/latest/configuration/) for more details on the available configurations.

For example, if you want to configure to use Hive catalog with Paimon, you can configure like following:
```yaml
datalake.format: paimon
datalake.paimon.metastore: hive
datalake.paimon.uri: thrift://<hive-metastore-host-name>:<port>
datalake.paimon.warehouse: hdfs:///path/to/warehouse
```

For Iceberg with Hive catalog:
```yaml
datalake.format: iceberg
datalake.iceberg.catalog-impl: org.apache.iceberg.hive.HiveCatalog
datalake.iceberg.uri: thrift://<hive-metastore-host-name>:<port>
datalake.iceberg.warehouse: hdfs:///path/to/warehouse
```

#### Add other jars required by datalake
While Fluss includes the core libraries for supported lakehouse formats, additional jars may still need to be manually added to the respective plugin directories according to your needs.

**For Paimon:**
Add jars to `${FLUSS_HOME}/plugins/paimon/`.
For example, for OSS filesystem support, you need to put `paimon-oss-<paimon_version>.jar` into directory `${FLUSS_HOME}/plugins/paimon/`.

**For Iceberg:**
Add jars to `${FLUSS_HOME}/plugins/iceberg/`.
For example, for AWS S3 filesystem support, you need to put `iceberg-aws-<iceberg_version>.jar` into directory `${FLUSS_HOME}/plugins/iceberg/`.

### Start The Datalake Tiering Service
Then, you must start the datalake tiering service to tier Fluss's data to the lakehouse storage.
#### Prerequisites
- A running Flink cluster (currently only Flink is supported as the tiering backend)
- Download [fluss-flink-tiering-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-tiering/$FLUSS_VERSION$/fluss-flink-tiering-$FLUSS_VERSION$.jar)

#### Prepare required jars
- Put [fluss-flink connector jar](/downloads) into `${FLINK_HOME}/lib`, you should choose a connector version matching your Flink version. If you're using Flink 1.20, please use [fluss-flink-1.20-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar)
- If you are using [Amazon S3](http://aws.amazon.com/s3/), [Aliyun OSS](https://www.aliyun.com/product/oss) or [HDFS(Hadoop Distributed File System)](https://hadoop.apache.org/docs/stable/) as Fluss's [remote storage](maintenance/tiered-storage/remote-storage.md),
  you should download the corresponding [Fluss filesystem jar](/downloads#filesystem-jars) and also put it into `${FLINK_HOME}/lib`
- Put the lakehouse format jar into `${FLINK_HOME}/lib`:
    - **For Paimon**: Put [fluss-lake-paimon jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-paimon/$FLUSS_VERSION$/fluss-lake-paimon-$FLUSS_VERSION$.jar) into `${FLINK_HOME}/lib`
    - **For Iceberg**: Put [fluss-lake-iceberg jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-iceberg/$FLUSS_VERSION$/fluss-lake-iceberg-$FLUSS_VERSION$.jar) into `${FLINK_HOME}/lib`
- [Download](https://flink.apache.org/downloads/) pre-bundled Hadoop jar `flink-shaded-hadoop-2-uber-*.jar` and put into `${FLINK_HOME}/lib`
- Put the respective lakehouse format's filesystem jars into `${FLINK_HOME}/lib`:
    - **For Paimon**: If you use S3 to store Paimon data, please put `paimon-s3` jar into `${FLINK_HOME}/lib`
    - **For Iceberg**: If you use S3 to store Iceberg data, please put `iceberg-aws` jar into `${FLINK_HOME}/lib`
- The other jars that the lakehouse format may require, for example, if you use HiveCatalog, you will need to put hive related jars

#### Start Datalake Tiering Service
After the Flink Cluster has been started, you can execute the `fluss-flink-tiering-$FLUSS_VERSION$.jar` by using the following command to start datalake tiering service:

**For Paimon:**
```shell
<FLINK_HOME>/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/paimon
```

**For Iceberg:**
```shell
<FLINK_HOME>/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.warehouse /tmp/iceberg \
    --datalake.iceberg.catalog-impl org.apache.iceberg.hadoop.HadoopCatalog
```

**Note:**
- The `fluss.bootstrap.servers` should be the bootstrap server address of your Fluss cluster. You must configure all options with the `datalake.` prefix in the [server.yaml](#modify-serveryaml) file to run the tiering service. The parameters should match exactly what you configured in your `server.yaml`.
- The Flink tiering service is stateless, and you can run multiple tiering services simultaneously to tier tables in Fluss.
  These tiering services are coordinated by the Fluss cluster to ensure exactly-once semantics when tiering data to the lake storage. This means you can freely scale the service up or down according to your workload.
- This follows the standard practice for [submitting jobs to Flink](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/cli/), where you can use the `-D` parameter to specify Flink-related configurations.
  For example, if you want to set the tiering service job name to `My Fluss Tiering Service1` and use `3` as the job parallelism, you can use the following command:
```shell
<FLINK_HOME>/bin/flink run \
    -Dpipeline.name="My Fluss Tiering Service1" \
    -Dparallelism.default=3 \
    /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/paimon
```

### Enable Lakehouse Storage Per Table
To enable lakehouse storage for a table, the table must be created with the option `'table.datalake.enabled' = 'true'`.

#### Table-level Configuration Options

**Data Freshness Control:**
The option `table.datalake.freshness` allows per-table configuration of data freshness in the datalake.
It defines the maximum amount of time that the datalake table's content should lag behind updates to the Fluss table.
Based on this target freshness, the Fluss tiering service automatically moves data from the Fluss table and updates to the datalake table, so that the data in the datalake table is kept up to date within this target.
The default is `3min`, if the data does not need to be as fresh, you can specify a longer target freshness time to reduce costs.

**Auto Maintenance (Iceberg only):**
The option `table.datalake.auto-maintenance` (default: `true`) configures whether to perform automatic maintenance tasks for Iceberg tables, including:
- File compaction to optimize read performance
- Snapshot expiration to manage storage costs
- Other maintenance operations to keep the table optimized

Example with all options:
```sql
CREATE TABLE example_table (
    id BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '1min',
    'table.datalake.auto-maintenance' = 'true'
);
```