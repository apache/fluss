---
title: "Lakehouse Storage"
sidebar_position: 3
---

# Lakehouse Storage

Lakehouse represents a new, open architecture that combines the best elements of data lakes and data warehouses.
Lakehouse combines data lake scalability and cost-effectiveness with data warehouse reliability and performance.

Fluss leverages well-known Lakehouse storage solutions like Apache Paimon, Apache Iceberg, Apache Hudi, and Delta Lake as
the tiered storage layer. Currently, only Apache Paimon is supported, but support for additional Lakehouse storage formats is planned.

Fluss's datalake tiering service continuously tiers Fluss's data to the Lakehouse storage. The data in Lakehouse storage can be read both by Fluss's client in a streaming manner and accessed directly
by external systems such as Flink, Spark, StarRocks, and others. With data tiered in Lakehouse storage, Fluss
can achieve significant storage cost reduction and analytics performance improvement.


## Enable Lakehouse Storage

Lakehouse Storage is disabled by default, so you must enable it manually.

### Lakehouse Storage Cluster Configurations
#### Modify `server.yaml`
First, you must add the lakehouse storage configuration in your `server.yaml` file. Take Paimon as an example, you must configure the following settings:
```yaml
# Paimon configuration
datalake.format: paimon

# the catalog configuration about Paimon, assuming using filesystem catalog
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon
```

Fluss processes Paimon configurations by removing the `datalake.paimon.` prefix and then uses the remaining configuration (without the prefix `datalake.paimon.`) to create the Paimon catalog. Check out the [Paimon documentation](https://paimon.apache.org/docs/1.1/maintenance/configurations/) for more details on the available configurations.

For example, if you want to configure to use Hive catalog, you can configure like the following:
```yaml
datalake.format: paimon
datalake.paimon.metastore: hive
datalake.paimon.uri: thrift://<hive-metastore-host-name>:<port>
datalake.paimon.warehouse: hdfs:///path/to/warehouse
```
#### Add other jars required by datalake
While Fluss includes the core Paimon library, you may still need to manually add additional jars to `${FLUSS_HOME}/plugins/paimon/` depending on the specific storage backend or features you plan to use.
For example, if you require OSS filesystem support, you need to add `paimon-oss-<paimon_version>.jar` to the `${FLUSS_HOME}/plugins/paimon/` directory; similarly, for S3 or HDFS support, ensure the corresponding Paimon filesystem jars are present.

### Start the Datalake Tiering Service
Then, you must start the datalake tiering service to tier Fluss's data to the lakehouse storage.
#### Prerequisites
- A running Flink cluster (currently only Flink is supported as the tiering backend).
- Download [fluss-flink-tiering-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-tiering/$FLUSS_VERSION$/fluss-flink-tiering-$FLUSS_VERSION$.jar).

#### Prepare required jars
- Download the Fluss Flink connector jar that matches your Flink version (for example, [fluss-flink-1.20-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar)) and place it into `${FLINK_HOME}/lib`.
- If you are using [Amazon S3](http://aws.amazon.com/s3/), [Aliyun OSS](https://www.aliyun.com/product/oss), or [HDFS (Hadoop Distributed File System)](https://hadoop.apache.org/docs/stable/) as Fluss's [remote storage](maintenance/tiered-storage/remote-storage.md), you need to ensure compatibility with these storage backends.
  Download the corresponding [Fluss filesystem jar](/downloads#filesystem-jars) for your storage type and place it into `${FLINK_HOME}/lib`.
- Put [fluss-lake-paimon jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-paimon/$FLUSS_VERSION$/fluss-lake-paimon-$FLUSS_VERSION$.jar) into `${FLINK_HOME}/lib`. Note: At this time, only Paimon is supported as a lakehouse storage format, so only the `fluss-lake-paimon` jar is available; support for other formats will be added in the future.
- [Download](https://flink.apache.org/downloads/) pre-bundled Hadoop jar `flink-shaded-hadoop-2-uber-${VERSION}.jar` and put into `${FLINK_HOME}/lib`.
- Download and put the required Paimon filesystem jar(s) from the [Paimon project download page](https://paimon.apache.org/docs/1.1/project/download/) into `${FLINK_HOME}/lib`. 
  For example, if you use S3 to store Paimon data, add `paimon-s3-<paimon_version>.jar` to `${FLINK_HOME}/lib`. Replace `<paimon_version>` with your actual Paimon version.
- Add any additional jars required by Paimon. For example, if you use `HiveCatalog`, ensure the related Hive jars are present in `${FLINK_HOME}/lib`.


#### Start Datalake Tiering Service
After the Flink Cluster has been started, you can submit the Fluss Flink tiering job using the following command to start the datalake tiering service:
```shell
<FLINK_HOME>/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/paimon
```

**Note:**
- The `fluss.bootstrap.servers` should be the bootstrap server address of your Fluss cluster. When starting the tiering service, you must provide all options with the `datalake.` prefix as command-line arguments, and these should match the configuration in the [server.yaml](#modify-serveryaml) file. In this case, these parameters are `--datalake.format`, `--datalake.paimon.metastore`, and `--datalake.paimon.warehouse`.
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
To enable lakehouse storage for a table, the table must be created with the option `'table.datalake.enabled' = 'true'`. Another option, `table.datalake.freshness`, allows per-table configuration of data freshness in the datalake. It defines the maximum amount of time that the datalake table's content should lag behind updates to the Fluss table. Based on this target freshness, the Fluss tiering service automatically moves data from the Fluss table and updates to the datalake table, so that the data in the datalake table is kept up to date within this target.
By default, if `table.datalake.freshness` is not explicitly set by the user, the system uses a default value of `3min`. If your data does not need to be as fresh, you can specify a longer target freshness time to reduce costs.


The following example illustrates how to set these options:

```sql title="Flink SQL"
USE CATALOG fluss_catalog;

CREATE TABLE fluss_order_with_lake (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `ptime` AS PROCTIME(),
    PRIMARY KEY (`order_key`) NOT ENFORCED
 ) WITH (
     'table.datalake.enabled' = 'true',
     'table.datalake.freshness' = '30s'
);
```
