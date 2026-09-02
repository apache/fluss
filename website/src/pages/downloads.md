# Apache Fluss Downloads

> Apache Fluss is a streaming storage built for real-time analytics & AI which can serve as the real-time data layer for Lakehouse architectures.

[Apache Fluss 0.9.1 (Incubating)](#apache-fluss-incubating-091) is the latest stable release.

<!-- TODO: remove comment when 1.0 release is ready; not made visible at the moment because this page is not versioned
## Apache Fluss 1.0

Coming soon...

## Verifying Downloads

Downloaded Apache Fluss artifacts can be verified by following [this tutorial](https://www.apache.org/info/verification.html) of the Apache Software Foundation using the Apache Fluss release-signing [KEYS](https://downloads.apache.org/fluss/KEYS).

------------------
-->
## Apache Incubator Releases

### Apache Fluss (Incubating) 0.9.1

| Artifact                                                                                                                            | Signature | SHA |
|-------------------------------------------------------------------------------------------------------------------------------------|---------|---------|
| [Fluss Binary Release](https://www.apache.org/dyn/closer.lua/incubator/fluss/fluss-0.9.1-incubating/fluss-0.9.1-incubating-bin.tgz) | [.asc](https://downloads.apache.org/incubator/fluss/fluss-0.9.1-incubating/fluss-0.9.1-incubating-bin.tgz.asc) | [.sha512](https://downloads.apache.org/incubator/fluss/fluss-0.9.1-incubating/fluss-0.9.1-incubating-bin.tgz.sha512) |
| [Fluss Source Release](https://www.apache.org/dyn/closer.lua/incubator/fluss/fluss-0.9.1-incubating/fluss-0.9.1-incubating-src.tgz) | [.asc](https://downloads.apache.org/incubator/fluss/fluss-0.9.1-incubating/fluss-0.9.1-incubating-src.tgz.asc) | [.sha512](https://downloads.apache.org/incubator/fluss/fluss-0.9.1-incubating/fluss-0.9.1-incubating-src.tgz.sha512) |
| [Fluss Helm Chart](https://www.apache.org/dyn/closer.lua/incubator/fluss/helm-chart/0.9.1-incubating/fluss-0.9.1-incubating.tgz)    | [.asc](https://downloads.apache.org/incubator/fluss/helm-chart/0.9.1-incubating/fluss-0.9.1-incubating.tgz.asc) | [.sha512](https://downloads.apache.org/incubator/fluss/helm-chart/0.9.1-incubating/fluss-0.9.1-incubating.tgz.sha512) |

Read the [release blog](/blog/releases/0.9/) about the new features and significant improvements in the Apache Fluss v0.9 release.

------------------

### Apache Fluss (Incubating) 0.8.0

| Artifact                                                                                                                            | Signature | SHA |
|-------------------------------------------------------------------------------------------------------------------------------------|---------|---------|
| [Fluss Binary Release](https://www.apache.org/dyn/closer.lua/incubator/fluss/fluss-0.8.0-incubating/fluss-0.8.0-incubating-bin.tgz) | [.asc](https://downloads.apache.org/incubator/fluss/fluss-0.8.0-incubating/fluss-0.8.0-incubating-bin.tgz.asc) | [.sha512](https://downloads.apache.org/incubator/fluss/fluss-0.8.0-incubating/fluss-0.8.0-incubating-bin.tgz.sha512) |
| [Fluss Source Release](https://www.apache.org/dyn/closer.lua/incubator/fluss/fluss-0.8.0-incubating/fluss-0.8.0-incubating-src.tgz) | [.asc](https://downloads.apache.org/incubator/fluss/fluss-0.8.0-incubating/fluss-0.8.0-incubating-src.tgz.asc) | [.sha512](https://downloads.apache.org/incubator/fluss/fluss-0.8.0-incubating/fluss-0.8.0-incubating-src.tgz.sha512) |
| [Fluss Helm Chart](https://www.apache.org/dyn/closer.lua/incubator/fluss/helm-chart/0.8.0-incubating/fluss-0.8.0-incubating.tgz)    | [.asc](https://downloads.apache.org/incubator/fluss/helm-chart/0.8.0-incubating/fluss-0.8.0-incubating.tgz.asc) | [.sha512](https://downloads.apache.org/incubator/fluss/helm-chart/0.8.0-incubating/fluss-0.8.0-incubating.tgz.sha512) |

Read the [release blog](/blog/releases/0.8/) about the new features and significant improvements in the Apache Fluss 0.8.0 release.

------------------

### Verifying Downloads

Downloaded Apache Fluss (Incubating) artifacts can be verified by following [this tutorial](https://www.apache.org/info/verification.html) of the Apache Software Foundation using the Apache Fluss (Incubating) release-signing [KEYS](https://downloads.apache.org/incubator/fluss/KEYS).

------------------

## Filesystem JARs

Fluss reaches [remote storage](/docs/maintenance/tiered-storage/remote-storage) through pluggable filesystem plugins. The binary
release already ships the HDFS, S3 and OSS plugins under `plugins/<scheme>/`, so those work out of
the box. The JARs below are for adding a filesystem that is not bundled, for upgrading a single
plugin in place, or for the Tiering Service running on Flink.

:::warning
A filesystem plugin JAR belongs in `${FLUSS_HOME}/plugins/<scheme>/` and **must not** also be copied
into `${FLUSS_HOME}/lib`. A copy on the main classpath shadows the one in the plugins directory and
is loaded without the dependencies bundled next to it, which fails at runtime with errors such as
`NoClassDefFoundError: org/apache/hadoop/hdfs/HdfsConfiguration`. See
[Troubleshooting Plugins](/docs/maintenance/troubleshooting-plugins).
:::

The following are the plugins of the latest stable release, Apache Fluss (Incubating) 0.9.1:

| Filesystem | URI scheme | In the binary release | JAR |
|------------|------------|-----------------------|-----|
| HDFS | `hdfs://` | yes | [fluss-fs-hdfs-0.9.1-incubating.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-hdfs/0.9.1-incubating/fluss-fs-hdfs-0.9.1-incubating.jar) |
| AWS S3 | `s3://` | yes | [fluss-fs-s3-0.9.1-incubating.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-s3/0.9.1-incubating/fluss-fs-s3-0.9.1-incubating.jar) |
| Aliyun OSS | `oss://` | yes | [fluss-fs-oss-0.9.1-incubating.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-oss/0.9.1-incubating/fluss-fs-oss-0.9.1-incubating.jar) |
| Google Cloud Storage | `gs://` | no | [fluss-fs-gs-0.9.1-incubating.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-gs/0.9.1-incubating/fluss-fs-gs-0.9.1-incubating.jar) |
| Azure Blob Storage | `abfs://` | no | [fluss-fs-azure-0.9.1-incubating.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-azure/0.9.1-incubating/fluss-fs-azure-0.9.1-incubating.jar) |
| HuaweiCloud OBS | `obs://` | no | [fluss-fs-obs-0.9.1-incubating.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-obs/0.9.1-incubating/fluss-fs-obs-0.9.1-incubating.jar) |

### Choosing an HDFS JAR

Two artifacts provide the `hdfs://` scheme and they differ only in whether Hadoop travels with them.
Pick one, never both:

| Artifact | Hadoop dependencies | Use it when |
|----------|---------------------|-------------|
| `fluss-fs-hdfs` | bundled, self-contained (~34 MB) | Default, and what the binary release ships. Works on machines with no Hadoop installation. |
| `fluss-fs-hadoop` | not bundled (~10 KB) | You provide Hadoop yourself through `HADOOP_CLASSPATH`, for example to match your cluster's Hadoop version or to use Kerberos. |

Deploying both at once gives two implementations of the same scheme, so use exactly one. See
[HDFS](/docs/maintenance/tiered-storage/filesystems/hdfs) for the Hadoop configuration options.
