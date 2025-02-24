/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.config;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.compression.ArrowCompressionType;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.MergeEngineType;
import com.alibaba.fluss.utils.ArrayUtils;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.fluss.config.ConfigBuilder.key;
import static com.alibaba.fluss.config.ConfigOptions.CompactionStyle.FIFO;
import static com.alibaba.fluss.config.ConfigOptions.CompactionStyle.LEVEL;
import static com.alibaba.fluss.config.ConfigOptions.CompactionStyle.NONE;
import static com.alibaba.fluss.config.ConfigOptions.CompactionStyle.UNIVERSAL;
import static com.alibaba.fluss.config.ConfigOptions.InfoLogLevel.INFO_LEVEL;
import static com.alibaba.fluss.config.ConfigOptions.NoKeyAssigner.ROUND_ROBIN;
import static com.alibaba.fluss.config.ConfigOptions.NoKeyAssigner.STICKY;

/**
 * Config options for Fluss.
 *
 * @since 0.1
 */
@PublicEvolving
public class ConfigOptions {

    @Internal
    public static final String[] PARENT_FIRST_LOGGING_PATTERNS =
            new String[] {
                "org.slf4j",
                "org.apache.log4j",
                "org.apache.logging",
                "org.apache.commons.logging",
                "ch.qos.logback"
            };

    // ------------------------------------------------------------------------
    //  ConfigOptions for Fluss Cluster
    // ------------------------------------------------------------------------
    public static final ConfigOption<Integer> DEFAULT_BUCKET_NUMBER =
            key("default.bucket.number")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The default number of buckets for a table in Fluss cluster. It's "
                                    + "a cluster-level parameter, "
                                    + "and all the tables without specifying bucket number in the cluster will use the value "
                                    + "as the bucket number.");

    public static final ConfigOption<Integer> DEFAULT_REPLICATION_FACTOR =
            key("default.replication.factor")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The default replication factor for the log of a table in Fluss cluster. It's "
                                    + "a cluster-level parameter, "
                                    + "and all the tables without specifying replication factor in the cluster will use the value "
                                    + "as replication factor.");

    public static final ConfigOption<String> REMOTE_DATA_DIR =
            key("remote.data.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The directory used for storing the kv snapshot data files and remote log for log tiered storage "
                                    + " in a Fluss supported filesystem.");

    public static final ConfigOption<MemorySize> REMOTE_FS_WRITE_BUFFER_SIZE =
            key("remote.fs.write-buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4kb"))
                    .withDescription(
                            "The default size of the write buffer for writing the local files to remote file systems.");

    public static final ConfigOption<List<String>> PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS =
            key("plugin.classloader.parent-first-patterns.default")
                    .stringType()
                    .asList()
                    .defaultValues(
                            ArrayUtils.concat(
                                    new String[] {
                                        "java.", "com.alibaba.fluss.", "javax.annotation."
                                    },
                                    PARENT_FIRST_LOGGING_PATTERNS))
                    .withDescription(
                            "A (semicolon-separated) list of patterns that specifies which classes should always be"
                                    + " resolved through the plugin parent ClassLoader first. A pattern is a simple prefix that is checked "
                                    + " against the fully qualified class name. This setting should generally not be modified.");

    public static final ConfigOption<Duration> AUTO_PARTITION_CHECK_INTERVAL =
            key("auto-partition.check.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "The interval of auto partition check. "
                                    + "The default value is 10 minutes.");

    // ------------------------------------------------------------------------
    //  ConfigOptions for Coordinator Server
    // ------------------------------------------------------------------------
    /**
     * The config parameter defining the network address to connect to for communication with the
     * coordinator server.
     *
     * <p>If the coordinator server is used as a bootstrap server (discover all the servers in the
     * cluster), the value of this config option should be a static hostname or address.
     */
    public static final ConfigOption<String> COORDINATOR_HOST =
            key("coordinator.host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The config parameter defining the network address to connect to"
                                    + " for communication with the coordinator server."
                                    + " If the coordinator server is used as a bootstrap server"
                                    + " (discover all the servers in the cluster), the value of"
                                    + " this config option should be a static hostname or address.");

    /**
     * The config parameter defining the network port to connect to for communication with the
     * coordinator server.
     *
     * <p>Like {@link ConfigOptions#COORDINATOR_HOST}, if the coordinator server is used as a
     * bootstrap server (discover all the servers in the cluster), the value of this config option
     * should be a static port. Otherwise, the value can be set to "0" for a dynamic service name
     * resolution. The value accepts a list of ports (“50100,50101”), ranges (“50100-50200”) or a
     * combination of both.
     */
    public static final ConfigOption<String> COORDINATOR_PORT =
            key("coordinator.port")
                    .stringType()
                    .defaultValue("9123")
                    .withDescription(
                            "The config parameter defining the network port to connect to"
                                    + " for communication with the coordinator server."
                                    + " Like "
                                    + COORDINATOR_HOST.key()
                                    + ", if the coordinator server is used as a bootstrap server"
                                    + " (discover all the servers in the cluster), the value of"
                                    + " this config option should be a static port. Otherwise,"
                                    + " the value can be set to \"0\" for a dynamic service name"
                                    + " resolution. The value accepts a list of ports"
                                    + " (“50100,50101”), ranges (“50100-50200”) or a combination"
                                    + " of both.");

    public static final ConfigOption<Integer> COORDINATOR_IO_POOL_SIZE =
            key("coordinator.io-pool.size")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The size of the IO thread pool to run blocking operations for coordinator server. "
                                    + "This includes discard unnecessary snapshot files. "
                                    + "Increase this value if you experience slow unnecessary snapshot files clean. "
                                    + "The default value is 1.");

    // ------------------------------------------------------------------------
    //  ConfigOptions for Tablet Server
    // ------------------------------------------------------------------------
    /** The external address of the network interface where the tablet server is exposed. */
    public static final ConfigOption<String> TABLET_SERVER_HOST =
            key("tablet-server.host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The external address of the network interface where the TabletServer is exposed."
                                    + " Because different TabletServer need different values for this option, usually it is specified in an"
                                    + " additional non-shared TabletServer-specific config file.");

    /**
     * The default network port the tablet server expects incoming IPC connections. The {@code "0"}
     * means that the TabletServer searches for a free port.
     */
    public static final ConfigOption<String> TABLET_SERVER_PORT =
            key("tablet-server.port")
                    .stringType()
                    .defaultValue("0")
                    .withDescription("The external RPC port where the TabletServer is exposed.");

    public static final ConfigOption<Integer> TABLET_SERVER_ID =
            key("tablet-server.id")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The id for the tablet server.");

    public static final ConfigOption<String> DATA_DIR =
            key("data.dir")
                    .stringType()
                    .defaultValue("/tmp/fluss-data")
                    .withDescription(
                            "This configuration controls the directory where fluss will store its data. "
                                    + "The default value is /tmp/fluss-data");

    public static final ConfigOption<Duration> WRITER_ID_EXPIRATION_TIME =
            key("server.writer-id.expiration-time")
                    .durationType()
                    .defaultValue(Duration.ofDays(7))
                    .withDescription(
                            "The time that the tablet server will wait without receiving any write request from "
                                    + "a client before expiring the related status. The default value is 7 days.");

    public static final ConfigOption<Duration> WRITER_ID_EXPIRATION_CHECK_INTERVAL =
            key("server.writer-id.expiration-check-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "The interval at which to remove writer ids that have expired due to "
                                    + WRITER_ID_EXPIRATION_TIME.key()
                                    + " passing. The default value is 10 minutes.");

    public static final ConfigOption<Integer> BACKGROUND_THREADS =
            key("server.background.threads")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The number of threads to use for various background processing tasks.");

    public static final ConfigOption<MemorySize> SERVER_BUFFER_MEMORY_SIZE =
            key("server.buffer.memory-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256mb"))
                    .withDescription(
                            "The total bytes of memory the server can use, e.g, buffer write-ahead-log rows.");

    public static final ConfigOption<MemorySize> SERVER_BUFFER_PAGE_SIZE =
            key("server.buffer.page-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128kb"))
                    .withDescription(
                            "Size of every page in memory buffers ('"
                                    + SERVER_BUFFER_MEMORY_SIZE.key()
                                    + "').");

    // ------------------------------------------------------------------
    // ZooKeeper Settings
    // ------------------------------------------------------------------

    /** The ZooKeeper address to use, when running Fluss with ZooKeeper. */
    public static final ConfigOption<String> ZOOKEEPER_ADDRESS =
            key("zookeeper.address")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The ZooKeeper address to use, when running Fluss with ZooKeeper.");

    /** The root path under which Fluss stores its entries in ZooKeeper. */
    public static final ConfigOption<String> ZOOKEEPER_ROOT =
            key("zookeeper.path.root")
                    .stringType()
                    .defaultValue("/fluss")
                    .withDescription(
                            "The root path under which Fluss stores its entries in ZooKeeper.");

    // ------------------------------------------------------------------------
    //  ZooKeeper Client Settings
    // ------------------------------------------------------------------------

    public static final ConfigOption<Integer> ZOOKEEPER_SESSION_TIMEOUT =
            key("zookeeper.client.session-timeout")
                    .intType()
                    .defaultValue(60000)
                    .withDeprecatedKeys("recovery.zookeeper.client.session-timeout")
                    .withDescription(
                            "Defines the session timeout for the ZooKeeper session in ms.");

    public static final ConfigOption<Integer> ZOOKEEPER_CONNECTION_TIMEOUT =
            key("zookeeper.client.connection-timeout")
                    .intType()
                    .defaultValue(15000)
                    .withDeprecatedKeys("recovery.zookeeper.client.connection-timeout")
                    .withDescription("Defines the connection timeout for ZooKeeper in ms.");

    public static final ConfigOption<Integer> ZOOKEEPER_RETRY_WAIT =
            key("zookeeper.client.retry-wait")
                    .intType()
                    .defaultValue(5000)
                    .withDeprecatedKeys("recovery.zookeeper.client.retry-wait")
                    .withDescription("Defines the pause between consecutive retries in ms.");

    public static final ConfigOption<Integer> ZOOKEEPER_MAX_RETRY_ATTEMPTS =
            key("zookeeper.client.max-retry-attempts")
                    .intType()
                    .defaultValue(3)
                    .withDeprecatedKeys("recovery.zookeeper.client.max-retry-attempts")
                    .withDescription(
                            "Defines the number of connection retries before the client gives up.");

    public static final ConfigOption<Boolean> ZOOKEEPER_TOLERATE_SUSPENDED_CONNECTIONS =
            key("zookeeper.client.tolerate-suspended-connections")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Defines whether a suspended ZooKeeper connection will be treated as an error that causes the leader "
                                    + "information to be invalidated or not. In case you set this option to %s, Fluss will wait until a "
                                    + "ZooKeeper connection is marked as lost before it revokes the leadership of components. This has the "
                                    + "effect that Fluss is more resilient against temporary connection instabilities at the cost of running "
                                    + "more likely into timing issues with ZooKeeper.");

    public static final ConfigOption<Boolean> ZOOKEEPER_ENSEMBLE_TRACKING =
            key("zookeeper.client.ensemble-tracker")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Defines whether Curator should enable ensemble tracker. This can be useful in certain scenarios "
                                    + "in which CuratorFramework is accessing to ZK clusters via load balancer or Virtual IPs. "
                                    + "Default Curator EnsembleTracking logic watches CuratorEventType.GET_CONFIG events and "
                                    + "changes ZooKeeper connection string. It is not desired behaviour when ZooKeeper is running under the Virtual IPs. "
                                    + "Under certain configurations EnsembleTracking can lead to setting of ZooKeeper connection string "
                                    + "with unresolvable hostnames.");

    // ------------------------------------------------------------------------
    //  ConfigOptions for Log
    // ------------------------------------------------------------------------

    public static final ConfigOption<MemorySize> LOG_SEGMENT_FILE_SIZE =
            key("log.segment.file-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1024m"))
                    .withDescription(
                            "This configuration controls the segment file size for the log. "
                                    + "Retention and cleaning is always done a file at a time so a "
                                    + "larger segment size means fewer files but less granular control over retention.");

    public static final ConfigOption<MemorySize> LOG_INDEX_FILE_SIZE =
            key("log.index.file-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("10m"))
                    .withDescription(
                            "This configuration controls the size of the index that maps offsets to file positions. "
                                    + "We preallocate this index file and shrink it only after log rolls. You generally "
                                    + "should not need to change this setting.");

    public static final ConfigOption<MemorySize> LOG_INDEX_INTERVAL_SIZE =
            key("log.index.interval-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4k"))
                    .withDescription(
                            "This setting controls how frequently fluss adds an index entry to its offset index. "
                                    + "The default setting ensures that we index a message roughly every 4096 bytes. "
                                    + "More indexing allows reads to jump closer to the exact position in the log but "
                                    + "makes the index larger. You probably don't need to change this.");

    public static final ConfigOption<Boolean> LOG_FILE_PREALLOCATE =
            key("log.file-preallocate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "True if we should preallocate the file on disk when creating a new log segment.");

    public static final ConfigOption<Long> LOG_FLUSH_INTERVAL_MESSAGES =
            key("log.flush.interval-messages")
                    .longType()
                    .defaultValue(Long.MAX_VALUE)
                    .withDescription(
                            "This setting allows specifying an interval at which we will force a "
                                    + "fsync of data written to the log. For example if this was set to 1, "
                                    + "we would fsync after every message; if it were 5 we would fsync after every "
                                    + "five messages.");

    public static final ConfigOption<Duration> LOG_REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL =
            key("log.replica.high-watermark.checkpoint-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription(
                            "The frequency with which the high watermark is saved out to disk. "
                                    + "The default setting is 5 seconds.");

    public static final ConfigOption<Duration> LOG_REPLICA_MAX_LAG_TIME =
            key("log.replica.max-lag-time")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "If a follower replica hasn't sent any fetch log requests or hasn't "
                                    + "consumed up the leaders log end offset for at least this time, "
                                    + "the leader will remove the follower replica form isr");

    public static final ConfigOption<Integer> LOG_REPLICA_WRITE_OPERATION_PURGE_NUMBER =
            key("log.replica.write-operation-purge-number")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The purge number (in number of requests) of the write operation manager, "
                                    + "the default value is 1000.");

    public static final ConfigOption<Integer> LOG_REPLICA_FETCH_OPERATION_PURGE_NUMBER =
            key("log.replica.fetch-operation-purge-number")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The purge number (in number of requests) of the fetch log operation manager, "
                                    + "the default value is 1000.");

    public static final ConfigOption<Integer> LOG_REPLICA_FETCHER_NUMBER =
            key("log.replica.fetcher-number")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Number of fetcher threads used to replicate log records from each source tablet server. "
                                    + "The total number of fetchers on each tablet server is bound by this parameter"
                                    + " multiplied by the number of tablet servers in the cluster. Increasing this "
                                    + "value can increase the degree of I/O parallelism in the follower and leader "
                                    + "tablet server at the cost of higher CPU and memory utilization.");

    public static final ConfigOption<Duration> LOG_REPLICA_FETCH_BACKOFF_INTERVAL =
            key("log.replica.fetch.backoff-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("The amount of time to sleep when fetch bucket error occurs.")
                    .withFallbackKeys("log.replica.fetch-backoff-interval");

    public static final ConfigOption<MemorySize> LOG_REPLICA_FETCH_MAX_BYTES =
            key("log.replica.fetch.max-bytes")
                    .memoryType()
                    .defaultValue(MemorySize.parse("16mb"))
                    .withDescription(
                            "The maximum amount of data the server should return for a fetch request from follower. "
                                    + "Records are fetched in batches, and if the first record batch in the first "
                                    + "non-empty bucket of the fetch is larger than this value, the record batch "
                                    + "will still be returned to ensure that the fetch can make progress. As such, "
                                    + "this is not a absolute maximum. Note that the fetcher performs multiple fetches "
                                    + "in parallel.")
                    .withDeprecatedKeys("log.fetch.max-bytes");

    public static final ConfigOption<MemorySize> LOG_REPLICA_FETCH_MAX_BYTES_FOR_BUCKET =
            key("log.replica.fetch.max-bytes-for-bucket")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1mb"))
                    .withDescription(
                            "The maximum amount of data the server should return for a table bucket in fetch request "
                                    + "from follower. Records are fetched in batches, the max bytes size is "
                                    + "config by this option.")
                    .withDeprecatedKeys("log.fetch.max-bytes-for-bucket");

    public static final ConfigOption<Duration> LOG_REPLICA_FETCH_WAIT_MAX_TIME =
            key("log.replica.fetch.wait-max-time")
                    .durationType()
                    .defaultValue(Duration.ofMillis(500))
                    .withDescription(
                            "The maximum time to wait for enough bytes to be available for a fetch log request "
                                    + "from follower to response. This value should always be less than the "
                                    + "'log.replica.max-lag-time' at all times to prevent frequent shrinking of ISR for "
                                    + "low throughput tables");

    public static final ConfigOption<MemorySize> LOG_REPLICA_FETCH_MIN_BYTES =
            key("log.replica.fetch.min-bytes")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1b"))
                    .withDescription(
                            "The minimum bytes expected for each fetch log request from follower to response. "
                                    + "If not enough bytes, wait up to "
                                    + LOG_REPLICA_FETCH_WAIT_MAX_TIME.key()
                                    + " time to return.");

    public static final ConfigOption<Integer> LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER =
            key("log.replica.min-in-sync-replicas-number")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "When a producer set acks to all (-1), this configuration specifies the "
                                    + "minimum number of replicas that must acknowledge a write for "
                                    + "the write to be considered successful. If this minimum cannot be met, "
                                    + "then the producer will raise an exception(NotEnoughReplicas). "
                                    + "when used together, this config and 'acks' allow you to "
                                    + "enforce greater durability guarantees. A typical scenario would be "
                                    + "to create a table with a replication factor of 3. set this conf to 2, and "
                                    + "produce with acks = -1. This will ensure that the producer raises an "
                                    + "exception if a majority of replicas don't receive a write.");

    // ------------------------------------------------------------------------
    //  ConfigOptions for Log tiered storage
    // ------------------------------------------------------------------------

    public static final ConfigOption<Duration> REMOTE_LOG_TASK_INTERVAL_DURATION =
            key("remote.log.task-interval-duration")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "Interval at which remote log manager runs the scheduled tasks like "
                                    + "copy segments, clean up remote log segments, delete local log segments etc. "
                                    + "If the value is set to 0, it means that the remote log storage is disabled.");

    public static final ConfigOption<MemorySize> REMOTE_LOG_INDEX_FILE_CACHE_SIZE =
            key("remote.log.index-file-cache-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1gb"))
                    .withDescription(
                            "The total size of the space allocated to store index files fetched "
                                    + "from remote storage in the local storage.");

    public static final ConfigOption<Integer> REMOTE_LOG_MANAGER_THREAD_POOL_SIZE =
            key("remote.log-manager.thread-pool-size")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Size of the thread pool used in scheduling tasks to copy segments, "
                                    + "fetch remote log indexes and clean up remote log segments.");

    public static final ConfigOption<Integer> REMOTE_LOG_DATA_TRANSFER_THREAD_NUM =
            key("remote.log.data-transfer-thread-num")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The number of threads the server uses to transfer (download and upload) "
                                    + "remote log file can be  data file, index file and remote log metadata file.");

    // ------------------------------------------------------------------------
    //  Netty Settings
    // ------------------------------------------------------------------------

    public static final ConfigOption<Integer> NETTY_SERVER_NUM_NETWORK_THREADS =
            key("netty.server.num-network-threads")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The number of threads that the server uses for receiving requests "
                                    + "from the network and sending responses to the network.");

    public static final ConfigOption<Integer> NETTY_SERVER_NUM_WORKER_THREADS =
            key("netty.server.num-worker-threads")
                    .intType()
                    .defaultValue(8)
                    .withDescription(
                            "The number of threads that the server uses for processing requests, "
                                    + "which may include disk and remote I/O.");

    public static final ConfigOption<Integer> NETTY_SERVER_MAX_QUEUED_REQUESTS =
            key("netty.server.max-queued-requests")
                    .intType()
                    .defaultValue(500)
                    .withDescription(
                            "The number of queued requests allowed for worker threads, before blocking the I/O threads.");

    public static final ConfigOption<Duration> NETTY_CONNECTION_MAX_IDLE_TIME =
            key("netty.connection.max-idle-time")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "Close idle connections after the number of milliseconds specified by this config.");

    public static final ConfigOption<Integer> NETTY_CLIENT_NUM_NETWORK_THREADS =
            key("netty.client.num-network-threads")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of threads that the client uses for sending requests to the "
                                    + "network and receiving responses from network. The default value is 1");

    // ------------------------------------------------------------------------
    //  Client Settings
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> CLIENT_ID =
            key("client.id")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "An id string to pass to the server when making requests. The purpose of this is "
                                    + "to be able to track the source of requests beyond just ip/port by allowing "
                                    + "a logical application name to be included in server-side request logging.");

    public static final ConfigOption<Duration> CLIENT_CONNECT_TIMEOUT =
            key("client.connect-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(120))
                    .withDescription("The Netty client connect timeout.");

    public static final ConfigOption<List<String>> BOOTSTRAP_SERVERS =
            key("bootstrap.servers")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A list of host/port pairs to use for establishing the initial connection to the Fluss cluster. "
                                    + "The list should be in the form host1:port1,host2:port2,.... "
                                    + "Since these servers are just used for the initial connection to discover the full cluster membership (which may change dynamically), "
                                    + "this list need not contain the full set of servers (you may want more than one, though, in case a server is down) ");

    public static final ConfigOption<MemorySize> CLIENT_WRITER_BUFFER_MEMORY_SIZE =
            key("client.writer.buffer.memory-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription(
                            "The total bytes of memory the writer can use to buffer internal rows.");

    public static final ConfigOption<MemorySize> CLIENT_WRITER_BUFFER_PAGE_SIZE =
            key("client.writer.buffer.page-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128kb"))
                    .withDescription(
                            "Size of every page in memory buffers ('"
                                    + CLIENT_WRITER_BUFFER_MEMORY_SIZE.key()
                                    + "').");

    public static final ConfigOption<MemorySize> CLIENT_WRITER_BATCH_SIZE =
            key("client.writer.batch-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription(
                            "The writer or walBuilder will attempt to batch records together into one batch for"
                                    + " the same bucket. This helps performance on both the client and the server.");

    @Deprecated
    public static final ConfigOption<MemorySize> CLIENT_WRITER_LEGACY_BATCH_SIZE =
            key("client.writer.legacy.batch-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64kb"))
                    .withDescription(
                            "The writer or walBuilder will attempt to batch records together into one batch for"
                                    + " the same bucket. This helps performance on both the client and the server.");

    public static final ConfigOption<Duration> CLIENT_WRITER_BATCH_TIMEOUT =
            key("client.writer.batch-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDescription(
                            "The writer groups ay rows that arrive in between request sends into a single batched"
                                    + " request. Normally this occurs only under load when rows arrive faster than they "
                                    + "can be sent out. However in some circumstances the writer may want to"
                                    + "reduce the number of requests even under moderate load. This setting accomplishes"
                                    + " this by adding a small amount of artificial delay, that is, rather than "
                                    + "immediately sending out a row, the writer will wait for up to the given "
                                    + "delay to allow other records to be sent so that the sends can be batched together. "
                                    + "This can be thought of as analogous to Nagle's algorithm in TCP. This setting "
                                    + "gives the upper bound on the delay for batching: once we get "
                                    + CLIENT_WRITER_BATCH_SIZE.key()
                                    + " worth of rows for a bucket it will be sent immediately regardless of this setting, "
                                    + "however if we have fewer than this many bytes accumulated for this bucket we will delay"
                                    + " for the specified time waiting for more records to show up. This setting defaults "
                                    + "to 100ms");

    public static final ConfigOption<NoKeyAssigner> CLIENT_WRITER_BUCKET_NO_KEY_ASSIGNER =
            key("client.writer.bucket.no-key-assigner")
                    .enumType(NoKeyAssigner.class)
                    .defaultValue(STICKY)
                    .withDescription(
                            String.format(
                                    "The bucket assigner for no key table. For table with bucket key or primary key, "
                                            + "we choose a bucket based on a hash of the key. For these table without "
                                            + "bucket key and primary key, we can use this option to specify bucket "
                                            + "assigner, the candidate assigner is %s, "
                                            + "the default assigner is %s.\n"
                                            + ROUND_ROBIN.name()
                                            + ": this strategy will assign the bucket id for the input row by round robin.\n"
                                            + STICKY.name()
                                            + ": this strategy will assign new bucket id only if the batch changed in record accumulator, "
                                            + "otherwise the bucket id will be the same as the front record.",
                                    Arrays.toString(NoKeyAssigner.values()),
                                    STICKY.name()));

    public static final ConfigOption<String> CLIENT_WRITER_ACKS =
            key("client.writer.acks")
                    .stringType()
                    .defaultValue("all")
                    .withDescription(
                            "The number of acknowledgments the writer requires the leader to have received before"
                                    + " considering a request complete. This controls the durability of records that "
                                    + "are sent. The following settings are allowed:\n"
                                    + "acks=0: If set to 0, then the writer will not wait for any acknowledgment "
                                    + "from the server at all. No guarantee can be mode that the server has received "
                                    + "the record in this case.\n"
                                    + "acks=1: This will mean the leader will write the record to its local log but "
                                    + "will respond without awaiting full acknowledge the record but before the followers"
                                    + " have replicated it then the record will be lost.\n"
                                    + "acks=-1 (all): This will mean the leader will wait for the full ser of in-sync "
                                    + "replicas to acknowledge the record. This guarantees that the record will not be "
                                    + "lost as long as at least one in-sync replica remains alive, This is the strongest"
                                    + " available guarantee.");

    public static final ConfigOption<MemorySize> CLIENT_WRITER_REQUEST_MAX_SIZE =
            key("client.writer.request-max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("10mb"))
                    .withDescription(
                            "The maximum size of a request in bytes. This setting will limit the number of "
                                    + "record batches the writer will send in a single request to avoid sending "
                                    + "huge requests. Note that this retry is no different than if the writer resent "
                                    + "the row upon receiving the error.");

    public static final ConfigOption<Integer> CLIENT_WRITER_RETRIES =
            key("client.writer.retries")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            "Setting a value greater than zero will cause the client to resend any record whose "
                                    + "send fails with a potentially transient error.");

    public static final ConfigOption<Boolean> CLIENT_WRITER_ENABLE_IDEMPOTENCE =
            key("client.writer.enable-idempotence")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enable idempotence for the writer. When idempotence is enabled, the writer "
                                    + "will ensure that exactly one copy of each record is written in the stream. "
                                    + "When idempotence is disabled, the writer retries due to server failures, "
                                    + "etc., may write duplicates of the retried record in the stream. Note that "
                                    + "enabling writer idempotence requires "
                                    + CLIENT_WRITER_RETRIES.key()
                                    + " to be greater than 0, and "
                                    + CLIENT_WRITER_ACKS.key()
                                    + " must be 'all'.\n"
                                    + "Writer idempotence is enabled by default if no conflicting config are set. "
                                    + "If conflicting config are set and writer idempotence is not explicitly enabled, "
                                    + "idempotence is disabled. If idempotence is explicitly enabled and conflicting "
                                    + "config are set, a ConfigException is thrown");

    public static final ConfigOption<Integer> CLIENT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET =
            key("client.writer.max-inflight-requests-per-bucket")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The maximum number of unacknowledged requests per bucket for writer. This configuration can work only if "
                                    + CLIENT_WRITER_ENABLE_IDEMPOTENCE.key()
                                    + " is set to true. When the number of inflight "
                                    + "requests per bucket exceeds this setting, the writer will wait for the inflight "
                                    + "requests to complete before sending out new requests. This setting defaults to 5");

    public static final ConfigOption<Duration> CLIENT_REQUEST_TIMEOUT =
            key("client.request-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The timeout for a request to complete. If user set the write ack to -1, "
                                    + "this timeout is the max time that delayed write try to complete. "
                                    + "The default setting is 30 seconds.");

    public static final ConfigOption<Boolean> CLIENT_SCANNER_LOG_CHECK_CRC =
            key("client.scanner.log.check-crc")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Automatically check the CRC3 of the read records for LogScanner. This ensures no on-the-wire "
                                    + "or on-disk corruption to the messages occurred. This check "
                                    + "adds some overhead, so it may be disabled in cases seeking extreme performance.");

    public static final ConfigOption<Integer> CLIENT_SCANNER_LOG_MAX_POLL_RECORDS =
            key("client.scanner.log.max-poll-records")
                    .intType()
                    .defaultValue(500)
                    .withDescription(
                            "The maximum number of records returned in a single call to poll() for LogScanner. "
                                    + "Note that this config doesn't impact the underlying fetching behavior. "
                                    + "The Scanner will cache the records from each fetch request and returns "
                                    + "them incrementally from each poll.");

    public static final ConfigOption<MemorySize> CLIENT_SCANNER_LOG_FETCH_MAX_BYTES =
            key("client.scanner.log.fetch.max-bytes")
                    .memoryType()
                    .defaultValue(MemorySize.parse("16mb"))
                    .withDescription(
                            "The maximum amount of data the server should return for a fetch request from client. "
                                    + "Records are fetched in batches, and if the first record batch in the first "
                                    + "non-empty bucket of the fetch is larger than this value, the record batch "
                                    + "will still be returned to ensure that the fetch can make progress. As such, "
                                    + "this is not a absolute maximum.");

    public static final ConfigOption<MemorySize> CLIENT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET =
            key("client.scanner.log.fetch.max-bytes-for-bucket")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1mb"))
                    .withDescription(
                            "The maximum amount of data the server should return for a table bucket in fetch request "
                                    + "from client. Records are fetched in batches, the max bytes size is config by "
                                    + "this option.");

    public static final ConfigOption<Duration> CLIENT_SCANNER_LOG_FETCH_WAIT_MAX_TIME =
            key("client.scanner.log.fetch.wait-max-time")
                    .durationType()
                    .defaultValue(Duration.ofMillis(500))
                    .withDescription(
                            "The maximum time to wait for enough bytes to be available for a fetch log "
                                    + "request from client to response.");

    public static final ConfigOption<MemorySize> CLIENT_SCANNER_LOG_FETCH_MIN_BYTES =
            key("client.scanner.log.fetch.min-bytes")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1b"))
                    .withDescription(
                            "The minimum bytes expected for each fetch log request from client to response. "
                                    + "If not enough bytes, wait up to "
                                    + CLIENT_SCANNER_LOG_FETCH_WAIT_MAX_TIME.key()
                                    + " time to return.");

    public static final ConfigOption<Integer> CLIENT_LOOKUP_QUEUE_SIZE =
            key("client.lookup.queue-size")
                    .intType()
                    .defaultValue(25600)
                    .withDescription("The maximum number of pending lookup operations.");

    public static final ConfigOption<Integer> CLIENT_LOOKUP_MAX_BATCH_SIZE =
            key("client.lookup.max-batch-size")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "The maximum batch size of merging lookup operations to one lookup request.");

    public static final ConfigOption<Integer> CLIENT_LOOKUP_MAX_INFLIGHT_SIZE =
            key("client.lookup.max-inflight-requests")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "The maximum number of unacknowledged lookup requests for lookup operations.");

    public static final ConfigOption<Duration> CLIENT_LOOKUP_BATCH_TIMEOUT =
            key("client.lookup.batch-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDescription(
                            "The maximum time to wait for the lookup batch to full, if this timeout is reached, "
                                    + "the lookup batch will be closed to send.");

    public static final ConfigOption<Integer> CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM =
            key("client.scanner.remote-log.prefetch-num")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The number of remote log segments to keep in local temp file for LogScanner, "
                                    + "which download from remote storage. The default setting is 4.");

    public static final ConfigOption<String> CLIENT_SCANNER_IO_TMP_DIR =
            key("client.scanner.io.tmpdir")
                    .stringType()
                    .defaultValue(System.getProperty("java.io.tmpdir") + "/fluss")
                    .withDescription(
                            "Local directory that is used by client for"
                                    + " storing the data files (like kv snapshot, log segment files) to read temporarily");

    public static final ConfigOption<Integer> REMOTE_FILE_DOWNLOAD_THREAD_NUM =
            key("client.remote-file.download-thread-num")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The number of threads the client uses to download remote files.");

    public static final ConfigOption<Duration> FILESYSTEM_SECURITY_TOKEN_RENEWAL_RETRY_BACKOFF =
            key("client.filesystem.security.token.renewal.backoff")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription(
                            "The time period how long to wait before retrying to obtain new security tokens "
                                    + "for filesystem after a failure.");

    public static final ConfigOption<Double> FILESYSTEM_SECURITY_TOKEN_RENEWAL_TIME_RATIO =
            key("client.filesystem.security.token.renewal.time-ratio")
                    .doubleType()
                    .defaultValue(0.75)
                    .withDescription(
                            "Ratio of the tokens's expiration time when new credentials for access filesystem should be re-obtained.");

    public static final ConfigOption<Boolean> CLIENT_METRICS_ENABLED =
            key("client.metrics.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enable metrics for client. When metrics is enabled, the client "
                                    + "will collect metrics and report by the JMX metrics reporter.");

    // ------------------------------------------------------------------------
    //  ConfigOptions for Fluss Table
    // ------------------------------------------------------------------------
    public static final ConfigOption<Integer> TABLE_REPLICATION_FACTOR =
            key("table.replication.factor")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The replication factor for the log of the new table. When it's not set, Fluss "
                                    + "will use the cluster's default replication factor configured by "
                                    + DEFAULT_REPLICATION_FACTOR.key()
                                    + ". It should be a positive number and not larger than the number of tablet servers in the "
                                    + "Fluss cluster. A value larger than the number of tablet servers in Fluss cluster "
                                    + "will result in an error when the new table is created.");

    public static final ConfigOption<LogFormat> TABLE_LOG_FORMAT =
            key("table.log.format")
                    .enumType(LogFormat.class)
                    .defaultValue(LogFormat.ARROW)
                    .withDescription(
                            "The format of the log records in log store. The default value is 'arrow'. "
                                    + "The supported formats are 'arrow' and 'indexed'.");

    public static final ConfigOption<ArrowCompressionType> TABLE_LOG_ARROW_COMPRESSION_TYPE =
            key("table.log.arrow.compression.type")
                    .enumType(ArrowCompressionType.class)
                    // TODO: change to ZSTD by default when it is stable
                    .defaultValue(ArrowCompressionType.NONE)
                    .withDescription(
                            "The compression type of the log records if the log format is set to 'ARROW'. "
                                    + "The candidate compression type is "
                                    + Arrays.toString(ArrowCompressionType.values()));

    public static final ConfigOption<Integer> TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL =
            key("table.log.arrow.compression.zstd.level")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The compression level of ZSTD for the log records if the log format is set to 'ARROW' "
                                    + "and the compression type is set to 'ZSTD'. The valid range is 1 to 22.");

    public static final ConfigOption<KvFormat> TABLE_KV_FORMAT =
            key("table.kv.format")
                    .enumType(KvFormat.class)
                    .defaultValue(KvFormat.COMPACTED)
                    .withDescription(
                            "The format of the kv records in kv store. The default value is 'compacted'. "
                                    + "The supported formats are 'compacted' and 'indexed'.");

    public static final ConfigOption<Boolean> TABLE_AUTO_PARTITION_ENABLED =
            key("table.auto-partition.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether enable auto partition for the table. Disable by default."
                                    + " When auto partition is enabled, the partitions of the table will be created automatically.");

    public static final ConfigOption<AutoPartitionTimeUnit> TABLE_AUTO_PARTITION_TIME_UNIT =
            key("table.auto-partition.time-unit")
                    .enumType(AutoPartitionTimeUnit.class)
                    .noDefaultValue()
                    .withDescription(
                            "The time granularity for auto created partitions. "
                                    + "Valid values are 'HOUR', 'DAY', 'MONTH', 'QUARTER', 'YEAR'. "
                                    + "If the value is 'HOUR', the partition format for "
                                    + "auto created is yyyyMMddHH. "
                                    + "If the value is 'DAY', the partition format for "
                                    + "auto created is yyyyMMdd. "
                                    + "If the value is 'MONTH', the partition format for "
                                    + "auto created is yyyyMM. "
                                    + "If the value is 'QUARTER', the partition format for "
                                    + "auto created is yyyyQ. "
                                    + "If the value is 'YEAR', the partition format for "
                                    + "auto created is yyyy.");

    public static final ConfigOption<String> TABLE_AUTO_PARTITION_TIMEZONE =
            key("table.auto-partition.time-zone")
                    .stringType()
                    .defaultValue(ZoneId.systemDefault().getId())
                    .withDescription(
                            "The time zone for auto partitions, which is by default the same as the system time zone.");

    public static final ConfigOption<Integer> TABLE_AUTO_PARTITION_NUM_PRECREATE =
            key("table.auto-partition.num-precreate")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The number of partitions to pre-create for auto created partitions in each check for auto partition. "
                                    + "For example, if the current check time is 2024-11-11 and the value is "
                                    + "configured as 3, then partitions 20241111, 20241112, 20241113 will be pre-created. "
                                    + "If any one partition exists, it'll skip creating the partition.");

    public static final ConfigOption<Integer> TABLE_AUTO_PARTITION_NUM_RETENTION =
            key("table.auto-partition.num-retention")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The number of history partitions to retain for auto created partitions in each check for auto partition. "
                                    + "The default value is -1 which means retain all partitions. "
                                    + "For example, if the current check time is 2024-11-11, time-unit is DAY, and the value is "
                                    + "configured as 3, then the history partitions 20241108, 20241109, 20241110 will be retained. "
                                    + "The partitions earlier than 20241108 will be deleted.");

    public static final ConfigOption<Duration> TABLE_LOG_TTL =
            key("table.log.ttl")
                    .durationType()
                    .defaultValue(Duration.ofDays(7))
                    .withDescription(
                            "The time to live for log segments. The configuration controls the maximum time "
                                    + "we will retain a log before we will delete old segments to free up "
                                    + "space. If set to -1, the log will not be deleted.");

    public static final ConfigOption<Integer> TABLE_TIERED_LOG_LOCAL_SEGMENTS =
            key("table.log.tiered.local-segments")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "The number of log segments to retain in local for each table when log tiered storage is enabled. "
                                    + "It must be greater that 0. The default is 2.");

    public static final ConfigOption<Boolean> TABLE_DATALAKE_ENABLED =
            key("table.datalake.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether enable lakehouse storage for the table. Disabled by default. "
                                    + "When this option is set to ture and the datalake tiering service is up,"
                                    + " the table will be tiered and compacted into datalake format stored on lakehouse storage.");

    public static final ConfigOption<DataLakeFormat> TABLE_DATALAKE_FORMAT =
            key("table.datalake.format")
                    .enumType(DataLakeFormat.class)
                    .noDefaultValue()
                    .withDescription(
                            "The format of the datalake that the Fluss cluster uses as lake storage for the table."
                                    + " It will be set with the Fluss's configuration 'datalake.format' by Fluss while creating the table. "
                                    + "If non-null, the data distribution will follow the strategy of the corresponding datalake uses.");

    public static final ConfigOption<MergeEngineType> TABLE_MERGE_ENGINE =
            key("table.merge-engine")
                    .enumType(MergeEngineType.class)
                    .noDefaultValue()
                    .withDescription(
                            "Defines the merge engine for the primary key table. By default, primary key table doesn't have merge engine. "
                                    + "The supported merge engines are 'first_row' and 'versioned'. "
                                    + "The 'first_row' merge engine will keep the first row of the same primary key. "
                                    + "The 'versioned' merge engine will keep the row with the largest version of the same primary key.");

    public static final ConfigOption<String> TABLE_MERGE_ENGINE_VERSION_COLUMN =
            // we may need to introduce "del-column" in the future to support delete operation
            key("table.merge-engine.versioned.ver-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The column name of the version column for the 'versioned' merge engine. "
                                    + "If the merge engine is set to 'versioned', the version column must be set.");

    // ------------------------------------------------------------------------
    //  ConfigOptions for Kv
    // ------------------------------------------------------------------------
    public static final ConfigOption<Duration> KV_SNAPSHOT_INTERVAL =
            key("kv.snapshot.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "The interval to perform periodic snapshot for kv data. "
                                    + "The default setting is 10 minutes.");

    public static final ConfigOption<Integer> KV_SNAPSHOT_SCHEDULER_THREAD_NUM =
            key("kv.snapshot.scheduler-thread-num")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of threads that the server uses to schedule snapshot kv data for all the replicas in the server.");

    public static final ConfigOption<Integer> KV_SNAPSHOT_TRANSFER_THREAD_NUM =
            key("kv.snapshot.transfer-thread-num")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The number of threads the server uses to transfer (download and upload) kv snapshot files.");

    public static final ConfigOption<Integer> KV_MAX_RETAINED_SNAPSHOTS =
            key("kv.snapshot.num-retained")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The maximum number of completed snapshots to retain.");

    public static final ConfigOption<Integer> KV_MAX_BACKGROUND_THREADS =
            key("kv.rocksdb.thread.num")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "The maximum number of concurrent background flush and compaction jobs (per bucket of table). "
                                    + "The default value is '2'.");

    public static final ConfigOption<Integer> KV_MAX_OPEN_FILES =
            key("kv.rocksdb.files.open")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The maximum number of open files (per  bucket of table) that can be used by the DB, '-1' means no limit. "
                                    + "The default value is '-1'.");

    public static final ConfigOption<MemorySize> KV_LOG_MAX_FILE_SIZE =
            key("kv.rocksdb.log.max-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("25mb"))
                    .withDescription(
                            "The maximum size of RocksDB's file used for information logging. "
                                    + "If the log files becomes larger than this, a new file will be created. "
                                    + "If 0, all logs will be written to one log file. "
                                    + "The default maximum file size is '25MB'. ");

    public static final ConfigOption<Integer> KV_LOG_FILE_NUM =
            key("kv.rocksdb.log.file-num")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The maximum number of files RocksDB should keep for information logging (Default setting: 4).");

    public static final ConfigOption<String> KV_LOG_DIR =
            key("kv.rocksdb.log.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The directory for RocksDB's information logging files. "
                                    + "If empty (Fluss default setting), log files will be in the same directory as the Fluss log. "
                                    + "If non-empty, this directory will be used and the data directory's absolute path will be used as the prefix of the log file name. "
                                    + "If setting this option as a non-existing location, e.g '/dev/null', RocksDB will then create the log under its own database folder as before.");

    public static final ConfigOption<InfoLogLevel> KV_LOG_LEVEL =
            key("kv.rocksdb.log.level")
                    .enumType(InfoLogLevel.class)
                    .defaultValue(INFO_LEVEL)
                    .withDescription(
                            String.format(
                                    "The specified information logging level for RocksDB. "
                                            + "Candidate log level is %s. If unset, Fluss will use %s. "
                                            + "Note: RocksDB info logs will not be written to the Fluss's tablet server logs and there "
                                            + "is no rolling strategy, unless you configure %s, %s, and %s accordingly. "
                                            + "Without a rolling strategy, it may lead to uncontrolled "
                                            + "disk space usage if configured with increased log levels! "
                                            + "There is no need to modify the RocksDB log level, unless for troubleshooting RocksDB.",
                                    Arrays.toString(InfoLogLevel.values()),
                                    INFO_LEVEL,
                                    KV_LOG_DIR.key(),
                                    KV_LOG_MAX_FILE_SIZE.key(),
                                    KV_LOG_FILE_NUM.key()));

    public static final ConfigOption<MemorySize> KV_WRITE_BATCH_SIZE =
            key("kv.rocksdb.write-batch-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription(
                            "The max size of the consumed memory for RocksDB batch write, "
                                    + "will flush just based on item count if this config set to 0.");

    // --------------------------------------------------------------------------
    // Provided configurable ColumnFamilyOptions within Fluss
    // --------------------------------------------------------------------------

    public static final ConfigOption<CompactionStyle> KV_COMPACTION_STYLE =
            key("kv.rocksdb.compaction.style")
                    .enumType(CompactionStyle.class)
                    .defaultValue(LEVEL)
                    .withDescription(
                            String.format(
                                    "The specified compaction style for DB. Candidate compaction style is %s, %s, %s or %s, "
                                            + "and Fluss chooses '%s' as default style.",
                                    LEVEL.name(),
                                    FIFO.name(),
                                    UNIVERSAL.name(),
                                    NONE.name(),
                                    LEVEL.name()));

    public static final ConfigOption<Boolean> KV_USE_DYNAMIC_LEVEL_SIZE =
            key("kv.rocksdb.compaction.level.use-dynamic-size")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, RocksDB will pick target size of each level dynamically. From an empty DB, "
                                    + "RocksDB would make last level the base level, which means merging L0 data into the last level, "
                                    + "until it exceeds max_bytes_for_level_base. And then repeat this process for second last level and so on. "
                                    + "The default value is 'false'. "
                                    + "For more information, please refer to %s https://github.com/facebook/rocksdb/wiki/Leveled-Compaction#level_compaction_dynamic_level_bytes-is-true"
                                    + "RocksDB's doc.");

    public static final ConfigOption<List<KvCompressionType>> KV_COMPRESSION_PER_LEVEL =
            key("kv.rocksdb.compression.per.level")
                    .enumType(KvCompressionType.class)
                    .asList()
                    .defaultValues(
                            KvCompressionType.LZ4,
                            KvCompressionType.LZ4,
                            KvCompressionType.LZ4,
                            KvCompressionType.LZ4,
                            KvCompressionType.LZ4,
                            KvCompressionType.ZSTD,
                            KvCompressionType.ZSTD)
                    .withDescription(
                            "A comma-separated list of Compression Type. Different levels can have different "
                                    + "compression policies. In many cases, lower levels use fast compression algorithms,"
                                    + " while higher levels with more data use slower but more effective compression algorithms. "
                                    + "The N th element in the List corresponds to the compression type of the level N-1"
                                    + "When 'kv.rocksdb.compaction.level.use-dynamic-size' is true, compression_per_level[0] still determines L0, but other "
                                    + "elements are based on the base level and may not match the level seen in the info log. "
                                    + "Note: If the List size is smaller than the level number, the undefined lower level uses the last Compression Type in the List. "
                                    + "The optional values include NO, SNAPPY, LZ4, ZSTD. "
                                    + "For more information about compression type, please refer to doc https://github.com/facebook/rocksdb/wiki/Compression. "
                                    + "The default value is ‘LZ4,LZ4,LZ4,LZ4,LZ4,ZSTD,ZSTD’, indicates there is lz4 compaction of level0 and level4，"
                                    + "ZSTD compaction algorithm is used from level5 to level6. "
                                    + "LZ4 is a lightweight compression algorithm so it usually strikes a good balance between space and CPU usage.  "
                                    + "ZSTD is more space save than LZ4, but it is more CPU-intensive. "
                                    + "Different machines deploy compaction modes according to CPU and I/O resources. The default value is for the scenario that "
                                    + "CPU resources are adequate. If you find the IO pressure of the system is not big when writing a lot of data,"
                                    + " but CPU resources are inadequate, you can exchange I/O resources for CPU resources and change the compaction mode to 'NO,NO,NO,LZ4,LZ4,ZSTD,ZSTD'. ");

    public static final ConfigOption<MemorySize> KV_TARGET_FILE_SIZE_BASE =
            key("kv.rocksdb.compaction.level.target-file-size-base")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription(
                            "The target file size for compaction, which determines a level-1 file size. "
                                    + "The default value is '64MB'.");

    public static final ConfigOption<MemorySize> KV_MAX_SIZE_LEVEL_BASE =
            key("kv.rocksdb.compaction.level.max-size-level-base")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256mb"))
                    .withDescription(
                            "The upper-bound of the total size of level base files in bytes. "
                                    + "The default value is '256MB'.");

    public static final ConfigOption<MemorySize> KV_WRITE_BUFFER_SIZE =
            key("kv.rocksdb.writebuffer.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription(
                            "The amount of data built up in memory (backed by an unsorted log on disk) "
                                    + "before converting to a sorted on-disk files. The default writebuffer size is '64MB'.");

    public static final ConfigOption<Integer> KV_MAX_WRITE_BUFFER_NUMBER =
            key("kv.rocksdb.writebuffer.count")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "The maximum number of write buffers that are built up in memory. "
                                    + "The default value is '2'.");

    public static final ConfigOption<Integer> KV_MIN_WRITE_BUFFER_NUMBER_TO_MERGE =
            key("kv.rocksdb.writebuffer.number-to-merge")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The minimum number of write buffers that will be merged together before writing to storage. "
                                    + "The default value is '1'.");

    public static final ConfigOption<MemorySize> KV_BLOCK_SIZE =
            key("kv.rocksdb.block.blocksize")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4kb"))
                    .withDescription(
                            "The approximate size (in bytes) of user data packed per block. "
                                    + "The default blocksize is '4KB'.");

    public static final ConfigOption<MemorySize> KV_METADATA_BLOCK_SIZE =
            key("kv.rocksdb.block.metadata-blocksize")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4kb"))
                    .withDescription(
                            "Approximate size of partitioned metadata packed per block. "
                                    + "Currently applied to indexes block when partitioned index/filters option is enabled. "
                                    + "The default blocksize is '4KB'.");

    public static final ConfigOption<MemorySize> KV_BLOCK_CACHE_SIZE =
            key("kv.rocksdb.block.cache-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("8mb"))
                    .withDescription(
                            "The amount of the cache for data blocks in RocksDB. "
                                    + "The default block-cache size is '8MB'.");

    public static final ConfigOption<Boolean> KV_USE_BLOOM_FILTER =
            key("kv.rocksdb.use-bloom-filter")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If true, every newly created SST file will contain a Bloom filter. "
                                    + "It is enabled by default.");

    public static final ConfigOption<Double> KV_BLOOM_FILTER_BITS_PER_KEY =
            key("kv.rocksdb.bloom-filter.bits-per-key")
                    .doubleType()
                    .defaultValue(10.0)
                    .withDescription(
                            "Bits per key that bloom filter will use, this only take effect when bloom filter is used. "
                                    + "The default value is 10.0.");

    public static final ConfigOption<Boolean> KV_BLOOM_FILTER_BLOCK_BASED_MODE =
            key("kv.rocksdb.bloom-filter.block-based-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, RocksDB will use block-based filter instead of full filter, this only take effect when bloom filter is used. "
                                    + "The default value is 'false'.");

    // ------------------------------------------------------------------------
    //  ConfigOptions for Kv recovering
    // ------------------------------------------------------------------------
    public static final ConfigOption<MemorySize> KV_RECOVER_LOG_RECORD_BATCH_MAX_SIZE =
            key("kv.recover.log-record-batch.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("16mb"))
                    .withDescription(
                            "The max fetch size for fetching log to apply to kv during recovering kv.");

    // ------------------------------------------------------------------------
    //  ConfigOptions for metrics
    // ------------------------------------------------------------------------
    public static final ConfigOption<List<String>> METRICS_REPORTERS =
            key("metrics.reporters")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "An optional list of reporter names. "
                                    + "If configured, only reporters whose name matches in the list will be started");

    public static final ConfigOption<String> METRICS_REPORTER_PROMETHEUS_PORT =
            key("metrics.reporter.prometheus.port")
                    .stringType()
                    .defaultValue("9249")
                    .withDescription(
                            "The port the Prometheus reporter listens on, defaults to 9249. "
                                    + "In order to be able to run several instances of the reporter "
                                    + "on one host (e.g. when one TabletServer is colocated with "
                                    + "the CoordinatorServer) it is advisable to use a port range "
                                    + "like 9250-9260.");

    // ------------------------------------------------------------------------
    //  ConfigOptions for jmx reporter
    // ------------------------------------------------------------------------
    public static final ConfigOption<String> METRICS_REPORTER_JMX_HOST =
            key("metrics.reporter.jmx.port")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The port for "
                                    + "the JMXServer that JMX clients can connect to. If not set, the JMXServer won't start. "
                                    + "In order to be able to run several instances of the reporter "
                                    + "on one host (e.g. when one TabletServer is colocated with "
                                    + "the CoordinatorServer) it is advisable to use a port range "
                                    + "like 9990-9999.");

    // ------------------------------------------------------------------------
    //  ConfigOptions for lakehouse storage
    // ------------------------------------------------------------------------
    public static final ConfigOption<DataLakeFormat> DATALAKE_FORMAT =
            key("datalake.format")
                    .enumType(DataLakeFormat.class)
                    .noDefaultValue()
                    .withDescription(
                            "The datalake format used by Fluss to be as lake storage, such as Paimon, Iceberg, Hudi. "
                                    + "Now, only support Paimon.");

    /**
     * Compaction style for Fluss's kv, which is same to rocksdb's, but help use avoid including
     * rocksdb dependency when only need include this common module.
     */
    public enum CompactionStyle {
        LEVEL,
        UNIVERSAL,
        FIFO,
        NONE,
    }

    /**
     * Compaction style for Fluss's kv, which is same to rocksdb's, but help use avoid including
     * rocksdb dependency when only need include this common module.
     */
    public enum InfoLogLevel {
        DEBUG_LEVEL,
        INFO_LEVEL,
        WARN_LEVEL,
        ERROR_LEVEL,
        FATAL_LEVEL,
        HEADER_LEVEL,
        NUM_INFO_LOG_LEVELS,
    }

    /** Append only row bucket assigner for Fluss writer. */
    public enum NoKeyAssigner {
        ROUND_ROBIN,
        STICKY
    }

    /** Compression type for Fluss's kv. Currently only exposes the following compression type. */
    public enum KvCompressionType {
        NO,
        SNAPPY,
        LZ4,
        ZSTD
    }
}
