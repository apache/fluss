/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.WriterMetricGroup;
import org.apache.fluss.client.write.RecordAccumulator.RecordAppendResult;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.CopyOnWriteMap;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.config.ConfigOptions.NoKeyAssigner.ROUND_ROBIN;
import static org.apache.fluss.config.ConfigOptions.NoKeyAssigner.STICKY;
import static org.apache.fluss.utils.ExceptionUtils.toException;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.apache.fluss.utils.PartitionUtils.generateAutoPartitionTime;

/**
 * A client that write records to server.
 *
 * <p>The writer consists of a pool of buffer space that holds records that haven't yet been
 * transmitted to the tablet server as well as a background I/O thread that is responsible for
 * turning these records into requests and transmitting them to the cluster. Failure to close the
 * {@link WriterClient} after use will leak these resources.
 *
 * <p>The send method is asynchronous. When called, it adds the log record to a buffer of pending
 * record sends and immediately returns. This allows the wrote record to batch together individual
 * records for efficiency.
 */
@ThreadSafe
@Internal
public class WriterClient {
    private static final Logger LOG = LoggerFactory.getLogger(WriterClient.class);

    public static final String SENDER_THREAD_PREFIX = "fluss-write-sender";
    /**
     * {@link ConfigOptions#CLIENT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET} should be less than or
     * equal to this value when idempotence producer enabled to ensure message ordering.
     */
    private static final int MAX_IN_FLIGHT_REQUESTS_PER_BUCKET_FOR_IDEMPOTENCE = 5;

    /**
     * The bounded time to wait for the sender thread to exit after it has been force closed. Once
     * force closed, the sender abandons all pending requests and exits promptly, so this is only a
     * safety net to avoid blocking close forever.
     */
    private static final long FORCE_CLOSE_TERMINATION_TIMEOUT_MS = 5000;

    private final Configuration conf;
    private final int maxRequestSize;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final ExecutorService ioThreadPool;
    private final MetadataUpdater metadataUpdater;
    // BucketAssigner cache keyed by TablePartition (partitioned tables) or tableId (non-
    // partitioned tables).
    private final Map<TablePartition, BucketAssigner> partitionBucketAssigners =
            new CopyOnWriteMap<>();
    private final Map<Long, BucketAssigner> tableBucketAssigners = new CopyOnWriteMap<>();
    private final IdempotenceManager idempotenceManager;
    private final WriterMetricGroup writerMetricGroup;
    private final DynamicPartitionCreator dynamicPartitionCreator;

    public WriterClient(
            Configuration conf,
            MetadataUpdater metadataUpdater,
            ClientMetricGroup clientMetricGroup,
            Admin admin) {
        this(conf, metadataUpdater, new WriterMetricGroup(clientMetricGroup), admin);
    }

    public WriterClient(
            Configuration conf,
            MetadataUpdater metadataUpdater,
            WriterMetricGroup writerMetricGroup,
            Admin admin) {
        int maxRequestSizeLocal = -1;
        IdempotenceManager idempotenceManagerLocal = null;
        try {
            this.conf = conf;
            this.metadataUpdater = metadataUpdater;
            maxRequestSizeLocal =
                    (int) conf.get(ConfigOptions.CLIENT_WRITER_REQUEST_MAX_SIZE).getBytes();
            this.maxRequestSize = maxRequestSizeLocal;
            this.writerMetricGroup = writerMetricGroup;
            idempotenceManagerLocal = buildIdempotenceManager();
            this.idempotenceManager = idempotenceManagerLocal;

            short acks = configureAcks(idempotenceManager.idempotenceEnabled());
            int retries = configureRetries(idempotenceManager.idempotenceEnabled());
            this.accumulator =
                    new RecordAccumulator(
                            conf, idempotenceManager, writerMetricGroup, SystemClock.getInstance());
            this.sender = newSender(acks, retries);
            this.ioThreadPool = createThreadPool();
            ioThreadPool.submit(sender);

            this.dynamicPartitionCreator =
                    new DynamicPartitionCreator(
                            metadataUpdater,
                            admin,
                            conf.get(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED),
                            conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT),
                            this::maybeAbortBatches);
        } catch (Throwable t) {
            LOG.error("Failed to construct writer.", t);
            close(Duration.ofMillis(0));
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to construct writer. Max request size: %d bytes, Idempotence enabled: %b",
                            maxRequestSizeLocal,
                            idempotenceManagerLocal != null
                                    && idempotenceManagerLocal.idempotenceEnabled()),
                    t);
        }
    }

    /**
     * Asynchronously send a record to a table and invoke the provided callback when to send has
     * been acknowledged.
     */
    public void send(WriteRecord record, WriteCallback callback) {
        doSend(record, callback);
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>
     * linger.ms</code> is greater than 0) and blocks on the completion of the requests associated
     * with these records. The post-condition of <code>flush()</code> is that any previously sent
     * record will have completed (e.g. <code>Future.isDone() == true</code>). A request is
     * considered completed when it is successfully acknowledged according to the <code>acks</code>
     * configuration you have specified or else it results in an error.
     *
     * <p>Other threads can continue sending records while one thread is blocked waiting for a flush
     * call to complete, however no guarantee is made about the completion of records sent after the
     * flush call begins.
     */
    public void flush() {
        LOG.trace("Flushing accumulated records in writer.");
        long start = System.currentTimeMillis();
        accumulator.beginFlush();
        sender.wakeup();
        try {
            accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Flush interrupted after %d ms. Writer may be in inconsistent state",
                            System.currentTimeMillis() - start),
                    e);
        }
        LOG.trace(
                "Flushed accumulated records in writer in {} ms.",
                System.currentTimeMillis() - start);
    }

    private void doSend(WriteRecord record, WriteCallback callback) {
        try {
            throwIfWriterClosed();

            TableInfo tableInfo = record.getTableInfo();
            PhysicalTablePath physicalTablePath = record.getPhysicalTablePath();
            Cluster cluster;
            // The path the record is physically written to. A retired partition's records land in
            // the historical partition, whose own bucket count must drive the assignment.
            PhysicalTablePath routingPath = physicalTablePath;
            if (tableInfo.isPartitioned()) {
                boolean historicalPartitionEnabled =
                        accumulator.checkAndCacheHistoricalPartitionEnabled(tableInfo);
                if (historicalPartitionEnabled
                        && mayBeExpiredHistoricalPartition(
                                physicalTablePath, tableInfo, Instant.now())) {
                    routingPath = resolveHistoricalWriteTarget(physicalTablePath);
                    cluster = metadataUpdater.getCluster();
                } else {
                    cluster =
                            dynamicPartitionCreator.checkAndCreatePartition(
                                    physicalTablePath, tableInfo);
                }
            } else {
                cluster = metadataUpdater.getCluster();
            }

            // maybe create bucket assigner.
            long tableId = tableInfo.getTableId();
            BucketAssigner bucketAssigner;
            int bucketCount;
            if (tableInfo.isPartitioned()) {
                PhysicalTablePath assignerPath = routingPath;
                TablePartition tablePartition =
                        cluster.getTablePartition(routingPath)
                                .orElseThrow(
                                        () ->
                                                new FlussRuntimeException(
                                                        "Partition metadata not available for "
                                                                + assignerPath));
                bucketCount =
                        cluster.getBucketCount(tablePartition)
                                .orElseGet(
                                        () -> {
                                            // bucketCountEpoch == 0 proves the table was never
                                            // rescaled, so the table-level bucket count IS this
                                            // partition's actual count — a safe, provable fallback
                                            // (e.g. an old server that never sends the
                                            // per-partition
                                            // count). Only epoch > 0 with a missing count is a real
                                            // inconsistency worth failing on.
                                            if (tableInfo.getBucketCountEpoch() > 0) {
                                                throw new StaleMetadataException(
                                                        "Per-partition bucket count is unavailable for "
                                                                + assignerPath
                                                                + " at bucketCountEpoch "
                                                                + tableInfo.getBucketCountEpoch()
                                                                + "; refusing to fall back to the"
                                                                + " table-level count.");
                                            }
                                            return tableInfo.getNumBuckets();
                                        });
                bucketAssigner =
                        partitionBucketAssigners.computeIfAbsent(
                                tablePartition,
                                k ->
                                        createBucketAssigner(
                                                tableInfo, assignerPath, bucketCount, conf));
            } else {
                bucketCount =
                        cluster.getBucketCountForTable(tableId).orElse(tableInfo.getNumBuckets());
                bucketAssigner =
                        tableBucketAssigners.computeIfAbsent(
                                tableId,
                                k ->
                                        createBucketAssigner(
                                                tableInfo, physicalTablePath, bucketCount, conf));
            }

            // Append the record to the accumulator.
            int bucketId = bucketAssigner.assignBucket(record.getBucketKey(), cluster);

            RecordAppendResult result =
                    accumulator.append(
                            record,
                            callback,
                            cluster,
                            bucketId,
                            bucketCount,
                            bucketAssigner.abortIfBatchFull());

            if (result.abortRecordForNewBatch) {
                int prevBucketId = bucketId;
                bucketAssigner.onNewBatch(cluster, prevBucketId);
                bucketId = bucketAssigner.assignBucket(record.getBucketKey(), cluster);
                LOG.trace(
                        "Retrying append due to new batch creation for table {} bucket {}, the old bucket was {}.",
                        physicalTablePath,
                        bucketId,
                        prevBucketId);
                result =
                        accumulator.append(record, callback, cluster, bucketId, bucketCount, false);
            }

            if (result.batchIsFull || result.newBatchCreated) {
                LOG.trace(
                        "Waking up the sender since table {} bucket {} is either full or getting a new batch",
                        record.getPhysicalTablePath(),
                        bucketId);
                sender.wakeup();
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to send record to table %s. Writer state: %s",
                            record.getPhysicalTablePath(),
                            sender != null && sender.isRunning() ? "running" : "closed"),
                    e);
        }
    }

    /**
     * Returns whether a partition of a historical-partition-enabled table is old enough that it may
     * have expired under its retention policy.
     *
     * <p>This client-side precheck uses the time zone resolved from the table configuration. A
     * {@code true} result does not confirm that the partition is missing; the caller must refresh
     * metadata before routing the write to a historical partition. If the table does not explicitly
     * configure a time zone and the Client and Coordinator use different defaults, they may
     * classify partitions near the retention boundary differently. Late classification may fail a
     * write to an already removed original partition, while early classification only causes an
     * extra metadata refresh.
     */
    static boolean mayBeExpiredHistoricalPartition(
            PhysicalTablePath physicalTablePath, TableInfo tableInfo, Instant now) {
        // TODO: Move this per-record configuration and time calculation off the hot path by using
        // periodically refreshed, server-authoritative partition status; see
        // https://github.com/apache/fluss/issues/4161.
        String partitionName = physicalTablePath.getPartitionName();
        if (partitionName == null) {
            return false;
        }

        AutoPartitionStrategy strategy = tableInfo.getTableConfig().getAutoPartitionStrategy();
        if (strategy.numToRetain() < 0) {
            return false;
        }

        ZonedDateTime currentDateTime =
                ZonedDateTime.ofInstant(now, strategy.timeZone().toZoneId());
        String earliestRetainedPartition =
                generateAutoPartitionTime(
                        currentDateTime, -strategy.numToRetain(), strategy.timeUnit(), strategy);
        return partitionName.compareTo(earliestRetainedPartition) < 0;
    }

    /** Returns the path the records of this original partition are physically written to. */
    private PhysicalTablePath resolveHistoricalWriteTarget(PhysicalTablePath originalPath) {
        // Keep refreshing while the target is still the original partition so its retirement can
        // be detected before more records are appended to the stale route. Ideally, the Client
        // should learn the server-authoritative partition status without synchronously refreshing
        // metadata on the per-record path; see https://github.com/apache/fluss/issues/4161.
        if (accumulator.hasHistoricalWriteTarget(originalPath)) {
            return PhysicalTablePath.of(originalPath.getTablePath(), HISTORICAL_PARTITION_VALUE);
        }

        PhysicalTablePath targetPath = originalPath;
        // The time check only limits metadata traffic. Invalidate a potentially stale cached route
        // and authoritatively choose the target before the record enters the queue.
        metadataUpdater.invalidPhysicalTableBucketAndPartitionMeta(
                Collections.singleton(originalPath));
        try {
            if (!metadataUpdater.checkAndUpdatePartitionMetadata(originalPath)) {
                throw new FlussRuntimeException(
                        "Failed to resolve write target for " + originalPath + '.');
            }
        } catch (PartitionNotExistException ignored) {
            targetPath =
                    PhysicalTablePath.of(originalPath.getTablePath(), HISTORICAL_PARTITION_VALUE);
            // TODO: Activate this target only after the lake-aware partition retirement protocol
            // guarantees that all accepted original writes are readable from the lake; see
            // https://github.com/apache/fluss/pull/3820.
            if (!metadataUpdater.checkAndUpdatePartitionMetadata(targetPath)) {
                throw new PartitionNotExistException(
                        "Historical partition " + targetPath + " does not exist.");
            }
        }

        accumulator.routeWritesTo(
                originalPath, targetPath, metadataUpdater.getPartitionIdOrElseThrow(targetPath));
        return targetPath;
    }

    private void maybeAbortBatches(Throwable t) {
        if (accumulator.hasIncomplete()) {
            LOG.error("Aborting all pending write batches due to fatal error", t);
            accumulator.abortAllBatches(toException(t));
        }
    }

    // Verify that writer instance has not been closed. This method throws IllegalStateException if
    // writer has already been closed.
    private void throwIfWriterClosed() {
        if (sender == null || !sender.isRunning()) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot perform write operation after writer has been closed. Sender running: %b, Thread pool shutdown: %b",
                            sender != null && sender.isRunning(),
                            ioThreadPool == null || ioThreadPool.isShutdown()));
        }
    }

    private IdempotenceManager buildIdempotenceManager() {
        boolean idempotenceEnabled =
                conf.getBoolean(ConfigOptions.CLIENT_WRITER_ENABLE_IDEMPOTENCE);
        int maxInflightRequestPerBucket =
                conf.getInt(ConfigOptions.CLIENT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET);
        if (idempotenceEnabled
                && maxInflightRequestPerBucket
                        > MAX_IN_FLIGHT_REQUESTS_PER_BUCKET_FOR_IDEMPOTENCE) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for idempotent writer. The value of %s (%d) should be less than or equal to %d when idempotence is enabled to ensure message ordering",
                            ConfigOptions.CLIENT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET.key(),
                            maxInflightRequestPerBucket,
                            MAX_IN_FLIGHT_REQUESTS_PER_BUCKET_FOR_IDEMPOTENCE));
        }

        TabletServerGateway tabletServerGateway = metadataUpdater.newRandomTabletServerClient();
        return idempotenceEnabled
                ? new IdempotenceManager(
                        true, maxInflightRequestPerBucket, tabletServerGateway, metadataUpdater)
                : new IdempotenceManager(
                        false, maxInflightRequestPerBucket, tabletServerGateway, metadataUpdater);
    }

    private short configureAcks(boolean idempotenceEnabled) {
        String acks = conf.get(ConfigOptions.CLIENT_WRITER_ACKS);
        short ack;
        if (acks.equals("all")) {
            ack = Short.parseShort("-1");
        } else {
            ack = Short.parseShort(acks);
        }

        if (idempotenceEnabled && ack != -1) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid acks configuration for idempotent writer. Must set %s to 'all' (current value: '%s') in order to use the idempotent writer. Otherwise we cannot guarantee idempotence",
                            ConfigOptions.CLIENT_WRITER_ACKS.key(), acks));
        }

        return ack;
    }

    private int configureRetries(boolean idempotenceEnabled) {
        int retries = conf.getInt(ConfigOptions.CLIENT_WRITER_RETRIES);
        if (idempotenceEnabled && retries == 0) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid retries configuration for idempotent writer. Must set %s to non-zero (current value: %d) when using the idempotent writer. Otherwise we cannot guarantee idempotence",
                            ConfigOptions.CLIENT_WRITER_RETRIES.key(), retries));
        }
        return retries;
    }

    private Sender newSender(short acks, int retries) {
        return new Sender(
                accumulator,
                (int) conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis(),
                maxRequestSize,
                acks,
                retries,
                metadataUpdater,
                idempotenceManager,
                writerMetricGroup,
                this::invalidateBucketAssigner);
    }

    public void close(Duration timeout) {
        long timeoutMs = timeout.toMillis();
        LOG.info("Closing writer with timeout {} ms.", timeoutMs);

        writerMetricGroup.close();

        if (sender != null) {
            sender.initiateClose();
        }

        if (ioThreadPool != null) {
            ioThreadPool.shutdown();

            if (timeoutMs > 0) {
                try {
                    if (!ioThreadPool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                        LOG.warn("Writer graceful close timed out after {} ms.", timeoutMs);
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while waiting for writer sender thread.", e);
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (sender != null && ioThreadPool != null && !ioThreadPool.isTerminated()) {
            LOG.info(
                    "Proceeding to force close the writer since pending requests could not be completed "
                            + "within timeout {} ms.",
                    timeoutMs);
            sender.forceClose();
            ioThreadPool.shutdownNow();
            try {
                if (!ioThreadPool.awaitTermination(
                        FORCE_CLOSE_TERMINATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    LOG.error("Failed to shutdown writer.");
                }
            } catch (InterruptedException e) {
                LOG.error("Interrupted while force closing writer sender thread.", e);
                Thread.currentThread().interrupt();
            }
        }

        LOG.info("Writer closed.");
    }

    private ExecutorService createThreadPool() {
        return Executors.newFixedThreadPool(1, new ExecutorThreadFactory(SENDER_THREAD_PREFIX));
    }

    /**
     * Removes the {@link BucketAssigner} associated with the given table bucket. Called by {@link
     * Sender} when a write batch receives STALE_METADATA so the next {@code send} creates a new
     * assigner with the refreshed bucket count.
     */
    private void invalidateBucketAssigner(TableBucket tableBucket) {
        Long partitionId = tableBucket.getPartitionId();
        if (partitionId != null) {
            partitionBucketAssigners.remove(
                    new TablePartition(tableBucket.getTableId(), partitionId));
        } else {
            tableBucketAssigners.remove(tableBucket.getTableId());
        }
    }

    private BucketAssigner createBucketAssigner(
            TableInfo tableInfo,
            PhysicalTablePath physicalTablePath,
            int bucketCount,
            Configuration conf) {
        List<String> bucketKeys = tableInfo.getBucketKeys();
        if (!bucketKeys.isEmpty()) {
            BucketingFunction function =
                    BucketingFunction.of(
                            tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
            return new HashBucketAssigner(bucketCount, function);
        } else {
            ConfigOptions.NoKeyAssigner noKeyAssigner =
                    conf.get(ConfigOptions.CLIENT_WRITER_BUCKET_NO_KEY_ASSIGNER);
            if (noKeyAssigner == ROUND_ROBIN) {
                return new RoundRobinBucketAssigner(physicalTablePath, bucketCount);
            } else if (noKeyAssigner == STICKY) {
                return new StickyBucketAssigner(physicalTablePath, bucketCount);
            } else {
                throw new IllegalArgumentException(
                        "Unsupported append only row bucket assigner: " + noKeyAssigner);
            }
        }
    }
}
