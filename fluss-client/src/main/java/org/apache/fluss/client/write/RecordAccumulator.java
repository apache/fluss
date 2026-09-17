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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metrics.WriterMetricGroup;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.BucketRescaleException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.PreAllocatedPagedOutputView;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.record.LogRecordBatchStatisticsCollector;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ChunkedAllocationManager;
import org.apache.fluss.utils.CopyOnWriteMap;
import org.apache.fluss.utils.MathUtils;
import org.apache.fluss.utils.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocatorUtil.createBufferAllocator;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class act as a queue that accumulates records into {@link WriteBatch} instances to be sent
 * to tablet servers.
 */
@Internal
public final class RecordAccumulator {
    private static final Logger LOG = LoggerFactory.getLogger(RecordAccumulator.class);

    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final int batchSize;
    private final BucketAssignerFactory bucketAssignerFactory;

    /**
     * An artificial delay time to add before declaring a records instance that isn't full ready for
     * sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade
     * off some latency for potentially better throughput due to more batching (and hence fewer,
     * larger requests).
     */
    private final int batchTimeoutMs;

    /**
     * The memory segment pool to allocate/deallocate {@link MemorySegment}s for {@link
     * ArrowLogWriteBatch}.
     */
    private final LazyMemorySegmentPool writerBufferPool;

    /** The arrow buffer allocator to allocate memory for arrow log write batch. */
    private final BufferAllocator bufferAllocator;

    /** The chunked allocation manager factory, stored for explicit native memory release. */
    private final ChunkedAllocationManager.ChunkedFactory chunkedFactory;

    /** Coordinates batch memory deallocation with resource destruction. */
    private final Object resourcesLock = new Object();

    @GuardedBy("resourcesLock")
    private boolean resourcesDestroyed;

    /** The pool of lazily created arrow {@link ArrowWriter}s for arrow log write batch. */
    private final ArrowWriterPool arrowWriterPool;

    private final ConcurrentMap<PhysicalTablePath, BucketAndWriteBatches> writeBatches =
            new CopyOnWriteMap<>();

    /** Whether tables observed by this writer have historical partition support enabled. */
    private final ConcurrentMap<TablePath, Boolean> historicalPartitionEnabledByTable =
            new ConcurrentHashMap<>();

    private final IncompleteBatches incomplete;

    private final Map<Integer, Integer> nodesDrainIndex;

    private final IdempotenceManager idempotenceManager;
    private final Clock clock;
    private final DynamicWriteBatchSizeEstimator batchSizeEstimator;

    // Per-bucket backpressure throttle expiry timestamp. Accessed strictly by key on
    // hot paths (get / put / remove); writes happen on every backpressure signal and
    // every eviction, so the container is sized for lock-striped O(1) updates without
    // any whole-map snapshot cost.
    private final ConcurrentMap<TableBucket, Long> throttleExpiryMs = new ConcurrentHashMap<>();
    private final long maxThrottleMs;

    // Latest Cluster snapshot fed to the metadata-driven throttle sweep. Identity
    // equality against this reference short-circuits the sweep when metadata hasn't
    // changed.
    private volatile Cluster lastClusterRef = Cluster.empty();

    // TODO add retryBackoffMs to retry the produce request upon receiving an error.
    // TODO add deliveryTimeoutMs to report success or failure on record delivery.
    // TODO add nextBatchExpiryTimeMs

    RecordAccumulator(
            Configuration conf,
            IdempotenceManager idempotenceManager,
            WriterMetricGroup writerMetricGroup,
            Clock clock) {
        this(
                conf,
                idempotenceManager,
                writerMetricGroup,
                clock,
                BucketAssignerFactory.defaultFactory(conf));
    }

    @VisibleForTesting
    RecordAccumulator(
            Configuration conf,
            IdempotenceManager idempotenceManager,
            WriterMetricGroup writerMetricGroup,
            Clock clock,
            BucketAssignerFactory bucketAssignerFactory) {
        this.bucketAssignerFactory = checkNotNull(bucketAssignerFactory);
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);

        this.batchTimeoutMs =
                Math.min(
                        Integer.MAX_VALUE,
                        (int) conf.get(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT).toMillis());
        this.batchSize =
                Math.max(1, (int) conf.get(ConfigOptions.CLIENT_WRITER_BATCH_SIZE).getBytes());

        this.writerBufferPool = LazyMemorySegmentPool.createWriterBufferPool(conf);
        this.chunkedFactory = new ChunkedAllocationManager.ChunkedFactory();
        this.bufferAllocator = createBufferAllocator(chunkedFactory);
        this.arrowWriterPool = new ArrowWriterPool(bufferAllocator);
        this.incomplete = new IncompleteBatches();
        this.nodesDrainIndex = new HashMap<>();
        this.batchSizeEstimator =
                new DynamicWriteBatchSizeEstimator(
                        conf.get(ConfigOptions.CLIENT_WRITER_DYNAMIC_BATCH_SIZE_ENABLED),
                        batchSize,
                        (int) conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE).getBytes());
        this.idempotenceManager = idempotenceManager;
        this.clock = clock;
        this.maxThrottleMs =
                conf.get(ConfigOptions.CLIENT_WRITER_KV_BACKPRESSURE_MAX_THROTTLE).toMillis();
        registerMetrics(writerMetricGroup);
    }

    private void registerMetrics(WriterMetricGroup writerMetricGroup) {
        // memory segment pool related metrics.
        writerMetricGroup.gauge(MetricNames.WRITER_BUFFER_TOTAL_BYTES, writerBufferPool::totalSize);
        writerMetricGroup.gauge(
                MetricNames.WRITER_BUFFER_AVAILABLE_BYTES, writerBufferPool::availableMemory);
        // The number of user threads blocked waiting for buffer memory to enqueue their records
        writerMetricGroup.gauge(
                MetricNames.WRITER_BUFFER_WAITING_THREADS, writerBufferPool::queued);
    }

    /** Assigns and appends a record using the layout owned by its write context. */
    public RecordAppendResult append(WriteRecord record, WriteCallback callback, Cluster cluster)
            throws Exception {
        PhysicalTablePath path = record.getPhysicalTablePath();
        TableInfo tableInfo = record.getTableInfo();
        BucketAndWriteBatches context =
                writeBatches.computeIfAbsent(
                        path, k -> createBucketAndWriteBatches(tableInfo, path, cluster));
        checkAndCacheHistoricalPartitionEnabled(tableInfo);
        RecordAppendResult appendResult;
        if (context.bucketCount == null) {
            synchronized (context) {
                appendResult = tryAppend(record, callback, cluster, context);
            }
        } else {
            appendResult = tryAppend(record, callback, cluster, context);
        }
        BucketAssignment newBatchAssignment = appendResult.newBatchAssignment;
        if (newBatchAssignment == null) {
            return appendResult;
        }

        // Resolving routing may need to free batches to satisfy this allocation.
        List<MemorySegment> memorySegments = allocateMemorySegments(record, path);
        try {
            RecordAppendResult result =
                    appendWithAllocatedMemory(
                            record,
                            callback,
                            cluster,
                            context,
                            newBatchAssignment.bucketId,
                            newBatchAssignment.bucketCount,
                            memorySegments);
            if (result.newBatchCreated) {
                memorySegments = Collections.emptyList();
            }
            return result;
        } finally {
            writerBufferPool.returnAll(memorySegments);
        }
    }

    private BucketAndWriteBatches createBucketAndWriteBatches(
            TableInfo tableInfo, PhysicalTablePath path, Cluster cluster) {
        int tempBucketCount =
                cluster.getBucketCount(TableOrPartition.ofTable(tableInfo.getTableId()))
                        .orElse(tableInfo.getNumBuckets());
        Long partitionId = null;
        Integer bucketCount = tempBucketCount;
        if (tableInfo.isPartitioned()) {
            // The metadata may return null for the partition id, and bucketCount,
            // but it is fine to pass null here, because we will fill the partitionId and
            // bucketCount in bucketReady() before send the batch.
            partitionId = cluster.getPartitionId(path).orElse(null);
            bucketCount =
                    partitionId == null
                            ? null
                            : cluster.getBucketCount(TableOrPartition.ofPartition(partitionId))
                                    .orElse(null);
        }
        return new BucketAndWriteBatches(
                partitionId,
                bucketCount,
                tempBucketCount,
                bucketAssignerFactory.createBucketAssigner(tableInfo, path),
                tableInfo.isPartitioned(),
                path);
    }

    /**
     * Get a list of nodes whose buckets are ready to be sent.
     *
     * <p>Also return the flag for whether there are any unknown leaders for the accumulated bucket
     * batches.
     *
     * <p>A destination node is ready to send data if:
     *
     * <pre>
     *     1.There is at least one bucket that is not backing off its send.
     *     2.The record set is full
     *     3.The record set has sat in the accumulator for at least lingerMs milliseconds
     *     4.The accumulator is out of memory and threads are blocking waiting for data (in
     *     this case all buckets are immediately considered ready).
     *     5.The accumulator has been closed
     * </pre>
     */
    public ReadyCheckResult ready(Cluster cluster) {
        Set<Integer> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = batchTimeoutMs;
        Set<PhysicalTablePath> unknownLeaderTables = new HashSet<>();
        // Go table by table so that we can get queue sizes for buckets in a table and calculate
        // cumulative frequency table (used in bucket assigner).

        for (BucketAndWriteBatches bucketAndWriteBatches : writeBatches.values()) {
            nextReadyCheckDelayMs =
                    bucketReady(
                            bucketAndWriteBatches,
                            readyNodes,
                            unknownLeaderTables,
                            cluster,
                            nextReadyCheckDelayMs);
        }

        // TODO and the earliest time at which any non-send-able bucket will be ready;

        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTables);
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit
     * within the specified size on a per-node basis. This method attempts to avoid choosing the
     * same table-node over and over.
     *
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @return A list of {@link ReadyWriteBatch} for each node specified with total size less than
     *     the requested maxSize.
     */
    public Map<Integer, List<ReadyWriteBatch>> drain(
            Cluster cluster, Set<Integer> nodes, int maxSize) throws Exception {
        if (nodes.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Integer, List<ReadyWriteBatch>> batches = new HashMap<>();
        for (Integer node : nodes) {
            List<ReadyWriteBatch> ready = drainBatchesForOneNode(cluster, node, maxSize);
            if (!ready.isEmpty()) {
                batches.put(node, ready);
            }
        }
        return batches;
    }

    /** Re-enqueue a batch unless it was completed concurrently. */
    public boolean reEnqueue(ReadyWriteBatch readyWriteBatch) {
        WriteBatch batch = readyWriteBatch.writeBatch();
        Deque<WriteBatch> deque =
                getDequeOrThrow(readyWriteBatch.tableBucket(), batch.physicalTablePath());
        synchronized (deque) {
            if (batch.isDone()) {
                return false;
            }
            batch.reEnqueued();
            if (idempotenceManager.idempotenceEnabled()) {
                insertInSequenceOrder(deque, batch, readyWriteBatch.tableBucket());
            } else {
                deque.addFirst(batch);
            }
            return true;
        }
    }

    /**
     * Routes writes for an original partition path to the given physical target.
     *
     * <p>The accumulator keeps queues keyed by {@code originalPath}, while metadata lookup, leader
     * discovery, and RPC sending use {@code targetPath}. The target may therefore be either the
     * original partition itself or the shared historical partition.
     *
     * <p>A normal target can be replaced by the historical target only while the original path has
     * no incomplete batch. This method never moves queued or inflight batches between physical
     * partitions.
     *
     * <p>Tables with historical partition support cannot be rescaled, so every target uses the
     * table's fixed bucket count.
     *
     * @throws FlussRuntimeException if a different target was fixed previously
     */
    void routeWritesTo(
            TableInfo tableInfo,
            PhysicalTablePath originalPath,
            PhysicalTablePath targetPath,
            long targetPartitionId) {
        int bucketCount = tableInfo.getNumBuckets();
        BucketAndWriteBatches resolvedTarget =
                new BucketAndWriteBatches(
                        targetPartitionId,
                        bucketCount,
                        bucketCount,
                        bucketAssignerFactory.createBucketAssigner(tableInfo, targetPath),
                        true,
                        targetPath);
        // Install the route atomically before append can create the first queue for this path.
        BucketAndWriteBatches existing = writeBatches.putIfAbsent(originalPath, resolvedTarget);
        if (existing == null) {
            return;
        }

        // Pair with appendNewBatch(). For a historical route change, either a normal batch is
        // registered as incomplete first and rejects the switch, or batch creation observes the
        // historical target.
        synchronized (existing) {
            if (existing.targetPath.equals(targetPath)) {
                existing.partitionId = targetPartitionId;
                return;
            }
            if (!existing.isHistoricalWriteTarget() && resolvedTarget.isHistoricalWriteTarget()) {
                if (hasIncompleteBatchFor(originalPath)) {
                    throw new FlussRuntimeException(
                            String.format(
                                    "Cannot route writes for %s to %s while this writer has "
                                            + "incomplete writes to %s.",
                                    originalPath, targetPath, existing.targetPath));
                }
                existing.switchToHistoricalTarget(targetPath, targetPartitionId);
                existing.bucketCount = bucketCount;
                return;
            }

            throw new FlussRuntimeException(
                    String.format(
                            "Cannot route writes for %s to %s because this writer already routed "
                                    + "the partition to %s.",
                            originalPath, targetPath, existing.targetPath));
        }
    }

    private boolean hasIncompleteBatchFor(PhysicalTablePath physicalTablePath) {
        for (WriteBatch batch : incomplete.copyAll()) {
            if (batch.physicalTablePath().equals(physicalTablePath)) {
                return true;
            }
        }
        return false;
    }

    /** Returns whether this original path is already routed to the historical partition. */
    boolean hasHistoricalWriteTarget(PhysicalTablePath originalPath) {
        BucketAndWriteBatches writeTarget = writeBatches.get(originalPath);
        return writeTarget != null && writeTarget.isHistoricalWriteTarget();
    }

    /**
     * Checks whether a table has historical partition support enabled and caches the result.
     *
     * <p>This method is called from the per-record write path. Reading the value from {@link
     * TableInfo#getTableConfig()} for every record would repeatedly enter the synchronized {@code
     * Configuration} lookup path. Caching both enabled and disabled results keeps subsequent checks
     * to a concurrent-map read. The cache is shared here because WriterClient, batch creation, and
     * Sender must use the same table classification.
     */
    boolean checkAndCacheHistoricalPartitionEnabled(TableInfo tableInfo) {
        TablePath tablePath = tableInfo.getTablePath();
        Boolean cached = historicalPartitionEnabledByTable.get(tablePath);
        if (cached != null) {
            return cached;
        }

        boolean historicalPartitionEnabled =
                tableInfo.getTableConfig().isHistoricalPartitionEnabled();
        // A writer observes stable table configuration, so concurrent initializers compute the
        // same value even if another thread wins putIfAbsent.
        historicalPartitionEnabledByTable.putIfAbsent(tablePath, historicalPartitionEnabled);
        return historicalPartitionEnabled;
    }

    /** Returns the cached historical partition setting for a table. */
    boolean isHistoricalPartitionEnabled(TablePath tablePath) {
        return Boolean.TRUE.equals(historicalPartitionEnabledByTable.get(tablePath));
    }

    /** Reroutes queued batches to the historical target, retaining their fixed bucket layout. */
    void rerouteQueuedWritesToHistorical(
            PhysicalTablePath originalPath,
            PhysicalTablePath historicalPath,
            long historicalPartitionId) {
        BucketAndWriteBatches writeTarget =
                checkNotNull(
                        writeBatches.get(originalPath),
                        "Write target for %s must exist.",
                        originalPath);
        long originalPartitionId;
        synchronized (writeTarget) {
            if (writeTarget.isHistoricalWriteTarget()) {
                writeTarget.partitionId = historicalPartitionId;
                return;
            }
            // New appends observe the historical route and are marked as historical. Existing
            // queued batches are converted below before the Sender can drain again.
            originalPartitionId =
                    writeTarget.switchToHistoricalTarget(historicalPath, historicalPartitionId);
        }
        // The caller has confirmed that no request to the original target remains in flight.
        // Convert every queued bucket under its deque lock so append and drain cannot observe a
        // batch between detaching its original idempotence state and marking it as historical.
        for (Map.Entry<Integer, Deque<WriteBatch>> entry : writeTarget.batches.entrySet()) {
            Deque<WriteBatch> deque = entry.getValue();
            synchronized (deque) {
                for (WriteBatch batch : deque) {
                    if (idempotenceManager.idempotenceEnabled() && batch.hasBatchSequence()) {
                        idempotenceManager.removeInFlightBatch(
                                new ReadyWriteBatch(
                                        new TableBucket(
                                                batch.tableId(),
                                                originalPartitionId,
                                                entry.getKey()),
                                        batch));
                        batch.resetWriterState(NO_WRITER_ID, NO_BATCH_SEQUENCE);
                    }
                    batch.rerouteToHistoricalPartition();
                }
            }
        }
    }

    /** Aborts incomplete batches whose current RPC target is {@code targetPath}. */
    void abortBatches(PhysicalTablePath targetPath, Exception reason) {
        for (WriteBatch batch : incomplete.copyAll()) {
            BucketAndWriteBatches writeTarget = writeBatches.get(batch.physicalTablePath());
            if (writeTarget != null && writeTarget.targetPath.equals(targetPath)) {
                abortBatch(reason, batch);
            }
        }
    }

    /** Abort all incomplete batches (whether they have been sent or not). */
    public void abortAllBatches(final Exception reason) {
        for (WriteBatch batch : incomplete.copyAll()) {
            abortBatch(reason, batch);
        }
    }

    private void abortBatch(final Exception reason, WriteBatch batch) {
        Deque<WriteBatch> deque = getDequeOrThrow(batch.physicalTablePath(), batch.bucketId());
        boolean aborted;
        synchronized (deque) {
            aborted = batch.trySetAborted();
            if (aborted) {
                batch.abortRecordAppends();
            }
            deque.remove(batch);
        }

        // A response may have completed the batch after abortAllBatches() took its snapshot. In
        // that case, skip the abort callback but still claim deallocation if it is still pending.
        try {
            if (aborted) {
                batch.completeAbort(reason);
            }
        } finally {
            deallocate(batch);
        }
    }

    /** Get the deque for the given table-bucket, or throw exception if it doesn't exist. */
    private Deque<WriteBatch> getDequeOrThrow(
            TableBucket tableBucket, PhysicalTablePath physicalTablePath) {
        BucketAndWriteBatches bucketAndWriteBatches = writeBatches.get(physicalTablePath);
        if (bucketAndWriteBatches == null) {
            throw new TableNotExistException(
                    String.format(
                            "Table %s does not exist in the accumulator for bucket %d.",
                            physicalTablePath, tableBucket.getBucket()));
        }
        return bucketAndWriteBatches.batches.computeIfAbsent(
                tableBucket.getBucket(), k -> new ArrayDeque<>());
    }

    /** Check whether there are any batches which haven't been drained. */
    public boolean hasUnDrained() {
        for (BucketAndWriteBatches bucketAndWriteBatches : writeBatches.values()) {
            for (Deque<WriteBatch> deque : bucketAndWriteBatches.batches.values()) {
                synchronized (deque) {
                    if (!deque.isEmpty()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /** Check whether there are any pending batches (whether sent or unsent). */
    public boolean hasIncomplete() {
        return !incomplete.isEmpty();
    }

    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately
     * ready.
     */
    public void beginFlush() {
        flushesInProgress.getAndIncrement();
    }

    /** Mark all buckets as ready to send and block until to send is complete. */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            // Obtain a copy of all the incomplete write request result(s) at the time of the
            // flush. We must be careful not to hold a reference to the ProduceBatch(s) so that
            // garbage collection can occur on the contents. The sender will remove write Batch(s)
            // from the original incomplete collection.
            for (WriteBatch.RequestFuture future : incomplete.requestResults()) {
                future.await();
            }
        } finally {
            flushesInProgress.decrementAndGet();
        }
    }

    /**
     * Deallocate the record batch if this call wins ownership of it.
     *
     * <p>Response handling and fatal cleanup may race, so removing the batch from the incomplete
     * set determines which caller returns its memory. The same lock prevents resource destruction
     * from overtaking that return.
     */
    public void deallocate(WriteBatch batch) {
        synchronized (resourcesLock) {
            if (incomplete.removeIfPresent(batch) && !resourcesDestroyed) {
                writerBufferPool.returnAll(batch.pooledMemorySegments());
            }
        }
    }

    /**
     * Get the ready deque for the given table path and bucket id, or null if it does not exist. A
     * deque is ready only after its bucket count and, for a partitioned table, partition ID have
     * been resolved and any incompatible tentative batches have been aborted.
     */
    @VisibleForTesting
    Deque<WriteBatch> getReadyDeque(PhysicalTablePath path, int bucketId) {
        BucketAndWriteBatches bucketAndWriteBatches = writeBatches.get(path);
        if (bucketAndWriteBatches == null) {
            return null;
        }

        // for the partitioned tables, we need to check whether the partition is ready
        if (bucketAndWriteBatches.bucketCount == null
                || (bucketAndWriteBatches.isPartitionedTable
                        && bucketAndWriteBatches.partitionId == null)) {
            return null;
        }

        return bucketAndWriteBatches.batches.get(bucketId);
    }

    /**
     * Get the deque for the given table path and bucket id, or null if it does not exist.
     *
     * <p>Note: this method does not check whether the partition is ready for partitioned tables.
     */
    @VisibleForTesting
    Deque<WriteBatch> getDequeOrThrow(PhysicalTablePath path, int bucketId) {
        BucketAndWriteBatches bucketAndWriteBatches = writeBatches.get(path);
        if (bucketAndWriteBatches == null) {
            return null;
        }

        return bucketAndWriteBatches.batches.get(bucketId);
    }

    public Set<PhysicalTablePath> getPhysicalTablePathsInBatches() {
        return writeBatches.keySet();
    }

    private List<MemorySegment> allocateMemorySegments(
            WriteRecord writeRecord, PhysicalTablePath physicalTablePath) throws IOException {
        int pagesPerBatch =
                Math.max(
                        1,
                        MathUtils.ceilDiv(
                                batchSizeEstimator.getEstimatedBatchSize(physicalTablePath),
                                writerBufferPool.pageSize()));

        if (writeRecord.getWriteFormat() == WriteFormat.ARROW_LOG) {
            // pre-allocate a batch memory size for Arrow, if it is not sufficient during batching,
            // it will allocate memory from heap
            return writerBufferPool.allocatePages(pagesPerBatch);
        } else {
            int estimatedSizeInBytes = writeRecord.getEstimatedSizeInBytes();
            if (estimatedSizeInBytes > batchSize) {
                // for row-orient log/kv batch, the pre-allocated memory shouldn't
                // smaller than the record size
                int pages =
                        MathUtils.ceilDiv(
                                writeRecord.getEstimatedSizeInBytes(), writerBufferPool.pageSize());
                return writerBufferPool.allocatePages(pages);
            } else {
                return writerBufferPool.allocatePages(pagesPerBatch);
            }
        }
    }

    /** Check whether there are bucket ready for input table. */
    private long bucketReady(
            BucketAndWriteBatches bucketAndWriteBatches,
            Set<Integer> readyNodes,
            Set<PhysicalTablePath> unknownLeaderTables,
            Cluster cluster,
            long nextReadyCheckDelayMs) {
        PhysicalTablePath targetPath = bucketAndWriteBatches.targetPath;
        Long partitionId =
                bucketAndWriteBatches.isPartitionedTable
                        ? cluster.getPartitionId(targetPath).orElse(null)
                        : null;
        Long tableId = cluster.getTableId(targetPath.getTablePath()).orElse(null);
        if (tableId == null || (bucketAndWriteBatches.isPartitionedTable && partitionId == null)) {
            unknownLeaderTables.add(targetPath);
            return nextReadyCheckDelayMs;
        }
        if (bucketAndWriteBatches.bucketCount == null) {
            Integer actualBucketCount =
                    cluster.getBucketCount(TableOrPartition.of(tableId, partitionId)).orElse(null);
            if (actualBucketCount == null) {
                unknownLeaderTables.add(targetPath);
                return nextReadyCheckDelayMs;
            }
            // Resolve both values from this snapshot. Do not cache a partition ID while its count
            // is missing: a later snapshot may already refer to a recreated partition.
            applyNewBucketCount(bucketAndWriteBatches, partitionId, actualBucketCount);
        }

        Map<Integer, Deque<WriteBatch>> batches = bucketAndWriteBatches.batches;
        // Collect the queue sizes for available buckets to be used in adaptive bucket allocate.
        boolean exhausted = writerBufferPool.queued() > 0;
        for (Map.Entry<Integer, Deque<WriteBatch>> entry : batches.entrySet()) {
            Deque<WriteBatch> deque = entry.getValue();

            final long waitedTimeMs;
            final int dequeSize;
            final boolean full;

            // Note: this loop is especially hot with large bucket counts.
            // We are careful to only perform the minimum required inside the synchronized
            // block, as this lock is also used to synchronize writer threads
            // attempting to append() to a bucket/batch.
            synchronized (deque) {
                // Deque are often empty in this path, esp with large bucket counts,
                // so we exit early if we can.
                WriteBatch batch = deque.peekFirst();
                if (batch == null) {
                    continue;
                }

                waitedTimeMs = batch.waitedTimeMs(clock.milliseconds());
                dequeSize = deque.size();
                full = dequeSize > 1 || batch.isClosed();
            }

            int bucketId = entry.getKey();
            TableBucket tableBucket = cluster.getTableBucket(tableId, targetPath, bucketId);

            // If this bucket is throttled, don't mark its node as ready.
            // Instead, factor the remaining throttle time into the next check delay.
            Long throttleExpiry = throttleExpiryMs.get(tableBucket);
            if (throttleExpiry != null) {
                long now = clock.milliseconds();
                if (now < throttleExpiry) {
                    nextReadyCheckDelayMs = Math.min(nextReadyCheckDelayMs, throttleExpiry - now);
                    continue;
                }
                // Expired — evict here to reclaim entries for buckets whose deque
                // has gone empty and won't reach the drain-time throttle check.
                throttleExpiryMs.remove(tableBucket);
            }

            Integer leader = cluster.leaderFor(tableBucket);
            if (leader == null) {
                // This is a bucket for which leader is not known, but messages are
                // available to send. Note that entries are currently not removed from
                // batches when deque is empty.
                unknownLeaderTables.add(targetPath);
            } else {
                nextReadyCheckDelayMs =
                        batchReady(
                                exhausted,
                                leader,
                                waitedTimeMs,
                                full,
                                readyNodes,
                                nextReadyCheckDelayMs);
            }
        }

        return nextReadyCheckDelayMs;
    }

    private void applyNewBucketCount(
            BucketAndWriteBatches context, @Nullable Long partitionId, int newBucketCount) {
        List<WriteBatch> aborted = new ArrayList<>();
        synchronized (context) {
            if (context.bucketCount == null) {
                boolean failBatches =
                        context.tempBucketCount != newBucketCount
                                && (isHashAssigner(context.bucketAssigner)
                                        || hasOutOfRangeBatches(context, newBucketCount));
                for (Deque<WriteBatch> deque : context.batches.values()) {
                    synchronized (deque) {
                        if (failBatches) {
                            WriteBatch batch;
                            while ((batch = deque.pollFirst()) != null) {
                                if (batch.trySetAborted()) {
                                    // This also releases an Arrow batch's writer without building
                                    // it.
                                    batch.abortRecordAppends();
                                    aborted.add(batch);
                                }
                            }
                        } else {
                            for (WriteBatch batch : deque) {
                                batch.setBucketCount(newBucketCount);
                            }
                        }
                    }
                }
                context.partitionId = partitionId;
                // Publish last so the Sender can drain only after every queued batch agrees
                // with the resolved layout.
                context.bucketCount = newBucketCount;
            }
        }

        if (!aborted.isEmpty()) {
            BucketRescaleException failure =
                    new BucketRescaleException(
                            String.format(
                                    "Bucket rescale changed the bucket count for %s "
                                            + "from %d to %d. Buffered records using the "
                                            + "temporary count were not sent. Retry the "
                                            + "failed records with the same writer to "
                                            + "use the updated bucket count.",
                                    context.targetPath, context.tempBucketCount, newBucketCount));
            // Callbacks may retry using the resolved count. Invoke them outside the context and
            // deque locks, after detaching every incompatible batch across all bucket queues.
            for (WriteBatch batch : aborted) {
                try {
                    batch.completeAbort(failure);
                } finally {
                    deallocate(batch);
                }
            }
        }
    }

    private boolean isHashAssigner(BucketAssigner assigner) {
        return assigner instanceof HashBucketAssigner;
    }

    private boolean hasOutOfRangeBatches(BucketAndWriteBatches context, int bucketCount) {
        for (Map.Entry<Integer, Deque<WriteBatch>> entry : context.batches.entrySet()) {
            if (entry.getKey() >= bucketCount) {
                synchronized (entry.getValue()) {
                    if (!entry.getValue().isEmpty()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private long batchReady(
            boolean exhausted,
            int leader,
            long waitedTimeMs,
            boolean full,
            Set<Integer> readyNodes,
            long nextReadyCheckDelayMs) {
        if (!readyNodes.contains(leader)) {
            // if the wait time larger than lingerMs, we can send this batch even if it is not full.
            boolean expired = waitedTimeMs >= (long) batchTimeoutMs;
            boolean sendAble = full || expired || exhausted || closed || flushInProgress();
            if (sendAble) {
                readyNodes.add(leader);
            } else {
                long timeLeftMs = Math.max(batchTimeoutMs - waitedTimeMs, 0);
                // Note that this results in a conservative estimate since an un-sendable bucket may
                // have
                // a leader that will later be found to have sendable data. However, this is good
                // enough
                // since we'll just wake up and then sleep again for the remaining time.
                nextReadyCheckDelayMs = Math.min(nextReadyCheckDelayMs, timeLeftMs);
            }
        }
        return nextReadyCheckDelayMs;
    }

    /**
     * Are there any threads currently waiting on a flush?
     *
     * <p>package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    private RecordAppendResult tryAppend(
            WriteRecord record,
            WriteCallback callback,
            Cluster cluster,
            BucketAndWriteBatches context)
            throws Exception {
        int bucketCount = context.routingBucketCount();
        int bucketId =
                context.bucketAssigner.assignBucket(record.getBucketKey(), cluster, bucketCount);
        Deque<WriteBatch> deque =
                context.batches.computeIfAbsent(bucketId, k -> new ArrayDeque<>());
        synchronized (deque) {
            RecordAppendResult result = tryAppend(record, callback, bucketCount, deque);
            if (result != null) {
                return result;
            }
        }
        if (context.bucketAssigner.abortIfBatchFull()) {
            context.bucketAssigner.onNewBatch(cluster, bucketCount, bucketId);
            bucketId =
                    context.bucketAssigner.assignBucket(
                            record.getBucketKey(), cluster, bucketCount);
        }
        return new RecordAppendResult(new BucketAssignment(bucketId, bucketCount));
    }

    private RecordAppendResult appendWithAllocatedMemory(
            WriteRecord record,
            WriteCallback callback,
            Cluster cluster,
            BucketAndWriteBatches context,
            int bucketId,
            int bucketCount,
            List<MemorySegment> segments)
            throws Exception {
        // Tentative routing resolution and historical target switches must wait for registration.
        // Resolved normal buckets append independently using only their deque locks.
        if (context.bucketCount == null
                || isHistoricalPartitionEnabled(record.getPhysicalTablePath().getTablePath())) {
            synchronized (context) {
                return appendNewBatch(
                        record, callback, cluster, context, bucketId, bucketCount, segments);
            }
        }
        return appendNewBatch(record, callback, cluster, context, bucketId, bucketCount, segments);
    }

    private RecordAppendResult appendNewBatch(
            WriteRecord writeRecord,
            WriteCallback callback,
            Cluster cluster,
            BucketAndWriteBatches context,
            int bucketId,
            int bucketCount,
            List<MemorySegment> segments)
            throws Exception {
        // Memory allocation runs without locks, so the count may have been resolved meanwhile.
        int currentBucketCount = context.routingBucketCount();
        if (bucketCount != currentBucketCount) {
            bucketCount = currentBucketCount;
            bucketId =
                    context.bucketAssigner.assignBucket(
                            writeRecord.getBucketKey(), cluster, bucketCount);
        }
        Deque<WriteBatch> deque =
                context.batches.computeIfAbsent(bucketId, k -> new ArrayDeque<>());
        synchronized (deque) {
            RecordAppendResult appendResult = tryAppend(writeRecord, callback, bucketCount, deque);
            if (appendResult != null) {
                // Somebody else found us a batch while we were allocating memory.
                return appendResult;
            }

            PhysicalTablePath physicalTablePath = writeRecord.getPhysicalTablePath();
            TableInfo tableInfo = writeRecord.getTableInfo();
            PreAllocatedPagedOutputView outputView = new PreAllocatedPagedOutputView(segments);
            int schemaId = tableInfo.getSchemaId();
            WriteFormat writeFormat = writeRecord.getWriteFormat();
            boolean isHistoricalPartition = context.isHistoricalWriteTarget();
            final WriteBatch batch =
                    createWriteBatch(
                            writeRecord,
                            bucketId,
                            bucketCount,
                            tableInfo,
                            writeFormat,
                            physicalTablePath,
                            outputView,
                            schemaId,
                            isHistoricalPartition);

            batch.tryAppend(writeRecord, callback);
            deque.addLast(batch);
            incomplete.add(batch);
            return new RecordAppendResult(deque.size() > 1 || batch.isClosed(), true);
        }
    }

    private WriteBatch createWriteBatch(
            WriteRecord writeRecord,
            int bucketId,
            int bucketCount,
            TableInfo tableInfo,
            WriteFormat writeFormat,
            PhysicalTablePath physicalTablePath,
            PreAllocatedPagedOutputView outputView,
            int schemaId,
            boolean isHistoricalPartition) {
        // If the table is kv table we need to create a kv batch, otherwise we create a log batch.
        int writeLimit = outputView.getPreAllocatedSize();
        switch (writeFormat) {
            case COMPACTED_KV:
            case INDEXED_KV:
                return new KvWriteBatch(
                        tableInfo.getTableId(),
                        bucketId,
                        bucketCount,
                        physicalTablePath,
                        tableInfo.getSchemaId(),
                        writeFormat.toKvFormat(),
                        writeLimit,
                        outputView,
                        writeRecord.getTargetColumns(),
                        writeRecord.getMergeMode(),
                        isHistoricalPartition,
                        clock.milliseconds());

            case ARROW_LOG:
                ArrowWriter arrowWriter =
                        arrowWriterPool.getOrCreateWriter(
                                tableInfo.getTableId(),
                                schemaId,
                                writeLimit,
                                tableInfo.getRowType(),
                                tableInfo.getTableConfig().getArrowCompressionInfo());
                LogRecordBatchStatisticsCollector statisticsCollector = null;
                if (tableInfo.isStatisticsEnabled()) {
                    statisticsCollector =
                            new LogRecordBatchStatisticsCollector(
                                    tableInfo.getRowType(), tableInfo.getStatsIndexMapping());
                }
                return new ArrowLogWriteBatch(
                        tableInfo.getTableId(),
                        bucketId,
                        bucketCount,
                        physicalTablePath,
                        tableInfo.getSchemaId(),
                        arrowWriter,
                        outputView,
                        isHistoricalPartition,
                        clock.milliseconds(),
                        statisticsCollector);

            case COMPACTED_LOG:
                return new CompactedLogWriteBatch(
                        tableInfo.getTableId(),
                        bucketId,
                        bucketCount,
                        physicalTablePath,
                        schemaId,
                        writeLimit,
                        outputView,
                        isHistoricalPartition,
                        clock.milliseconds());

            case INDEXED_LOG:
                return new IndexedLogWriteBatch(
                        tableInfo.getTableId(),
                        bucketId,
                        bucketCount,
                        physicalTablePath,
                        tableInfo.getSchemaId(),
                        writeLimit,
                        outputView,
                        isHistoricalPartition,
                        clock.milliseconds());

            default:
                throw new UnsupportedOperationException("Unsupported write format: " + writeFormat);
        }
    }

    private RecordAppendResult tryAppend(
            WriteRecord writeRecord,
            WriteCallback callback,
            int bucketCount,
            Deque<WriteBatch> deque)
            throws Exception {
        if (closed) {
            throw new FlussRuntimeException("Writer closed while send in progress");
        }
        WriteBatch last = deque.peekLast();
        if (last != null) {
            boolean success =
                    last.getBucketCount() == bucketCount && last.tryAppend(writeRecord, callback);
            if (!success) {
                // The last batch is either full/closed or belongs to a different table, write
                // format, schema, or bucket layout. Close it so the incoming record rolls over to
                // a compatible new batch.
                // TODO For ArrowLogWriteBatch, close here is a heavy operation (including build
                // logic), we need to avoid do that in an lock which locked dq. However, why we not
                // remove build logic out of close for ArrowLogWriteBatch is that we want to release
                // non-heap memory hold by arrowWriter as soon as possible to avoid OOM. Maybe we
                // need to introduce a more reasonable way to solve these two problems.
                last.close();
            } else {
                return new RecordAppendResult(deque.size() > 1 || last.isClosed(), false);
            }
        }
        return null;
    }

    private List<ReadyWriteBatch> drainBatchesForOneNode(Cluster cluster, Integer node, int maxSize)
            throws Exception {
        int size = 0;
        List<BucketLocation> buckets = getAllBucketsInCurrentNode(node, cluster);
        List<ReadyWriteBatch> ready = new ArrayList<>();
        if (buckets.isEmpty()) {
            return ready;
        }
        // to make starvation less likely each node has its own drainIndex.
        int drainIndex = getDrainIndex(node);
        int start = drainIndex = drainIndex % buckets.size();
        do {
            BucketLocation bucket = buckets.get(drainIndex);
            PhysicalTablePath physicalTablePath = bucket.getPhysicalTablePath();
            TableBucket tableBucket = bucket.getTableBucket();
            updateDrainIndex(node, drainIndex);
            drainIndex = (drainIndex + 1) % buckets.size();

            Deque<WriteBatch> deque = getReadyDeque(physicalTablePath, tableBucket.getBucket());
            if (deque == null) {
                continue;
            }

            final WriteBatch batch;
            List<WriteBatch> staleBatches = null;
            long oldStaleTableId = -1L;
            synchronized (deque) {
                WriteBatch first = deque.peekFirst();
                if (first == null) {
                    continue;
                }

                if (tableBucket.getTableId() != first.tableId()) {
                    // Table has been dropped and re-created with a new table id. Drain ALL
                    // consecutive head batches that belong to the old table instance in one
                    // pass under the lock, then abort them outside the lock with a single
                    // aggregated WARN line.
                    oldStaleTableId = first.tableId();
                    staleBatches = new ArrayList<>();
                    while (first != null
                            && first.tableId() == oldStaleTableId
                            && first.tableId() != tableBucket.getTableId()) {
                        staleBatches.add(deque.pollFirst());
                        first = deque.peekFirst();
                    }
                    batch = null;
                } else {
                    // TODO retry back off check.

                    if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                        // there is a rare case that a single batch size is larger than the
                        // request size due to compression; in this case we will still
                        // eventually send this batch in a single request.
                        break;
                    } else if (shouldSkipBucket(first, tableBucket)) {
                        // Buckets are independent — skip this one, keep draining others.
                        continue;
                    }

                    batch = deque.pollFirst();
                    long writerId =
                            idempotenceManager.idempotenceEnabled()
                                    ? idempotenceManager.writerId()
                                    : NO_WRITER_ID;
                    if (writerId != NO_WRITER_ID && !batch.hasBatchSequence()) {
                        // If writer id of the bucket do not match the latest one of writer,
                        // we update it and reset the batch sequence. This should be only done when
                        // all
                        // its in-flight batches have completed. This is guarantee in
                        // `shouldSkipBucket`.
                        idempotenceManager.maybeUpdateWriterId(tableBucket);

                        // If the batch already has an assigned batch sequence, then we should not
                        // change writer id and batch sequence, since this may introduce
                        // duplicates. In particular, the previous attempt may actually have been
                        // accepted, and if we change writer id and sequence here, this attempt
                        // will also be accepted, causing a duplicate.
                        //
                        // Additionally, we update the next batch sequence bound for the table
                        // bucket,
                        // and also have the writerStateManager track the batch to ensure
                        // that sequence ordering is maintained even if we receive out of order
                        // responses.
                        batch.setWriterState(
                                writerId, idempotenceManager.nextSequence(tableBucket));
                        idempotenceManager.incrementBatchSequence(tableBucket);
                        LOG.debug(
                                "Assigner writerId {} to batch with batch sequence {} being sent to table bucket {}",
                                writerId,
                                batch.batchSequence(),
                                tableBucket);
                        idempotenceManager.addInFlightBatch(batch, tableBucket);
                    }
                }
            }

            // Abort stale batches *outside* the deque lock so user callbacks (alien methods)
            // and memory deallocation never run while holding it. Aggregate to a single WARN.
            if (staleBatches != null) {
                LOG.warn(
                        "Table {} has been dropped and re-created with a new table ID. "
                                + "Old ID: {}, New ID: {}. Aborting {} pending batches for the old table instance.",
                        physicalTablePath,
                        oldStaleTableId,
                        tableBucket.getTableId(),
                        staleBatches.size());
                TableNotExistException reason =
                        new TableNotExistException(
                                String.format(
                                        "Table '%s' has been dropped and re-created with a new table ID (old: %d, new: %d). "
                                                + "Further writes to the old table instance cannot proceed. "
                                                + "Please recreate the writer with the new table metadata.",
                                        physicalTablePath,
                                        oldStaleTableId,
                                        tableBucket.getTableId()),
                                null,
                                true);
                for (WriteBatch staleBatch : staleBatches) {
                    abortBatch(reason, staleBatch);
                }
                continue;
            }

            // the rest of the work by processing outside the lock close() is particularly expensive
            checkNotNull(batch, "batch should not be null");
            batch.close();
            int currentBatchSize = batch.estimatedSizeInBytes();
            size += currentBatchSize;
            batchSizeEstimator.updateEstimation(physicalTablePath, currentBatchSize);

            ready.add(new ReadyWriteBatch(tableBucket, batch));
            // mark the batch as drained.
            batch.drained(System.currentTimeMillis());
        } while (start != drainIndex);
        return ready;
    }

    private boolean shouldSkipBucket(WriteBatch first, TableBucket tableBucket) {
        // Backpressure throttle check: skip this bucket if still under throttle
        if (isThrottled(tableBucket)) {
            return true;
        }
        if (idempotenceManager.idempotenceEnabled()) {
            if (!idempotenceManager.isWriterIdValid()) {
                // we cannot send the batch until we have refreshed writer id.
                return true;
            }

            // If the queued batch already has an assigned batch sequence, then it is being
            // retried. In this case, we wait until the next immediate batch is ready and
            // drain that. We only move on when the next in line batch is complete (either
            // successfully or due to a fatal server error). This effectively reduces our in
            // flight request count to 1.
            int firstInFlightSequence = idempotenceManager.firstInFlightBatchSequence(tableBucket);
            boolean isFirstInFlightBatch =
                    firstInFlightSequence == NO_BATCH_SEQUENCE
                            || (first.hasBatchSequence()
                                    && first.batchSequence() == firstInFlightSequence);

            if (isFirstInFlightBatch) {
                return false;
            } else {
                if (!first.hasBatchSequence()) {
                    // For batches that haven't been assigned a batchSequence, we consider them as
                    // new batches. In this case, we need to ensure that the number of inflight
                    // requests does not exceed maxInflightRequestsPerBucket.
                    return !idempotenceManager.canSendMoreRequests(tableBucket);
                } else {
                    // For batches that have been assigned a batchSequence, we consider that these
                    // batches have encountered a retriable error. In such cases, only the first
                    // batch  allow to be sent until the retriable error is resolved. This
                    // approach helps reduce the number of requests sent over the network.
                    return true;
                }
            }
        }
        return false;
    }

    // ---- Backpressure throttle methods ----

    /**
     * Check if a bucket is currently under backpressure throttle.
     *
     * <p>Performs lazy eviction: if the throttle has expired, the entry is removed from the map to
     * prevent unbounded growth.
     *
     * @return true if the bucket should be skipped during drain
     */
    boolean isThrottled(TableBucket tableBucket) {
        Long expiry = throttleExpiryMs.get(tableBucket);
        if (expiry == null) {
            return false;
        }
        if (clock.milliseconds() < expiry) {
            return true;
        }
        // Expired — evict to prevent map leak
        throttleExpiryMs.remove(tableBucket);
        return false;
    }

    /**
     * Update the throttle state for a bucket based on the received pressure signal.
     *
     * <p>The delay grows quadratically with pressure: {@code delay = maxThrottleMs * p^2}, where
     * {@code p ∈ [0, 1)}. This provides meaningful throttling across the full ramp-up window while
     * remaining gentle at low pressure.
     *
     * @param tableBucket the bucket to update
     * @param pressure value in {@code [0, 1)} on the wire; {@code 0} means recovered, positive
     *     values trigger a throttle window. {@code 1.0f} is reserved as the internal hard-rejection
     *     value (never sent by the server): the Sender passes it when the server rejected the write
     *     outright, and it installs the full {@link #maxThrottleMs} window directly.
     */
    void updateThrottle(TableBucket tableBucket, float pressure) {
        if (pressure >= 1f) {
            // Hard rejection: stall the bucket for the full max throttle window, bypassing the
            // quadratic curve to avoid long-to-float rounding.
            throttleExpiryMs.put(tableBucket, clock.milliseconds() + maxThrottleMs);
            return;
        }
        if (pressure > 0f) {
            long delay = (long) (maxThrottleMs * pressure * pressure);
            if (delay > 0) {
                throttleExpiryMs.put(tableBucket, clock.milliseconds() + delay);
                return;
            }
        }
        // Recovered or below the meaningful resolution: remove throttle.
        // Note: in production, recovery relies on the last throttle window expiring naturally
        // (server stops sending the pressure field once p reaches 0). This branch exists as
        // defensive completeness and is exercised by unit tests.
        throttleExpiryMs.remove(tableBucket);
    }

    /**
     * Evict throttle entries whose buckets no longer exist in the given cluster (leader unknown,
     * partition dropped, table dropped).
     *
     * <p>Invoked on every Sender loop with the current cluster snapshot. The identity short-circuit
     * makes this an O(1) no-op when metadata hasn't changed, so the actual O(N) walk only runs once
     * per real metadata refresh.
     */
    void maybeEvictStaleThrottles(Cluster cluster) {
        if (cluster == lastClusterRef) {
            return;
        }
        lastClusterRef = cluster;
        if (throttleExpiryMs.isEmpty()) {
            return;
        }
        throttleExpiryMs.keySet().removeIf(tb -> cluster.leaderFor(tb) == null);
    }

    private int getDrainIndex(int id) {
        return nodesDrainIndex.computeIfAbsent(id, s -> 0);
    }

    private void updateDrainIndex(int id, int drainIndex) {
        nodesDrainIndex.put(id, drainIndex);
    }

    /**
     * TODO This is a very time-consuming operation, which will be moved to be computed in the
     * Cluster later on.
     */
    private List<BucketLocation> getAllBucketsInCurrentNode(Integer currentNode, Cluster cluster) {
        List<BucketLocation> buckets = new ArrayList<>();
        Set<PhysicalTablePath> physicalTablePaths = cluster.getBucketLocationsByPath().keySet();
        for (PhysicalTablePath path : physicalTablePaths) {
            BucketAndWriteBatches bucketAndWriteBatches = writeBatches.get(path);
            // A historical route uses the original path only as the accumulator queue key. Its
            // actual bucket locations come from the historical target and are added below.
            if (bucketAndWriteBatches != null && bucketAndWriteBatches.isHistoricalWriteTarget()) {
                continue;
            }
            List<BucketLocation> bucketsForTable =
                    cluster.getAvailableBucketsForPhysicalTablePath(path);
            for (BucketLocation bucket : bucketsForTable) {
                // the bucket leader is always not null in available list,
                // but we still check here to avoid NPE warning.
                if (bucket.getLeader() != null && Objects.equals(currentNode, bucket.getLeader())) {
                    buckets.add(bucket);
                }
            }
        }

        // Historical queues remain keyed by their original partition path. Add a location using
        // that queue key while retaining the historical bucket as the RPC target.
        for (Map.Entry<PhysicalTablePath, BucketAndWriteBatches> entry : writeBatches.entrySet()) {
            BucketAndWriteBatches bucketAndWriteBatches = entry.getValue();
            PhysicalTablePath originalPath = entry.getKey();
            if (!bucketAndWriteBatches.isHistoricalWriteTarget()) {
                continue;
            }
            for (BucketLocation bucketLocation :
                    cluster.getAvailableBucketsForPhysicalTablePath(
                            bucketAndWriteBatches.targetPath)) {
                if (bucketLocation.getLeader() != null
                        && Objects.equals(currentNode, bucketLocation.getLeader())) {
                    // Keep the original path so drain can find its original-keyed queue. The
                    // TableBucket, leader, and replicas still describe the historical RPC target.
                    buckets.add(
                            new BucketLocation(
                                    originalPath,
                                    bucketLocation.getTableBucket(),
                                    bucketLocation.getLeader(),
                                    bucketLocation.getReplicas()));
                }
            }
        }
        return buckets;
    }

    /**
     * The deque for the bucket may have to be reordered in situations where leadership changes in
     * between batch drains. Since the requests are on different connections, we no longer have any
     * guarantees about ordering of the responses. Hence, we will have to check if there is anything
     * out of order and ensure the batch is queued in the correct sequence order.
     *
     * <p>Note that this assumes that all the batches in the queue which have an assigned batch
     * sequence also have the current writer id. We will not attempt to reorder messages if the
     * writer id has changed.
     */
    private void insertInSequenceOrder(
            Deque<WriteBatch> deque, WriteBatch batch, TableBucket tableBucket) {
        // When we are re-enqueue and have enabled idempotence, the re-enqueued batch must always
        // have a batch sequence.
        if (batch.batchSequence() == NO_BATCH_SEQUENCE) {
            throw new IllegalStateException(
                    "Trying to re-enqueue a batch which doesn't have a sequence even "
                            + "though idempotence is enabled.");
        }

        if (idempotenceManager.nextBatchBySequence(tableBucket) == null) {
            throw new IllegalStateException(
                    "We are re-enqueueing a batch which is not tracked as part of the in flight "
                            + "requests.batch.tableBucket: "
                            + tableBucket
                            + "; batch.batchSequence: "
                            + batch.batchSequence());
        }

        // If there are no inflight batches being tracked by the writerStateManager, it means
        // that the writer id must have changed and the batches being re enqueued are from the
        // old writer id. In this case we don't try to ensure ordering amongst them. They will
        // eventually fail with an OutOfOrderSequence, or they will succeed.
        if (batch.batchSequence()
                != idempotenceManager.nextBatchBySequence(tableBucket).batchSequence()) {
            // The incoming batch can't be inserted at the front of the queue without violating the
            // sequence ordering. This means that the incoming batch should be placed somewhere
            // further back.
            // We need to find the right place for the incoming batch and insert it there.
            // We will only enter this branch if we have multiple in-flights sent to different
            // brokers, perhaps because a leadership change occurred in between the drains. In this
            // scenario, responses can come back out of order, requiring us to re-order the batches
            // ourselves rather than relying on the implicit ordering guarantees of the network
            // client which are only on a per-connection basis.

            List<WriteBatch> orderedBatches = new ArrayList<>();
            while (deque.peekFirst() != null
                    && deque.peekFirst().hasBatchSequence()
                    && deque.peekFirst().batchSequence() < batch.batchSequence()) {
                orderedBatches.add(deque.pollFirst());
            }

            LOG.debug(
                    "Reordered incoming batch with sequence {} for bucket {}. It was placed in the queue at "
                            + "position {}",
                    batch.batchSequence(),
                    tableBucket,
                    orderedBatches.size());
            // Either we have reached a point where there are batches without a sequence (i.e. never
            // been drained and are hence in order by default), or the batch at the front of the
            // queue has a sequence greater than the incoming batch. This is the right place to
            // add the incoming batch.
            deque.addFirst(batch);

            // Now we have to re-insert the previously queued batches in the right order.
            for (int i = orderedBatches.size() - 1; i >= 0; --i) {
                deque.addFirst(orderedBatches.get(i));
            }

            // At this point, the incoming batch has been queued in the correct place according to
            // its sequence.
        } else {
            deque.addFirst(batch);
        }
    }

    /** Metadata about a record just appended to the record accumulator. */
    public static final class RecordAppendResult {
        public final boolean batchIsFull;
        public final boolean newBatchCreated;

        @Nullable private final BucketAssignment newBatchAssignment;

        /** Creates the result describing batch fullness and whether a batch was created. */
        public RecordAppendResult(boolean batchIsFull, boolean newBatchCreated) {
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
            this.newBatchAssignment = null;
        }

        private RecordAppendResult(BucketAssignment newBatchAssignment) {
            this.batchIsFull = false;
            this.newBatchCreated = false;
            this.newBatchAssignment = newBatchAssignment;
        }
    }

    /** Routing retained only when an append needs to allocate memory for a new batch. */
    private static final class BucketAssignment {
        private final int bucketId;
        private final int bucketCount;

        private BucketAssignment(int bucketId, int bucketCount) {
            this.bucketId = bucketId;
            this.bucketCount = bucketCount;
        }
    }

    /** The set of nodes that have at leader one complete record batch in the accumulator. */
    public static final class ReadyCheckResult {
        public final Set<Integer> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<PhysicalTablePath> unknownLeaderTables;

        public ReadyCheckResult(
                Set<Integer> readyNodes,
                long nextReadyCheckDelayMs,
                Set<PhysicalTablePath> unknownLeaderTables) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTables = unknownLeaderTables;
        }
    }

    /** Close this accumulator to reject new appends. */
    public void close() {
        closed = true;
        for (BucketAndWriteBatches context : writeBatches.values()) {
            synchronized (context) {
                // Resolved normal appends only hold their deque lock during registration.
                for (Deque<WriteBatch> deque : context.batches.values()) {
                    synchronized (deque) {
                        // Wait for registration before fatal cleanup takes its snapshot.
                    }
                }
            }
        }
    }

    /**
     * Destroy all resources held by this accumulator including the Arrow writer pool and buffer
     * allocator.
     *
     * <p>This must only be called after the sender thread has fully exited and no more drain
     * operations will occur. Otherwise, draining batches may attempt to recycle Arrow writers using
     * an already-closed allocator, causing "Accounted size went negative" errors.
     *
     * <p>This method is idempotent: subsequent calls after the first are no-ops.
     */
    @VisibleForTesting
    public void destroyResources() {
        synchronized (resourcesLock) {
            if (resourcesDestroyed) {
                return;
            }
            resourcesDestroyed = true;
            writerBufferPool.close();
            arrowWriterPool.close();
            bufferAllocator.close();
            chunkedFactory.close();
        }
    }

    /** Per table bucket and write batches. */
    private static class BucketAndWriteBatches {
        public final boolean isPartitionedTable;

        /**
         * The temporary bucket count used for routing before the authoritative bucket count is
         * known.
         */
        private final int tempBucketCount;

        /** The bucket assigner to assign record to bucket id for the given path. */
        private final BucketAssigner bucketAssigner;

        /** The physical partition used for metadata lookup, leader discovery, and write RPCs. */
        private volatile PhysicalTablePath targetPath;

        /** Null until every queued batch agrees with the authoritative layout. */
        private volatile @Nullable Integer bucketCount;

        public volatile @Nullable Long partitionId;
        // Write batches for each bucket in queue.
        public final Map<Integer, Deque<WriteBatch>> batches = new CopyOnWriteMap<>();

        private BucketAndWriteBatches(
                @Nullable Long partitionId,
                @Nullable Integer bucketCount,
                int tempBucketCount,
                BucketAssigner bucketAssigner,
                boolean isPartitionedTable,
                PhysicalTablePath targetPath) {
            this.partitionId = partitionId;
            this.bucketCount = bucketCount;
            this.tempBucketCount = tempBucketCount;
            this.bucketAssigner = bucketAssigner;
            this.isPartitionedTable = isPartitionedTable;
            this.targetPath = targetPath;
        }

        private int routingBucketCount() {
            Integer current = bucketCount;
            return current == null ? tempBucketCount : current;
        }

        public boolean isHistoricalWriteTarget() {
            return HISTORICAL_PARTITION_VALUE.equals(targetPath.getPartitionName());
        }

        /**
         * Atomically switches the target path and partition ID to the historical partition.
         *
         * <p>This method must be called while the current target is still the original partition.
         * The returned original partition ID is used to detach queued batches from their original
         * idempotence state before rerouting them.
         */
        private synchronized long switchToHistoricalTarget(
                PhysicalTablePath historicalPath, long historicalPartitionId) {
            long originalPartitionId =
                    checkNotNull(
                            partitionId,
                            "Original partition ID must be resolved before rerouting.");
            targetPath = historicalPath;
            partitionId = historicalPartitionId;
            return originalPartitionId;
        }
    }
}
