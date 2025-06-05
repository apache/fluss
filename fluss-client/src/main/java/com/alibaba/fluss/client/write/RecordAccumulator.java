/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.client.metrics.WriterMetricGroup;
import com.alibaba.fluss.cluster.BucketLocation;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.memory.LazyMemorySegmentPool;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.PreAllocatedPagedOutputView;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.row.arrow.ArrowWriter;
import com.alibaba.fluss.row.arrow.ArrowWriterPool;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import com.alibaba.fluss.utils.CopyOnWriteMap;
import com.alibaba.fluss.utils.MathUtils;
import com.alibaba.fluss.utils.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.fluss.record.LogRecordBatch.NO_WRITER_ID;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

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
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final int pagesPerBatch;

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

    /** The pool of lazily created arrow {@link ArrowWriter}s for arrow log write batch. */
    private final ArrowWriterPool arrowWriterPool;

    private final ConcurrentMap<PhysicalTablePath, BucketAndWriteBatches> writeBatches =
            new CopyOnWriteMap<>();

    private final IncompleteBatches incomplete;

    private final Map<Integer, Integer> nodesDrainIndex;

    private final IdempotenceManager idempotenceManager;
    private final Clock clock;

    // TODO add retryBackoffMs to retry the produce request upon receiving an error.
    // TODO add deliveryTimeoutMs to report success or failure on record delivery.
    // TODO add nextBatchExpiryTimeMs

    RecordAccumulator(
            Configuration conf,
            IdempotenceManager idempotenceManager,
            WriterMetricGroup writerMetricGroup,
            Clock clock) {
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);

        this.batchTimeoutMs =
                Math.min(
                        Integer.MAX_VALUE,
                        (int) conf.get(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT).toMillis());
        this.batchSize =
                Math.max(1, (int) conf.get(ConfigOptions.CLIENT_WRITER_BATCH_SIZE).getBytes());

        this.writerBufferPool = LazyMemorySegmentPool.createWriterBufferPool(conf);
        this.pagesPerBatch = Math.max(1, MathUtils.ceilDiv(batchSize, writerBufferPool.pageSize()));
        this.bufferAllocator = new RootAllocator(Long.MAX_VALUE);
        this.arrowWriterPool = new ArrowWriterPool(bufferAllocator);
        this.incomplete = new IncompleteBatches();
        this.nodesDrainIndex = new HashMap<>();
        this.idempotenceManager = idempotenceManager;
        this.clock = clock;
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

    /**
     * Add a record to the accumulator, return to append result.
     *
     * <p>The append result will contain the future metadata, and flag for whether the appended
     * batch is full or a new batch is created.
     */
    public RecordAppendResult append(
            WriteRecord writeRecord,
            WriteCallback callback,
            Cluster cluster,
            int bucketId,
            boolean abortIfBatchFull)
            throws Exception {
        PhysicalTablePath physicalTablePath = writeRecord.getPhysicalTablePath();

        TableInfo tableInfo = cluster.getTableOrElseThrow(physicalTablePath.getTablePath());
        Optional<Long> partitionIdOpt = cluster.getPartitionId(physicalTablePath);
        BucketAndWriteBatches bucketAndWriteBatches =
                writeBatches.computeIfAbsent(
                        physicalTablePath,
                        k ->
                                new BucketAndWriteBatches(
                                        tableInfo.getTableId(),
                                        partitionIdOpt.orElse(null),
                                        tableInfo.isPartitioned()));

        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        appendsInProgress.incrementAndGet();
        List<MemorySegment> memorySegments = Collections.emptyList();
        try {
            // check if we have an in-progress batch
            Deque<WriteBatch> dq =
                    bucketAndWriteBatches.batches.computeIfAbsent(
                            bucketId, k -> new ArrayDeque<>());
            synchronized (dq) {
                RecordAppendResult appendResult = tryAppend(writeRecord, callback, dq);
                if (appendResult != null) {
                    return appendResult;
                }
            }

            // we don't have an in-progress record batch try to allocate a new batch
            if (abortIfBatchFull) {
                // Return a result that will cause another call to append.
                return new RecordAppendResult(true, false, true);
            }

            memorySegments = allocateMemorySegments(writeRecord);
            synchronized (dq) {
                RecordAppendResult appendResult =
                        appendNewBatch(
                                writeRecord,
                                callback,
                                bucketId,
                                tableInfo,
                                dq,
                                memorySegments,
                                cluster);
                if (appendResult.newBatchCreated) {
                    memorySegments = Collections.emptyList();
                }
                return appendResult;
            }
        } finally {
            // Other append operations by the Sender thread may have created a new batch, causing
            // the temporarily allocated memorySegments here to go unused, and therefore, it needs
            // to be released.
            writerBufferPool.returnAll(memorySegments);
            appendsInProgress.decrementAndGet();
        }
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
        Set<ServerNode> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = batchTimeoutMs;
        Set<PhysicalTablePath> unknownLeaderTables = new HashSet<>();
        // Go table by table so that we can get queue sizes for buckets in a table and calculate
        // cumulative frequency table (used in bucket assigner).

        for (Map.Entry<PhysicalTablePath, BucketAndWriteBatches> writeBatchesEntry :
                writeBatches.entrySet()) {
            nextReadyCheckDelayMs =
                    bucketReady(
                            writeBatchesEntry.getKey(),
                            writeBatchesEntry.getValue(),
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
            Cluster cluster, Set<ServerNode> nodes, int maxSize) throws Exception {
        if (nodes.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Integer, List<ReadyWriteBatch>> batches = new HashMap<>();
        for (ServerNode node : nodes) {
            List<ReadyWriteBatch> ready = drainBatchesForOneNode(cluster, node, maxSize);
            if (!ready.isEmpty()) {
                batches.put(node.id(), ready);
            }
        }
        return batches;
    }

    public void reEnqueue(ReadyWriteBatch readyWriteBatch) {
        WriteBatch batch = readyWriteBatch.writeBatch();
        batch.reEnqueued();
        Deque<WriteBatch> deque =
                getOrCreateDeque(readyWriteBatch.tableBucket(), batch.physicalTablePath());
        synchronized (deque) {
            if (idempotenceManager.idempotenceEnabled()) {
                insertInSequenceOrder(deque, batch, readyWriteBatch.tableBucket());
            } else {
                deque.addFirst(batch);
            }
        }
    }

    /** Abort all incomplete batches (whether they have been sent or not). */
    public void abortBatches(final Exception reason) {
        for (WriteBatch batch : incomplete.copyAll()) {
            Deque<WriteBatch> dq = getDeque(batch.physicalTablePath(), batch.bucketId());
            synchronized (dq) {
                batch.abortRecordAppends();
                dq.remove(batch);
            }
            batch.abort(reason);
            deallocate(batch);
        }
    }

    /** Get the deque for the given table-bucket, creating it if necessary. */
    private Deque<WriteBatch> getOrCreateDeque(
            TableBucket tableBucket, PhysicalTablePath physicalTablePath) {
        BucketAndWriteBatches bucketAndWriteBatches =
                writeBatches.computeIfAbsent(
                        physicalTablePath,
                        k ->
                                new BucketAndWriteBatches(
                                        tableBucket.getTableId(),
                                        tableBucket.getPartitionId(),
                                        physicalTablePath.getPartitionName() != null));
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

    /** Deallocate the record batch. */
    public void deallocate(WriteBatch batch) {
        incomplete.remove(batch);
        writerBufferPool.returnAll(batch.pooledMemorySegments());
    }

    /**
     * Get the ready deque for the given table path and bucket id, or null if it does not exist. A
     * deque is considered ready if it's not a partitioned table or the partition is created and
     * partition_id is fetched.
     */
    @VisibleForTesting
    Deque<WriteBatch> getReadyDeque(PhysicalTablePath path, int bucketId) {
        BucketAndWriteBatches bucketAndWriteBatches = writeBatches.get(path);
        if (bucketAndWriteBatches == null) {
            return null;
        }

        // for the partitioned tables, we need to check whether the partition is ready
        if (bucketAndWriteBatches.isPartitionedTable && bucketAndWriteBatches.partitionId == null) {
            return null;
        }

        return bucketAndWriteBatches.batches.get(bucketId);
    }

    /**
     * Get the deque for the given table path and bucket id, or null if it does not exist.
     *
     * <p>Note: this method does not check whether the partition is ready for partitioned tables.
     */
    private Deque<WriteBatch> getDeque(PhysicalTablePath path, int bucketId) {
        BucketAndWriteBatches bucketAndWriteBatches = writeBatches.get(path);
        if (bucketAndWriteBatches == null) {
            return null;
        }

        return bucketAndWriteBatches.batches.get(bucketId);
    }

    public Set<PhysicalTablePath> getPhysicalTablePathsInBatches() {
        return writeBatches.keySet();
    }

    private List<MemorySegment> allocateMemorySegments(WriteRecord writeRecord) throws IOException {
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
            PhysicalTablePath physicalTablePath,
            BucketAndWriteBatches bucketAndWriteBatches,
            Set<ServerNode> readyNodes,
            Set<PhysicalTablePath> unknownLeaderTables,
            Cluster cluster,
            long nextReadyCheckDelayMs) {
        // first check this table has partitionId.
        if (bucketAndWriteBatches.isPartitionedTable && bucketAndWriteBatches.partitionId == null) {
            Optional<Long> optionIdOpt = cluster.getPartitionId(physicalTablePath);
            if (optionIdOpt.isPresent()) {
                bucketAndWriteBatches.partitionId = optionIdOpt.get();
            } else {
                LOG.debug(
                        "Partition not exists for {}, bucket will not be set to ready",
                        physicalTablePath);
                // TODO: we shouldn't add unready partitions to unknownLeaderTables,
                //  because it cases PartitionNotExistException later
                unknownLeaderTables.add(physicalTablePath);
                return nextReadyCheckDelayMs;
            }
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
            TableBucket tableBucket = cluster.getTableBucket(physicalTablePath, bucketId);
            ServerNode leader = cluster.leaderFor(tableBucket);
            if (leader == null) {
                // This is a bucket for which leader is not known, but messages are
                // available to send. Note that entries are currently not removed from
                // batches when deque is empty.
                unknownLeaderTables.add(physicalTablePath);
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

    private long batchReady(
            boolean exhausted,
            ServerNode leader,
            long waitedTimeMs,
            boolean full,
            Set<ServerNode> readyNodes,
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

    private RecordAppendResult appendNewBatch(
            WriteRecord writeRecord,
            WriteCallback callback,
            int bucketId,
            TableInfo tableInfo,
            Deque<WriteBatch> deque,
            List<MemorySegment> segments,
            Cluster cluster)
            throws Exception {
        RecordAppendResult appendResult = tryAppend(writeRecord, callback, deque);
        if (appendResult != null) {
            // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't
            // happen often...
            return appendResult;
        }

        PhysicalTablePath physicalTablePath = writeRecord.getPhysicalTablePath();
        PreAllocatedPagedOutputView outputView = new PreAllocatedPagedOutputView(segments);
        int schemaId = tableInfo.getSchemaId();
        WriteFormat writeFormat = writeRecord.getWriteFormat();
        // If the table is kv table we need to create a kv batch, otherwise we create a log batch.
        final WriteBatch batch;
        if (writeFormat == WriteFormat.KV) {
            batch =
                    new KvWriteBatch(
                            bucketId,
                            physicalTablePath,
                            schemaId,
                            tableInfo.getTableConfig().getKvFormat(),
                            outputView.getPreAllocatedSize(),
                            outputView,
                            writeRecord.getTargetColumns(),
                            clock.milliseconds());
        } else if (writeFormat == WriteFormat.ARROW_LOG) {
            ArrowWriter arrowWriter =
                    arrowWriterPool.getOrCreateWriter(
                            tableInfo.getTableId(),
                            schemaId,
                            outputView.getPreAllocatedSize(),
                            tableInfo.getRowType(),
                            tableInfo.getTableConfig().getArrowCompressionInfo());
            batch =
                    new ArrowLogWriteBatch(
                            bucketId,
                            physicalTablePath,
                            schemaId,
                            arrowWriter,
                            outputView,
                            clock.milliseconds());
        } else {
            batch =
                    new IndexedLogWriteBatch(
                            bucketId,
                            physicalTablePath,
                            schemaId,
                            outputView.getPreAllocatedSize(),
                            outputView,
                            clock.milliseconds());
        }

        batch.tryAppend(writeRecord, callback);
        deque.addLast(batch);
        incomplete.add(batch);
        return new RecordAppendResult(deque.size() > 1 || batch.isClosed(), true, false);
    }

    private RecordAppendResult tryAppend(
            WriteRecord writeRecord, WriteCallback callback, Deque<WriteBatch> deque)
            throws Exception {
        if (closed) {
            throw new FlussRuntimeException("Writer closed while send in progress");
        }
        WriteBatch last = deque.peekLast();
        if (last != null) {
            boolean success = last.tryAppend(writeRecord, callback);
            if (!success) {
                last.close();
            } else {
                return new RecordAppendResult(deque.size() > 1 || last.isClosed(), false, false);
            }
        }
        return null;
    }

    private List<ReadyWriteBatch> drainBatchesForOneNode(
            Cluster cluster, ServerNode node, int maxSize) throws Exception {
        int size = 0;
        List<BucketLocation> buckets = getAllBucketsInCurrentNode(node, cluster);
        List<ReadyWriteBatch> ready = new ArrayList<>();
        if (buckets.isEmpty()) {
            return ready;
        }
        // to make starvation less likely each node has its own drainIndex.
        int drainIndex = getDrainIndex(node.id());
        int start = drainIndex = drainIndex % buckets.size();
        do {
            BucketLocation bucket = buckets.get(drainIndex);
            PhysicalTablePath physicalTablePath = bucket.getPhysicalTablePath();
            TableBucket tableBucket = bucket.getTableBucket();
            updateDrainIndex(node.id(), drainIndex);
            drainIndex = (drainIndex + 1) % buckets.size();

            Deque<WriteBatch> deque = getReadyDeque(physicalTablePath, tableBucket.getBucket());
            if (deque == null) {
                continue;
            }

            final WriteBatch batch;
            synchronized (deque) {
                WriteBatch first = deque.peekFirst();
                if (first == null) {
                    continue;
                }

                // TODO retry back off check.

                if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                    // there is a rare case that a single batch size is larger than the request size
                    // due to compression; in this case we will still eventually send this batch in
                    // a single request.
                    break;
                } else {
                    if (shouldStopDrainBatchesForBucket(first, tableBucket)) {
                        break;
                    }
                }

                batch = deque.pollFirst();
                long writerId =
                        idempotenceManager.idempotenceEnabled()
                                ? idempotenceManager.writerId()
                                : NO_WRITER_ID;
                if (writerId != NO_WRITER_ID && !batch.hasBatchSequence()) {
                    // If writer id of the bucket do not match the latest one of writer,
                    // we update it and reset the batch sequence. This should be only done when all
                    // its in-flight batches have completed. This is guarantee in
                    // `shouldStopDrainBatchesForBucket`.
                    idempotenceManager.maybeUpdateWriterId(tableBucket);

                    // If the batch already has an assigned batch sequence, then we should not
                    // change writer id and batch sequence, since this may introduce
                    // duplicates. In particular, the previous attempt may actually have been
                    // accepted, and if we change writer id and sequence here, this attempt
                    // will also be accepted, causing a duplicate.
                    //
                    // Additionally, we update the next batch sequence bound for the table bucket,
                    // and also have the writerStateManager track the batch to ensure
                    // that sequence ordering is maintained even if we receive out of order
                    // responses.
                    batch.setWriterState(writerId, idempotenceManager.nextSequence(tableBucket));
                    idempotenceManager.incrementBatchSequence(tableBucket);
                    LOG.debug(
                            "Assigner writerId {} to batch with batch sequence {} being sent to table bucket {}",
                            writerId,
                            batch.batchSequence(),
                            tableBucket);
                    idempotenceManager.addInFlightBatch(batch, tableBucket);
                }
            }

            // the rest of the work by processing outside the lock close() is particularly expensive
            checkNotNull(batch, "batch should not be null");
            batch.close();
            size += batch.estimatedSizeInBytes();
            ready.add(new ReadyWriteBatch(tableBucket, batch));
            // mark the batch as drained.
            batch.drained(System.currentTimeMillis());
        } while (start != drainIndex);
        return ready;
    }

    private boolean shouldStopDrainBatchesForBucket(WriteBatch first, TableBucket tableBucket) {
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
                    firstInFlightSequence == LogRecordBatch.NO_BATCH_SEQUENCE
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
    private List<BucketLocation> getAllBucketsInCurrentNode(
            ServerNode currentNode, Cluster cluster) {
        List<BucketLocation> buckets = new ArrayList<>();
        Set<PhysicalTablePath> physicalTablePaths = cluster.getBucketLocationsByPath().keySet();
        for (PhysicalTablePath path : physicalTablePaths) {
            List<BucketLocation> bucketsForTable =
                    cluster.getAvailableBucketsForPhysicalTablePath(path);
            for (BucketLocation bucket : bucketsForTable) {
                // the bucket leader is always not null in available list,
                // but we still check here to avoid NPE warning.
                if (bucket.getLeader() != null && currentNode.id() == bucket.getLeader().id()) {
                    buckets.add(bucket);
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
        if (batch.batchSequence() == LogRecordBatch.NO_BATCH_SEQUENCE) {
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
            // queue has a sequence greater than the incoming batch. This is the right place to add
            // the incoming batch.
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
        /** Whether this record was abort because the new batch created in record accumulator. */
        public final boolean abortRecordForNewBatch;

        public RecordAppendResult(
                boolean batchIsFull, boolean newBatchCreated, boolean abortRecordForNewBatch) {
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
            this.abortRecordForNewBatch = abortRecordForNewBatch;
        }
    }

    /** The set of nodes that have at leader one complete record batch in the accumulator. */
    public static final class ReadyCheckResult {
        public final Set<ServerNode> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<PhysicalTablePath> unknownLeaderTables;

        public ReadyCheckResult(
                Set<ServerNode> readyNodes,
                long nextReadyCheckDelayMs,
                Set<PhysicalTablePath> unknownLeaderTables) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTables = unknownLeaderTables;
        }
    }

    /** Close this accumulator and force all the record buffers to be drained. */
    public void close() {
        closed = true;

        writerBufferPool.close();
        arrowWriterPool.close();
        bufferAllocator.close();
    }

    /** Per table bucket and write batches. */
    private static class BucketAndWriteBatches {
        public final long tableId;
        public final boolean isPartitionedTable;
        public volatile @Nullable Long partitionId;
        // Write batches for each bucket in queue.
        public final Map<Integer, Deque<WriteBatch>> batches = new CopyOnWriteMap<>();

        public BucketAndWriteBatches(
                long tableId, @Nullable Long partitionId, boolean isPartitionedTable) {
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.isPartitionedTable = isPartitionedTable;
        }
    }
}
