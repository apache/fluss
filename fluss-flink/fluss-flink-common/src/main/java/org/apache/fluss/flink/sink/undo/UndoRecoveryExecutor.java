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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.LogScannerImpl;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.ByteArrayWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Executes undo recovery for multiple buckets using streaming execution.
 *
 * <p>This executor manages:
 *
 * <ul>
 *   <li>Single LogScanner with multiple bucket subscriptions
 *   <li>Changelog reading with immediate undo operation execution
 *   <li>Async write operations via UpsertWriter
 * </ul>
 *
 * <p>The execution flow (streaming):
 *
 * <ol>
 *   <li>Subscribe all buckets to the LogScanner
 *   <li>Poll changelog records and execute undo operations immediately
 *   <li>Track a bounded window of write completion futures
 *   <li>Flush and wait for all futures to ensure all writes complete
 * </ol>
 */
public class UndoRecoveryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryExecutor.class);

    private static final int MAX_PENDING_UNDO_WRITES = 1024;

    /** Fixed poll timeout in milliseconds. */
    private static final long POLL_TIMEOUT_MS = 10_000;

    /** Default maximum total wait time in milliseconds before failing (1 hour). */
    private static final long DEFAULT_MAX_TOTAL_WAIT_TIME_MS = 60 * 60 * 1000; // 1 hour

    /** Interval for logging progress during long waits (5 minutes). */
    private static final long PROGRESS_LOG_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

    private final LogScanner scanner;
    private final UpsertWriter writer;
    private final UndoComputer undoComputer;

    // Configurable timeout parameter
    private final long maxTotalWaitTimeMs;

    public UndoRecoveryExecutor(
            LogScanner scanner, UpsertWriter writer, UndoComputer undoComputer) {
        this(scanner, writer, undoComputer, DEFAULT_MAX_TOTAL_WAIT_TIME_MS);
    }

    /**
     * Creates an executor with custom timeout configuration (for testing).
     *
     * @param scanner the log scanner
     * @param writer the upsert writer
     * @param undoComputer the undo computer
     * @param maxTotalWaitTimeMs maximum total wait time before failing
     */
    public UndoRecoveryExecutor(
            LogScanner scanner,
            UpsertWriter writer,
            UndoComputer undoComputer,
            long maxTotalWaitTimeMs) {
        this.scanner = scanner;
        this.writer = writer;
        this.undoComputer = undoComputer;
        this.maxTotalWaitTimeMs = maxTotalWaitTimeMs;
    }

    /**
     * Executes undo recovery for the given bucket contexts.
     *
     * @param contexts the bucket recovery contexts (must have target offsets set)
     * @return safe bucket baselines after all recovery writes have completed
     * @throws Exception if recovery fails
     */
    public Map<TableBucket, Long> execute(List<BucketRecoveryContext> contexts) throws Exception {
        Map<TableBucket, Long> recoveredOffsets = new ConcurrentHashMap<>();
        for (BucketRecoveryContext ctx : contexts) {
            recoveredOffsets.put(ctx.getBucket(), ctx.getLogEndOffset());
        }
        // Filter contexts that need recovery
        List<BucketRecoveryContext> toRecover = filterContextsNeedingRecovery(contexts);
        if (toRecover.isEmpty()) {
            LOG.debug("No buckets need recovery after filtering");
            return recoveredOffsets;
        }

        LOG.debug("Executing undo recovery for {} bucket(s)", toRecover.size());

        // Subscribe and read changelog with streaming execution
        subscribeAll(toRecover);
        List<CompletableFuture<?>> pendingWrites =
                readChangelogAndExecute(toRecover, recoveredOffsets);

        flushAndAwait(pendingWrites);

        // Log summary at INFO level for visibility
        long totalUndoOps = 0;
        for (BucketRecoveryContext ctx : toRecover) {
            totalUndoOps += ctx.getTotalUndoOperations();
        }

        LOG.info(
                "Undo recovery execution completed: {} bucket(s), {} total undo operation(s)",
                toRecover.size(),
                totalUndoOps);
        return new HashMap<>(recoveredOffsets);
    }

    private List<BucketRecoveryContext> filterContextsNeedingRecovery(
            List<BucketRecoveryContext> contexts) {
        List<BucketRecoveryContext> result = new ArrayList<>();
        for (BucketRecoveryContext ctx : contexts) {
            if (ctx.needsRecovery()) {
                LOG.debug(
                        "Bucket {} needs recovery: checkpoint={}, logEndOffset={}",
                        ctx.getBucket(),
                        ctx.getCheckpointOffset(),
                        ctx.getLogEndOffset());
                result.add(ctx);
            } else {
                LOG.debug("Bucket {} already up-to-date, no recovery needed", ctx.getBucket());
            }
        }
        return result;
    }

    private void subscribeAll(List<BucketRecoveryContext> contexts) {
        for (BucketRecoveryContext ctx : contexts) {
            TableBucket bucket = ctx.getBucket();
            if (bucket.getPartitionId() != null) {
                scanner.subscribe(
                        bucket.getPartitionId(), bucket.getBucket(), ctx.getCheckpointOffset());
            } else {
                scanner.subscribe(bucket.getBucket(), ctx.getCheckpointOffset());
            }
        }
    }

    private List<CompletableFuture<?>> readChangelogAndExecute(
            List<BucketRecoveryContext> contexts, Map<TableBucket, Long> recoveredOffsets)
            throws Exception {
        List<CompletableFuture<?>> pendingWrites = new ArrayList<>();

        // Early exit if all contexts are already complete (no records to read)
        if (allComplete(contexts)) {
            LOG.debug("All buckets already complete, no changelog reading needed");
            return pendingWrites;
        }

        long startTimeMs = System.currentTimeMillis();
        long lastProgressLogTime = System.currentTimeMillis();
        Set<TableBucket> unsubscribedBuckets = new HashSet<>();
        Map<TableBucket, BucketRecoveryContext> contextsByBucket = new HashMap<>();
        for (BucketRecoveryContext context : contexts) {
            contextsByBucket.put(context.getBucket(), context);
        }

        while (!allComplete(contexts)) {
            ScanRecords records;
            Map<TableBucket, Deque<SelectedKey>> encodedKeys =
                    scanner instanceof LogScannerImpl ? new HashMap<>() : null;
            if (encodedKeys != null) {
                // Reserve keys only within this poll. Persistent deduplication is updated after
                // the selected rows have been copied and their undo writes have been submitted.
                Map<TableBucket, Set<ByteArrayWrapper>> selectedKeys = new HashMap<>();
                records =
                        ((LogScannerImpl) scanner)
                                .pollFiltered(
                                        Duration.ofMillis(POLL_TIMEOUT_MS),
                                        (bucket, record) -> {
                                            BucketRecoveryContext context =
                                                    contextsByBucket.get(bucket);
                                            if (context == null
                                                    || context.isComplete()
                                                    || record.logOffset()
                                                            >= context.getLogEndOffset()) {
                                                return false;
                                            }
                                            ByteArrayWrapper key =
                                                    undoComputer.selectKey(
                                                            record,
                                                            context.getProcessedKeys(),
                                                            selectedKeys.computeIfAbsent(
                                                                    bucket,
                                                                    ignored -> new HashSet<>()));
                                            if (key == null) {
                                                return false;
                                            }
                                            encodedKeys
                                                    .computeIfAbsent(
                                                            bucket, ignored -> new ArrayDeque<>())
                                                    .addLast(
                                                            new SelectedKey(
                                                                    record.logOffset(), key));
                                            return true;
                                        });
            } else {
                records = scanner.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
            }

            if (!records.hasProgress()) {
                // Check if we've exceeded the maximum total wait time
                long elapsedMs = System.currentTimeMillis() - startTimeMs;
                if (elapsedMs >= maxTotalWaitTimeMs) {
                    throw new RuntimeException(
                            String.format(
                                    "Undo recovery timed out: unable to read all changelog records after "
                                            + "%.1f minutes of waiting. %d bucket(s) still incomplete. "
                                            + "The job will restart and retry the recovery.",
                                    elapsedMs / 60000.0, countIncomplete(contexts)));
                }

                LOG.debug("Empty poll (total elapsed: {}ms)", elapsedMs);
            }

            // Process records before applying the post-poll progress. A poll may consume the
            // complete recovery range while returning its final records.
            for (BucketRecoveryContext ctx : contexts) {
                if (ctx.isComplete()) {
                    continue;
                }
                List<ScanRecord> bucketRecords = records.records(ctx.getBucket());
                if (!bucketRecords.isEmpty()) {
                    processRecords(
                            ctx,
                            bucketRecords,
                            encodedKeys == null
                                    ? null
                                    : checkNotNull(
                                            encodedKeys.get(ctx.getBucket()),
                                            "Missing selected keys"),
                            pendingWrites,
                            recoveredOffsets);
                }
                Long consumedUpToOffset = records.consumedUpToOffset(ctx.getBucket());
                if (consumedUpToOffset != null) {
                    ctx.updateScanPosition(consumedUpToOffset);
                }
                if (ctx.isComplete() && unsubscribedBuckets.add(ctx.getBucket())) {
                    unsubscribeBucket(ctx.getBucket());
                    ctx.releaseProcessedKeys();
                }
            }

            // Log progress periodically (at the end of each loop iteration)
            long now = System.currentTimeMillis();
            if (now - lastProgressLogTime >= PROGRESS_LOG_INTERVAL_MS) {
                long elapsedMs = now - startTimeMs;
                LOG.info(
                        "Undo recovery waiting for changelog records: {} bucket(s) incomplete, "
                                + "waited {} minutes so far (max: {} minutes)",
                        countIncomplete(contexts),
                        String.format("%.1f", elapsedMs / 60000.0),
                        String.format("%.1f", maxTotalWaitTimeMs / 60000.0));
                lastProgressLogTime = now;
            }
        }

        // Log summary
        if (LOG.isDebugEnabled()) {
            for (BucketRecoveryContext ctx : contexts) {
                LOG.debug(
                        "Bucket {} processed {} returned records, executed {} undo operations",
                        ctx.getBucket(),
                        ctx.getTotalRecordsProcessed(),
                        ctx.getTotalUndoOperations());
            }
        }

        return pendingWrites;
    }

    private void unsubscribeBucket(TableBucket bucket) {
        if (bucket.getPartitionId() != null) {
            scanner.unsubscribe(bucket.getPartitionId(), bucket.getBucket());
        } else {
            scanner.unsubscribe(bucket.getBucket());
        }
    }

    private void processRecords(
            BucketRecoveryContext ctx,
            List<ScanRecord> records,
            @Nullable Deque<SelectedKey> selectedKeys,
            List<CompletableFuture<?>> futures,
            Map<TableBucket, Long> recoveredOffsets)
            throws Exception {
        TableBucket bucket = ctx.getBucket();
        Set<ByteArrayWrapper> processedKeys = ctx.getProcessedKeys();
        for (ScanRecord record : records) {
            if (record.logOffset() >= ctx.getLogEndOffset()) {
                continue;
            }
            CompletableFuture<Long> future;
            if (selectedKeys == null) {
                future = undoComputer.processRecord(record, processedKeys);
            } else {
                SelectedKey selected = selectedKeys.pollFirst();
                checkState(
                        selected != null && selected.offset == record.logOffset(),
                        "Selected key does not match the materialized record for bucket %s at offset %s",
                        bucket,
                        record.logOffset());
                future = undoComputer.processRecord(record, selected.key, processedKeys);
            }
            if (future != null) {
                ctx.recordUndoOperation();
                futures.add(
                        future.thenAccept(
                                offset -> recoveredOffsets.merge(bucket, offset, Math::max)));
                if (futures.size() == MAX_PENDING_UNDO_WRITES) {
                    flushAndAwait(futures);
                }
            }
            ctx.recordProcessed();
        }
    }

    private void flushAndAwait(List<CompletableFuture<?>> futures) throws Exception {
        // ACKs can depend on records still buffered in the writer.
        writer.flush();
        if (!futures.isEmpty()) {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            futures.clear();
        }
    }

    private static class SelectedKey {
        private final long offset;
        private final ByteArrayWrapper key;

        private SelectedKey(long offset, ByteArrayWrapper key) {
            this.offset = offset;
            this.key = key;
        }
    }

    private boolean allComplete(List<BucketRecoveryContext> contexts) {
        for (BucketRecoveryContext ctx : contexts) {
            if (!ctx.isComplete()) {
                return false;
            }
        }
        return true;
    }

    private int countIncomplete(List<BucketRecoveryContext> contexts) {
        int count = 0;
        for (BucketRecoveryContext ctx : contexts) {
            if (!ctx.isComplete()) {
                count++;
            }
        }
        return count;
    }
}
