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
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.ByteArrayWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
 *   <li>Collect CompletableFutures for completion tracking
 *   <li>Flush and wait for all futures to ensure all writes complete
 * </ol>
 */
public class UndoRecoveryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryExecutor.class);

    /** Fixed poll timeout in milliseconds. */
    private static final long POLL_TIMEOUT_MS = 10_000;

    /** Default maximum idle time for each bucket before failing (1 hour). */
    private static final long DEFAULT_MAX_IDLE_TIME_MS = 60 * 60 * 1000; // 1 hour

    /** Interval for logging progress during long waits (5 minutes). */
    private static final long PROGRESS_LOG_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

    private final LogScanner scanner;
    private final UpsertWriter writer;
    private final UndoComputer undoComputer;

    private final long maxIdleTimeMs;
    private final long maxIdleTimeNanos;

    public UndoRecoveryExecutor(
            LogScanner scanner, UpsertWriter writer, UndoComputer undoComputer) {
        this(scanner, writer, undoComputer, DEFAULT_MAX_IDLE_TIME_MS);
    }

    /**
     * Creates an executor with custom timeout configuration (for testing).
     *
     * @param scanner the log scanner
     * @param writer the upsert writer
     * @param undoComputer the undo computer
     * @param maxIdleTimeMs maximum time a bucket may make no progress before failing
     */
    public UndoRecoveryExecutor(
            LogScanner scanner,
            UpsertWriter writer,
            UndoComputer undoComputer,
            long maxIdleTimeMs) {
        this.scanner = scanner;
        this.writer = writer;
        this.undoComputer = undoComputer;
        this.maxIdleTimeMs = maxIdleTimeMs;
        this.maxIdleTimeNanos = TimeUnit.MILLISECONDS.toNanos(maxIdleTimeMs);
    }

    /**
     * Executes undo recovery for the given bucket contexts.
     *
     * @param contexts the bucket recovery contexts (must have target offsets set)
     * @throws Exception if recovery fails
     */
    public void execute(List<BucketRecoveryContext> contexts) throws Exception {
        // Filter contexts that need recovery
        List<BucketRecoveryContext> toRecover = filterContextsNeedingRecovery(contexts);
        if (toRecover.isEmpty()) {
            LOG.debug("No buckets need recovery after filtering");
            return;
        }

        LOG.debug("Executing undo recovery for {} bucket(s)", toRecover.size());

        // Subscribe and read changelog with streaming execution
        subscribeAll(toRecover);
        List<CompletableFuture<?>> allFutures = readChangelogAndExecute(toRecover);

        // Flush first, then wait for all async writes to complete
        writer.flush();
        if (!allFutures.isEmpty()) {
            CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).get();
        }

        // Log summary at INFO level for visibility
        int totalUndoOps = 0;
        for (BucketRecoveryContext ctx : toRecover) {
            totalUndoOps += ctx.getProcessedKeys().size();
        }

        LOG.info(
                "Undo recovery execution completed: {} bucket(s), {} total undo operation(s)",
                toRecover.size(),
                totalUndoOps);
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

    private List<CompletableFuture<?>> readChangelogAndExecute(List<BucketRecoveryContext> contexts)
            throws Exception {
        List<CompletableFuture<?>> allFutures = new ArrayList<>();

        // Early exit if all contexts are already complete (no records to read)
        if (allComplete(contexts)) {
            LOG.debug("All buckets already complete, no changelog reading needed");
            return allFutures;
        }

        long startTimeNanos = System.nanoTime();
        long lastProgressLogTimeNanos = startTimeNanos;
        Map<TableBucket, Long> lastProgressNanos = new HashMap<>();
        for (BucketRecoveryContext ctx : contexts) {
            lastProgressNanos.put(ctx.getBucket(), startTimeNanos);
        }

        while (!allComplete(contexts)) {
            ScanRecords records = scanner.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
            long progressTimeNanos = System.nanoTime();

            // Process records before applying the post-poll position. The poll may reach the
            // target while returning the final records below it.
            for (BucketRecoveryContext ctx : contexts) {
                if (ctx.isComplete()) {
                    continue;
                }
                List<ScanRecord> bucketRecords = records.records(ctx.getBucket());
                if (!bucketRecords.isEmpty() && processRecords(ctx, bucketRecords, allFutures)) {
                    lastProgressNanos.put(ctx.getBucket(), progressTimeNanos);
                }
            }

            // Scanner position advances across both user records and empty batches, so it is the
            // completion signal for the bounded recovery range.
            for (BucketRecoveryContext ctx : contexts) {
                if (ctx.isComplete()) {
                    continue;
                }
                Long position = scanner.position(ctx.getBucket());
                if (position == null) {
                    throw new IllegalStateException(
                            String.format(
                                    "Log scanner returned null position for active bucket %s during undo recovery",
                                    ctx.getBucket()));
                }
                if (ctx.updateScanPosition(position)) {
                    lastProgressNanos.put(ctx.getBucket(), progressTimeNanos);
                }
                if (ctx.isComplete()) {
                    unsubscribeBucket(ctx.getBucket());
                }
            }

            long nowNanos = System.nanoTime();
            checkIdleTimeouts(contexts, lastProgressNanos, nowNanos);

            if (nowNanos - lastProgressLogTimeNanos
                    >= TimeUnit.MILLISECONDS.toNanos(PROGRESS_LOG_INTERVAL_MS)) {
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(nowNanos - startTimeNanos);
                LOG.info(
                        "Undo recovery waiting for changelog records: {} bucket(s) incomplete, "
                                + "waited {} minutes so far (per-bucket idle limit: {} minutes)",
                        countIncomplete(contexts),
                        String.format("%.1f", elapsedMs / 60000.0),
                        String.format("%.1f", maxIdleTimeMs / 60000.0));
                lastProgressLogTimeNanos = nowNanos;
            }
        }

        // Log summary
        if (LOG.isDebugEnabled()) {
            for (BucketRecoveryContext ctx : contexts) {
                LOG.debug(
                        "Bucket {} read {} records, executed {} undo operations",
                        ctx.getBucket(),
                        ctx.getTotalRecordsProcessed(),
                        ctx.getProcessedKeys().size());
            }
        }

        return allFutures;
    }

    private void checkIdleTimeouts(
            List<BucketRecoveryContext> contexts,
            Map<TableBucket, Long> lastProgressNanos,
            long nowNanos) {
        for (BucketRecoveryContext ctx : contexts) {
            if (ctx.isComplete()) {
                continue;
            }
            long idleNanos = nowNanos - lastProgressNanos.get(ctx.getBucket());
            if (idleNanos >= maxIdleTimeNanos) {
                throw new RuntimeException(
                        String.format(
                                "Undo recovery timed out: bucket=%s, baseline=%d, target=%d, "
                                        + "position=%d, idle=%dms (limit=%dms). "
                                        + "The job will restart and retry the recovery.",
                                ctx.getBucket(),
                                ctx.getCheckpointOffset(),
                                ctx.getLogEndOffset(),
                                ctx.getScanPosition(),
                                TimeUnit.NANOSECONDS.toMillis(idleNanos),
                                maxIdleTimeMs));
            }
        }
    }

    private void unsubscribeBucket(TableBucket bucket) {
        if (bucket.getPartitionId() != null) {
            scanner.unsubscribe(bucket.getPartitionId(), bucket.getBucket());
        } else {
            scanner.unsubscribe(bucket.getBucket());
        }
    }

    private boolean processRecords(
            BucketRecoveryContext ctx,
            List<ScanRecord> records,
            List<CompletableFuture<?>> futures) {
        Set<ByteArrayWrapper> processedKeys = ctx.getProcessedKeys();
        boolean madeProgress = false;
        for (ScanRecord record : records) {
            if (record.logOffset() >= ctx.getLogEndOffset()) {
                continue;
            }
            long previousProcessedOffset = ctx.getLastProcessedOffset();
            CompletableFuture<?> future = undoComputer.processRecord(record, processedKeys);
            if (future != null) {
                futures.add(future);
            }
            ctx.recordProcessed(record.logOffset());
            if (record.logOffset() > previousProcessedOffset) {
                madeProgress = true;
            }
        }
        return madeProgress;
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
