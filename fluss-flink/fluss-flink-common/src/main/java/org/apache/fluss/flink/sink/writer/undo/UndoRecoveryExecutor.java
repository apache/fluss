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

package org.apache.fluss.flink.sink.writer.undo;

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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

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
 *   <li>Wait for all futures and flush to ensure all writes complete
 * </ol>
 */
public class UndoRecoveryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryExecutor.class);

    /** Default initial poll timeout in milliseconds. */
    private static final long DEFAULT_INITIAL_POLL_TIMEOUT_MS = 100;

    /** Default maximum poll timeout in milliseconds after exponential backoff. */
    private static final long DEFAULT_MAX_POLL_TIMEOUT_MS = 30_000; // 30 seconds

    /** Default maximum total wait time in milliseconds before failing (1 hour). */
    private static final long DEFAULT_MAX_TOTAL_WAIT_TIME_MS = 60 * 60 * 1000; // 1 hour

    /** Interval for logging progress during long waits (5 minutes). */
    private static final long PROGRESS_LOG_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

    private final LogScanner scanner;
    private final UpsertWriter writer;
    private final UndoComputer undoComputer;

    // Configurable timeout parameters
    private final long initialPollTimeoutMs;
    private final long maxPollTimeoutMs;
    private final long maxTotalWaitTimeMs;

    public UndoRecoveryExecutor(
            LogScanner scanner, UpsertWriter writer, UndoComputer undoComputer) {
        this(
                scanner,
                writer,
                undoComputer,
                DEFAULT_INITIAL_POLL_TIMEOUT_MS,
                DEFAULT_MAX_POLL_TIMEOUT_MS,
                DEFAULT_MAX_TOTAL_WAIT_TIME_MS);
    }

    /**
     * Creates an executor with custom timeout configuration (for testing).
     *
     * @param scanner the log scanner
     * @param writer the upsert writer
     * @param undoComputer the undo computer
     * @param initialPollTimeoutMs initial poll timeout in milliseconds
     * @param maxPollTimeoutMs maximum poll timeout after exponential backoff
     * @param maxTotalWaitTimeMs maximum total wait time before failing
     */
    public UndoRecoveryExecutor(
            LogScanner scanner,
            UpsertWriter writer,
            UndoComputer undoComputer,
            long initialPollTimeoutMs,
            long maxPollTimeoutMs,
            long maxTotalWaitTimeMs) {
        this.scanner = scanner;
        this.writer = writer;
        this.undoComputer = undoComputer;
        this.initialPollTimeoutMs = initialPollTimeoutMs;
        this.maxPollTimeoutMs = maxPollTimeoutMs;
        this.maxTotalWaitTimeMs = maxTotalWaitTimeMs;
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

        // Wait for all async writes to complete
        if (!allFutures.isEmpty()) {
            CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).get();
        }
        writer.flush();

        // Log summary at INFO level for visibility
        int totalUndoOps = 0;
        for (BucketRecoveryContext ctx : toRecover) {
            totalUndoOps += ctx.getProcessedKeys().size();
        }

        LOG.debug(
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

        long currentPollTimeoutMs = initialPollTimeoutMs;
        long totalWaitTimeMs = 0;
        long lastProgressLogTime = System.currentTimeMillis();

        while (!allComplete(contexts)) {
            ScanRecords records = scanner.poll(Duration.ofMillis(currentPollTimeoutMs));

            if (records.isEmpty()) {
                totalWaitTimeMs += currentPollTimeoutMs;

                // Check if we've exceeded the maximum total wait time
                if (totalWaitTimeMs >= maxTotalWaitTimeMs) {
                    // Undo recovery failure is fatal - incomplete undo leads to data inconsistency
                    String incompleteBucketsDetail = getIncompleteBucketsDetail(contexts);
                    throw new RuntimeException(
                            String.format(
                                    "Undo recovery failed: unable to read all changelog records after "
                                            + "%.1f minutes of waiting. Incomplete buckets: %s. "
                                            + "This is a fatal error as incomplete undo recovery "
                                            + "would lead to data inconsistency.",
                                    totalWaitTimeMs / 60000.0, incompleteBucketsDetail));
                }

                // Log progress periodically during long waits
                long now = System.currentTimeMillis();
                if (now - lastProgressLogTime >= PROGRESS_LOG_INTERVAL_MS) {
                    LOG.info(
                            "Undo recovery waiting for changelog records: {} bucket(s) incomplete, "
                                    + "waited {} minutes so far (max: {} minutes)",
                            countIncomplete(contexts),
                            String.format("%.1f", totalWaitTimeMs / 60000.0),
                            String.format("%.1f", maxTotalWaitTimeMs / 60000.0));
                    lastProgressLogTime = now;
                }

                // Exponential backoff: double the timeout up to maxPollTimeoutMs
                currentPollTimeoutMs = Math.min(currentPollTimeoutMs * 2, maxPollTimeoutMs);
                LOG.debug(
                        "Empty poll, backing off to {}ms timeout (total wait: {}ms)",
                        currentPollTimeoutMs,
                        totalWaitTimeMs);
                continue;
            }

            // Reset timeout on successful poll (but keep tracking total wait time for logging)
            currentPollTimeoutMs = initialPollTimeoutMs;

            // Process records for each bucket, resetting writer state before each bucket
            for (BucketRecoveryContext ctx : contexts) {
                if (ctx.isComplete()) {
                    continue;
                }
                List<ScanRecord> bucketRecords = records.records(ctx.getBucket());
                if (!bucketRecords.isEmpty()) {
                    undoComputer.resetWriterState();
                    processRecords(ctx, bucketRecords, allFutures);
                }
            }
        }

        // Log summary
        for (BucketRecoveryContext ctx : contexts) {
            LOG.debug(
                    "Bucket {} read {} records, executed {} undo operations",
                    ctx.getBucket(),
                    ctx.getTotalRecordsProcessed(),
                    ctx.getProcessedKeys().size());
        }

        return allFutures;
    }

    /**
     * Gets detailed information about incomplete buckets for error reporting.
     *
     * @param contexts all bucket contexts
     * @return formatted string with detailed status of each incomplete bucket
     */
    private String getIncompleteBucketsDetail(List<BucketRecoveryContext> contexts) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (BucketRecoveryContext ctx : contexts) {
            if (!ctx.isComplete()) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(ctx.getBucket())
                        .append("{checkpointOffset=")
                        .append(ctx.getCheckpointOffset())
                        .append(", logEndOffset=")
                        .append(ctx.getLogEndOffset())
                        .append(", lastProcessedOffset=")
                        .append(ctx.getLastProcessedOffset())
                        .append(", recordsProcessed=")
                        .append(ctx.getTotalRecordsProcessed())
                        .append("}");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    private void processRecords(
            BucketRecoveryContext ctx,
            List<ScanRecord> records,
            List<CompletableFuture<?>> futures) {
        Set<ByteArrayWrapper> processedKeys = ctx.getProcessedKeys();
        for (ScanRecord record : records) {
            CompletableFuture<?> future = undoComputer.processRecord(record, processedKeys);
            if (future != null) {
                futures.add(future);
            }
            ctx.recordProcessed(record.logOffset());
            if (ctx.isComplete()) {
                break;
            }
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
