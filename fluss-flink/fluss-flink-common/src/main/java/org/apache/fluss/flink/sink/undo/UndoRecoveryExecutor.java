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
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.config.ConfigOptions.CLIENT_IO_TMP_DIR;

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
 *   <li>Subscribe all buckets that need recovery
 *   <li>Poll changelog records and execute undo operations immediately
 *   <li>Track acknowledged offsets and failures in write callbacks
 *   <li>Flush once and wait for the callbacks before returning the recovered offsets
 * </ol>
 */
public class UndoRecoveryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryExecutor.class);

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
    Map<TableBucket, Long> execute(List<BucketRecoveryContext> contexts) throws Exception {
        return execute(contexts, Paths.get(CLIENT_IO_TMP_DIR.defaultValue()));
    }

    /** Executes undo recovery using a temporary key store under the supplied directory. */
    public Map<TableBucket, Long> execute(List<BucketRecoveryContext> contexts, Path temporaryRoot)
            throws Exception {
        Map<TableBucket, Long> recoveredOffsets = new HashMap<>();
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

        WriteResults writeResults = new WriteResults(recoveredOffsets);
        try (UndoRecoveryDeduplicationStore deduplicationStore =
                new UndoRecoveryDeduplicationStore(temporaryRoot)) {
            readChangelogAndExecute(toRecover, deduplicationStore, writeResults);
            writeResults.checkAsyncException();
            writer.flush();
            writeResults.awaitCallbacks();
            writeResults.checkAsyncException();
        }

        return recoveredOffsets;
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

    private void subscribe(BucketRecoveryContext context) {
        TableBucket bucket = context.getBucket();
        if (bucket.getPartitionId() != null) {
            scanner.subscribe(
                    bucket.getPartitionId(), bucket.getBucket(), context.getCheckpointOffset());
        } else {
            scanner.subscribe(bucket.getBucket(), context.getCheckpointOffset());
        }
    }

    private void readChangelogAndExecute(
            List<BucketRecoveryContext> contexts,
            UndoRecoveryDeduplicationStore deduplicationStore,
            WriteResults writeResults)
            throws Exception {
        long startTimeMs = System.currentTimeMillis();
        long lastProgressLogTime = System.currentTimeMillis();
        Map<TableBucket, BucketRecoveryContext> active = new LinkedHashMap<>();
        for (BucketRecoveryContext context : contexts) {
            subscribe(context);
            active.put(context.getBucket(), context);
        }

        while (!active.isEmpty()) {
            if (Thread.interrupted()) {
                throw new InterruptedException("Undo recovery scan interrupted");
            }
            writeResults.checkAsyncException();
            ScanRecords records;
            if (scanner instanceof LogScannerImpl) {
                // Reserve keys only within this poll. Persistent deduplication is updated after
                // the selected rows have been copied and their undo writes have been submitted.
                Map<TableBucket, Map<ByteArrayWrapper, Long>> selectedKeys = new HashMap<>();
                records =
                        ((LogScannerImpl) scanner)
                                .pollFiltered(
                                        Duration.ofMillis(POLL_TIMEOUT_MS),
                                        (bucket, record) -> {
                                            BucketRecoveryContext context = active.get(bucket);
                                            if (context == null
                                                    || record.logOffset()
                                                            >= context.getLogEndOffset()) {
                                                return false;
                                            }
                                            return undoComputer.selectRecord(
                                                    record,
                                                    deduplicationStore,
                                                    selectedKeys.computeIfAbsent(
                                                            bucket, ignored -> new HashMap<>()));
                                        });
            } else {
                records = scanner.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
            }
            writeResults.checkAsyncException();

            if (!records.hasProgress()) {
                // Check if we've exceeded the maximum total wait time
                long elapsedMs = System.currentTimeMillis() - startTimeMs;
                if (elapsedMs >= maxTotalWaitTimeMs) {
                    throw new RuntimeException(
                            String.format(
                                    "Undo recovery timed out: unable to read all changelog records after "
                                            + "%.1f minutes of waiting. %d bucket(s) still incomplete. "
                                            + "The job will restart and retry the recovery.",
                                    elapsedMs / 60000.0, active.size()));
                }

                LOG.debug("Empty poll (total elapsed: {}ms)", elapsedMs);
            }

            // Process records before applying the post-poll progress. A poll may consume the
            // complete recovery range while returning its final records.
            Iterator<BucketRecoveryContext> activeIterator = active.values().iterator();
            while (activeIterator.hasNext()) {
                BucketRecoveryContext ctx = activeIterator.next();
                List<ScanRecord> bucketRecords = records.records(ctx.getBucket());
                if (!bucketRecords.isEmpty()) {
                    processRecords(ctx, bucketRecords, deduplicationStore, writeResults);
                }
                Long consumedUpToOffset = records.consumedUpToOffset(ctx.getBucket());
                if (consumedUpToOffset != null) {
                    ctx.updateScanPosition(consumedUpToOffset);
                }
                if (ctx.isComplete()) {
                    unsubscribeBucket(ctx.getBucket());
                    activeIterator.remove();
                }
            }

            // Log progress periodically (at the end of each loop iteration)
            long now = System.currentTimeMillis();
            if (now - lastProgressLogTime >= PROGRESS_LOG_INTERVAL_MS) {
                long elapsedMs = now - startTimeMs;
                LOG.info(
                        "Undo recovery waiting for changelog records: {} bucket(s) incomplete, "
                                + "waited {} minutes so far (max: {} minutes)",
                        active.size(),
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
            UndoRecoveryDeduplicationStore deduplicationStore,
            WriteResults writeResults)
            throws Exception {
        TableBucket bucket = ctx.getBucket();
        for (ScanRecord record : records) {
            writeResults.checkAsyncException();
            if (record.logOffset() >= ctx.getLogEndOffset()) {
                continue;
            }
            CompletableFuture<Long> future = undoComputer.processRecord(record, deduplicationStore);
            if (future != null) {
                ctx.recordUndoOperation();
                writeResults.track(bucket, future);
            }
            ctx.recordProcessed();
        }
    }

    /** Tracks callback completion independently of the client's flush bookkeeping. */
    private static final class WriteResults {
        private final Map<TableBucket, Long> recoveredOffsets;
        private long pendingCallbacks;
        private volatile Throwable asyncWriterException;

        private WriteResults(Map<TableBucket, Long> recoveredOffsets) {
            this.recoveredOffsets = recoveredOffsets;
        }

        private void track(TableBucket bucket, CompletableFuture<Long> future) {
            synchronized (this) {
                pendingCallbacks++;
            }
            future.whenComplete((offset, failure) -> complete(bucket, offset, failure));
        }

        private synchronized void complete(TableBucket bucket, Long offset, Throwable failure) {
            if (failure == null) {
                recoveredOffsets.merge(bucket, offset, Math::max);
            } else if (asyncWriterException == null) {
                asyncWriterException = ExceptionUtils.stripCompletionException(failure);
            }
            if (--pendingCallbacks == 0) {
                notifyAll();
            }
        }

        private synchronized void awaitCallbacks() throws InterruptedException {
            // Fatal writer cleanup can remove a batch before its result callbacks finish.
            while (pendingCallbacks != 0) {
                wait();
            }
        }

        private void checkAsyncException() throws ExecutionException {
            Throwable failure = asyncWriterException;
            if (failure != null) {
                throw new ExecutionException(failure);
            }
        }
    }
}
