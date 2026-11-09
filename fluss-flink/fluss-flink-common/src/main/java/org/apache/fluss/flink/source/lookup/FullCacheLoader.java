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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.KvSnapshotLease;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.KvBatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.KvBatchStrategy;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Loads snapshots and changelog records for the full lookup cache.
 *
 * <p>Loads either remote SST snapshots or server-side KV scans into the local cache, then replays
 * changelog records directly into that cache up to fixed per-bucket offsets before continuously
 * consuming updates. No in-memory changelog merge is needed. The caller must keep lookups blocked
 * until replay completes and reuse the log scanner for continuous consumption so records polled
 * beyond the targets are not lost. Cache synchronization, readiness, and resource lifecycle are
 * managed by the caller.
 *
 * <p>This class is <b>not thread-safe</b>; a single calling thread must own the loading process.
 */
@Internal
@NotThreadSafe
final class FullCacheLoader {

    private static final Logger LOG = LoggerFactory.getLogger(FullCacheLoader.class);
    private static final Duration SCAN_POLL_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration LOG_POLL_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration SNAPSHOT_LEASE_DURATION = Duration.ofDays(1);

    private final Admin admin;
    private final KvBatchStrategy strategy;
    private final Table table;
    private final TableInfo tableInfo;
    private final Set<Integer> ownedBuckets;
    @Nullable private KvSnapshotLease snapshotLease;
    private long nextLeaseRenewalNanos;

    FullCacheLoader(
            Admin admin,
            KvBatchStrategy strategy,
            Table table,
            TableInfo tableInfo,
            Set<Integer> ownedBuckets) {
        this.admin = checkNotNull(admin, "admin must not be null.");
        this.strategy = checkNotNull(strategy, "strategy must not be null.");
        this.table = checkNotNull(table, "table must not be null.");
        this.tableInfo = checkNotNull(tableInfo, "tableInfo must not be null.");
        this.ownedBuckets = checkNotNull(ownedBuckets, "ownedBuckets must not be null.");
    }

    /**
     * Scans all owned buckets' KV snapshots and passes rows to the callback. Blocks until all
     * buckets are fully scanned.
     *
     * @param snapshotConsumer callback that synchronously consumes each snapshot row
     * @return result with row count and per-bucket snapshot offsets
     * @throws Exception if any bucket scan fails
     */
    Result runSnapshot(SnapshotRowConsumer snapshotConsumer) throws Exception {
        checkNotNull(snapshotConsumer, "snapshotConsumer must not be null.");
        long snapshotRows;
        Map<Integer, Long> bucketSnapshotOffsets = new HashMap<>();
        if (strategy == KvBatchStrategy.SNAPSHOT_MERGE) {
            snapshotRows = loadSnapshotFiles(snapshotConsumer, bucketSnapshotOffsets);
        } else {
            snapshotRows = scanServerSnapshots(snapshotConsumer, bucketSnapshotOffsets);
        }
        return new Result(snapshotRows, bucketSnapshotOffsets);
    }

    /**
     * Replays at least through the offsets captured after loading snapshots. Targets are fixed so
     * continuous writes cannot keep moving the readiness barrier. Whole polled batches are applied,
     * including records past a target, and the same scanner must then serve continuous consumption.
     */
    void replayChangelog(
            LogScanner scanner,
            Map<Integer, Long> snapshotOffsets,
            ChangelogConsumer changelogConsumer)
            throws Exception {
        Map<Integer, Long> targets =
                new HashMap<>(
                        admin.listOffsets(
                                        tableInfo.getTablePath(),
                                        ownedBuckets,
                                        new OffsetSpec.LatestSpec())
                                .all()
                                .get());
        for (Integer bucket : ownedBuckets) {
            Long start = snapshotOffsets.get(bucket);
            Long target = targets.get(bucket);
            checkState(
                    start != null && target != null,
                    "Missing bootstrap offset for bucket %s.",
                    bucket);
            scanner.subscribe(bucket, start);
            if (start >= target) {
                targets.remove(bucket);
            }
        }
        while (!targets.isEmpty()) {
            checkInterrupted();
            ScanRecords records = scanner.poll(LOG_POLL_TIMEOUT);
            changelogConsumer.accept(records);
            for (TableBucket bucket : records.buckets()) {
                Long target = targets.get(bucket.getBucket());
                if (target == null) {
                    continue;
                }
                Long consumed = records.consumedUpToOffset(bucket);
                if (consumed != null && consumed >= target) {
                    targets.remove(bucket.getBucket());
                    continue;
                }
                for (ScanRecord record : records.records(bucket)) {
                    if (record.logOffset() >= target - 1) {
                        targets.remove(bucket.getBucket());
                        break;
                    }
                }
            }
        }
    }

    /**
     * Consumes changelog updates until the caller closes the cache. Transient fetch recovery is
     * handled by the client scanner; any escaped failure is propagated to the caller.
     */
    void consumeChangelog(
            LogScanner scanner, ChangelogConsumer changelogConsumer, BooleanSupplier isClosed)
            throws Exception {
        while (!isClosed.getAsBoolean()) {
            ScanRecords records = scanner.poll(LOG_POLL_TIMEOUT);
            if (records.hasProgress()) {
                changelogConsumer.accept(records);
            }
        }
    }

    // ----------------------- internal methods -------------------------------
    private long scanServerSnapshots(
            SnapshotRowConsumer snapshotConsumer, Map<Integer, Long> offsets) throws Exception {
        long snapshotRows = 0L;
        for (Integer bucketId : ownedBuckets) {
            checkInterrupted();
            TableBucket bucket = new TableBucket(tableInfo.getTableId(), bucketId);
            KvBatchScanner scanner = (KvBatchScanner) table.newScan().createBatchScanner(bucket);
            CloseableIterator<InternalRow> firstBatch = null;
            try {
                firstBatch = pollInitialSnapshotBatch(scanner, bucketId);
                OptionalLong snapshotOffset = scanner.getSnapshotLogOffset();
                checkState(
                        snapshotOffset.isPresent(),
                        "KV scanner for bucket %s did not return a snapshot log offset.",
                        bucketId);
                snapshotRows += drainBatches(scanner, firstBatch, snapshotConsumer);
                offsets.put(bucketId, snapshotOffset.getAsLong());
            } finally {
                IOUtils.closeQuietly(firstBatch);
                IOUtils.closeQuietly(scanner);
            }
        }
        return snapshotRows;
    }

    private long loadSnapshotFiles(SnapshotRowConsumer snapshotConsumer, Map<Integer, Long> offsets)
            throws Exception {
        KvSnapshots snapshots = admin.getLatestKvSnapshots(tableInfo.getTablePath()).get();
        checkState(
                snapshots.getTableId() == tableInfo.getTableId(),
                "Table changed during bootstrap.");
        Map<TableBucket, Long> snapshotsToLease = new HashMap<>();
        for (Integer bucketId : ownedBuckets) {
            OptionalLong snapshotId = snapshots.getSnapshotId(bucketId);
            if (snapshotId.isPresent()) {
                snapshotsToLease.put(
                        new TableBucket(tableInfo.getTableId(), bucketId), snapshotId.getAsLong());
            }
        }

        long snapshotRows = 0L;
        try {
            acquireSnapshotLease(snapshotsToLease);
            for (Integer bucketId : ownedBuckets) {
                checkInterrupted();
                OptionalLong snapshotId = snapshots.getSnapshotId(bucketId);
                if (!snapshotId.isPresent()) {
                    // Starting at the current earliest offset could silently omit deleted logs.
                    // Without a snapshot we need the complete history; missing logs must fail.
                    offsets.put(bucketId, 0L);
                    continue;
                }
                OptionalLong offset = snapshots.getLogOffset(bucketId);
                checkState(
                        offset.isPresent(), "Snapshot for bucket %s has no log offset.", bucketId);
                TableBucket bucket = new TableBucket(tableInfo.getTableId(), bucketId);
                try (BatchScanner scanner =
                        table.newScan().createBatchScanner(bucket, snapshotId.getAsLong())) {
                    snapshotRows +=
                            drainBatches(
                                    scanner,
                                    scanner.pollBatch(SCAN_POLL_TIMEOUT),
                                    snapshotConsumer);
                }
                offsets.put(bucketId, offset.getAsLong());
            }
        } finally {
            if (snapshotLease != null) {
                snapshotLease
                        .dropLease()
                        .whenComplete(
                                (ignored, failure) -> {
                                    if (failure != null) {
                                        LOG.warn(
                                                "Failed to drop full-cache snapshot lease.",
                                                failure);
                                    }
                                });
                snapshotLease = null;
            }
        }
        return snapshotRows;
    }

    private void acquireSnapshotLease(Map<TableBucket, Long> snapshots) throws Exception {
        if (snapshots.isEmpty()) {
            return;
        }
        snapshotLease =
                admin.createKvSnapshotLease(
                        "lookup-full-" + UUID.randomUUID(), SNAPSHOT_LEASE_DURATION.toMillis());
        try {
            Set<TableBucket> unavailable =
                    snapshotLease.acquireSnapshots(snapshots).get().getUnavailableTableBucketSet();
            checkState(
                    unavailable.isEmpty(),
                    "Snapshots are no longer available for %s.",
                    unavailable);
            nextLeaseRenewalNanos = System.nanoTime() + SNAPSHOT_LEASE_DURATION.toNanos() / 2;
        } catch (Exception e) {
            if (!ExceptionUtils.findThrowable(e, UnsupportedVersionException.class).isPresent()) {
                throw e;
            }
            // As with the Flink source, older servers can still read SST snapshots without leases.
            snapshotLease = null;
            LOG.warn(
                    "Server does not support snapshot leases; full-cache bootstrap will read SST "
                            + "snapshots without retention protection. Snapshot cleanup may fail the scan.");
        }
    }

    private void renewSnapshotLease() throws Exception {
        if (snapshotLease != null && System.nanoTime() - nextLeaseRenewalNanos >= 0) {
            snapshotLease.renew().get();
            nextLeaseRenewalNanos = System.nanoTime() + SNAPSHOT_LEASE_DURATION.toNanos() / 2;
        }
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Full-cache bootstrap interrupted.");
        }
    }

    /**
     * Polls until the scanner returns a batch that carries the snapshot log offset.
     *
     * <p>The offset is reported with the first successful scan response, so any batch returned
     * before that response must be empty. A batch with rows but no offset would mean rows were read
     * without a changelog fence and is treated as a fatal invariant violation.
     */
    @Nullable
    private CloseableIterator<InternalRow> pollInitialSnapshotBatch(
            KvBatchScanner scanner, int bucketId) throws IOException {
        while (true) {
            CloseableIterator<InternalRow> batch = scanner.pollBatch(SCAN_POLL_TIMEOUT);
            if (scanner.getSnapshotLogOffset().isPresent()) {
                return batch;
            }
            checkState(
                    batch != null,
                    "KV scanner for bucket %s finished without a snapshot log offset.",
                    bucketId);
            try {
                checkState(
                        !batch.hasNext(),
                        "KV scanner for bucket %s returned rows without a snapshot log offset.",
                        bucketId);
            } finally {
                batch.close();
            }
        }
    }

    /** Drains the first batch (if any) and all subsequent batches until the scan is exhausted. */
    private long drainBatches(
            BatchScanner scanner,
            @Nullable CloseableIterator<InternalRow> firstBatch,
            SnapshotRowConsumer snapshotConsumer)
            throws Exception {
        long rowCount = 0L;
        CloseableIterator<InternalRow> batch = firstBatch;
        while (batch != null) {
            try {
                renewSnapshotLease();
                while (batch.hasNext()) {
                    checkInterrupted();
                    if (rowCount % 1024 == 0) {
                        renewSnapshotLease();
                    }
                    snapshotConsumer.accept(batch.next());
                    rowCount++;
                }
            } finally {
                batch.close();
            }
            batch = scanner.pollBatch(SCAN_POLL_TIMEOUT);
        }
        return rowCount;
    }

    /** Synchronously consumes snapshot rows before the scanner can reuse them. */
    @FunctionalInterface
    interface SnapshotRowConsumer {

        /** Consumes the row without retaining it beyond this call. */
        void accept(InternalRow row) throws Exception;
    }

    /** Applies changelog records directly to the local cache. */
    @FunctionalInterface
    interface ChangelogConsumer {

        /** Applies a polled batch in order before reporting its consumed offsets. */
        void accept(ScanRecords records) throws Exception;
    }

    /** Result of the completed snapshot loading phase. */
    static final class Result {

        private final long snapshotRows;
        private final Map<Integer, Long> bucketSnapshotOffsets;

        private Result(long snapshotRows, Map<Integer, Long> bucketSnapshotOffsets) {
            this.snapshotRows = snapshotRows;
            this.bucketSnapshotOffsets =
                    Collections.unmodifiableMap(new HashMap<>(bucketSnapshotOffsets));
        }

        /** Returns the total number of snapshot rows written to the cache. */
        public long getSnapshotRows() {
            return snapshotRows;
        }

        /**
         * Returns the next log offset after each loaded snapshot, keyed by bucket id. Buckets
         * without an SST snapshot start at zero and require their complete changelog history.
         */
        public Map<Integer, Long> getBucketSnapshotOffsets() {
            return bucketSnapshotOffsets;
        }
    }
}
