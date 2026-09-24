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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.KvBatchScanner;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Orchestrates the snapshot phase of the full lookup cache bootstrap.
 *
 * <p>For each owned bucket, opens a {@link KvBatchScanner}, drains all snapshot rows via a {@link
 * RowSink} callback, and records the snapshot log offset. Buckets are scanned sequentially, so at
 * most one KV scanner session is open at a time. The caller is responsible for subscribing CDC
 * after bootstrap completes.
 *
 * <p>This class is <b>not thread-safe</b>; a single calling thread must own the bootstrap.
 */
@Internal
@NotThreadSafe
final class FullCacheBootstrapper {

    private static final Duration SCAN_POLL_TIMEOUT = Duration.ofSeconds(30);

    private final Table table;
    private final TableInfo tableInfo;
    private final Set<Integer> ownedBuckets;
    private final KeyEncoder primaryKeyEncoder;
    private final RowSerializer rowSerializer;

    FullCacheBootstrapper(
            Table table,
            TableInfo tableInfo,
            Set<Integer> ownedBuckets,
            KeyEncoder primaryKeyEncoder,
            RowSerializer rowSerializer) {
        this.table = checkNotNull(table, "table must not be null.");
        this.tableInfo = checkNotNull(tableInfo, "tableInfo must not be null.");
        this.ownedBuckets = checkNotNull(ownedBuckets, "ownedBuckets must not be null.");
        this.primaryKeyEncoder =
                checkNotNull(primaryKeyEncoder, "primaryKeyEncoder must not be null.");
        this.rowSerializer = checkNotNull(rowSerializer, "rowSerializer must not be null.");
    }

    /**
     * Scans all owned buckets' KV snapshots and writes rows via the sink. Blocks until all buckets
     * are fully scanned.
     *
     * @param snapshotSink callback that receives encoded (key, value) pairs
     * @return result with row count, duration, and per-bucket snapshot offsets
     * @throws Exception if any bucket scan fails
     */
    Result run(RowSink snapshotSink) throws Exception {
        checkNotNull(snapshotSink, "snapshotSink must not be null.");
        long startNanos = System.nanoTime();
        long snapshotRows = 0L;
        Map<Integer, Long> bucketSnapshotOffsets = new HashMap<>();
        for (Integer bucketId : ownedBuckets) {
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
                snapshotRows += drainBatches(scanner, firstBatch, snapshotSink);
                bucketSnapshotOffsets.put(bucketId, snapshotOffset.getAsLong());
            } finally {
                IOUtils.closeQuietly(firstBatch);
                IOUtils.closeQuietly(scanner);
            }
        }
        long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;
        return new Result(snapshotRows, durationMs, bucketSnapshotOffsets);
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
            KvBatchScanner scanner,
            @Nullable CloseableIterator<InternalRow> firstBatch,
            RowSink snapshotSink)
            throws Exception {
        long rowCount = 0L;
        CloseableIterator<InternalRow> batch = firstBatch;
        while (batch != null) {
            try {
                while (batch.hasNext()) {
                    putSnapshotRow(snapshotSink, batch.next());
                    rowCount++;
                }
            } finally {
                batch.close();
            }
            batch = scanner.pollBatch(SCAN_POLL_TIMEOUT);
        }
        return rowCount;
    }

    /** Encodes one snapshot row and hands an independent key-value pair to the sink. */
    private void putSnapshotRow(RowSink snapshotSink, InternalRow row) throws Exception {
        byte[] key = primaryKeyEncoder.encodeKey(row);
        BinaryRow binaryRow = rowSerializer.toBinaryRow(row);
        byte[] value = new byte[binaryRow.getSizeInBytes()];
        binaryRow.copyTo(value, 0);
        snapshotSink.accept(key, value);
    }

    /** Callback that receives the encoded key-value pairs produced by the snapshot scan. */
    @FunctionalInterface
    interface RowSink {

        /**
         * Writes one encoded key-value pair into the cache store.
         *
         * @param key the encoded primary key
         * @param value the serialized row value
         */
        void accept(byte[] key, byte[] value) throws Exception;
    }

    /** Result of a completed bootstrap. */
    static final class Result {

        private final long snapshotRows;
        private final long durationMs;
        private final Map<Integer, Long> bucketSnapshotOffsets;

        private Result(
                long snapshotRows, long durationMs, Map<Integer, Long> bucketSnapshotOffsets) {
            this.snapshotRows = snapshotRows;
            this.durationMs = durationMs;
            this.bucketSnapshotOffsets =
                    Collections.unmodifiableMap(new HashMap<>(bucketSnapshotOffsets));
        }

        /** Returns the total number of snapshot rows written to the cache. */
        public long getSnapshotRows() {
            return snapshotRows;
        }

        /** Returns the wall-clock duration of the snapshot bootstrap, in milliseconds. */
        public long getDurationMs() {
            return durationMs;
        }

        /**
         * Returns the exclusive log offset captured when each owned bucket's snapshot was opened,
         * keyed by bucket id. CDC consumption for a bucket must start at its snapshot offset.
         */
        public Map<Integer, Long> getBucketSnapshotOffsets() {
            return bucketSnapshotOffsets;
        }
    }
}
