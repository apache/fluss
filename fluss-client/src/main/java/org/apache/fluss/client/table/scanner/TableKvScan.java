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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.scanner.batch.KvBatchScanner;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Implementation of {@link KvScan} that discovers all buckets across all partitions and chains
 * {@link KvBatchScanner} instances to produce a unified lazy iterator.
 *
 * <p>Bucket list is built eagerly at {@link #execute()} time; for partitioned tables partition
 * metadata is refreshed from the server before building the list. Individual {@link KvBatchScanner}
 * instances are created lazily one at a time as each bucket is consumed.
 */
public class TableKvScan implements KvScan {

    /**
     * Short poll timeout used inside the iterator's {@code hasNext()} loop. This keeps the busy
     * loop responsive while still allowing the prefetch to arrive.
     *
     * <p>TODO: make this configurable via a client config option.
     */
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(10);

    private final TableInfo tableInfo;
    private final SchemaGetter schemaGetter;
    private final MetadataUpdater metadataUpdater;

    public TableKvScan(
            TableInfo tableInfo, SchemaGetter schemaGetter, MetadataUpdater metadataUpdater) {
        this.tableInfo = tableInfo;
        this.schemaGetter = schemaGetter;
        this.metadataUpdater = metadataUpdater;
    }

    @Override
    public CloseableIterator<InternalRow> execute() {
        return new AllBucketsIterator(discoverBuckets());
    }

    /**
     * Returns the full ordered list of {@link TableBucket}s to scan.
     *
     * <ul>
     *   <li>Non-partitioned tables: buckets 0 … numBuckets-1, no partitionId.
     *   <li>Partitioned tables: metadata refreshed from the server; buckets 0 … numBuckets-1 for
     *       each discovered partition.
     * </ul>
     */
    private List<TableBucket> discoverBuckets() {
        TablePath tablePath = tableInfo.getTablePath();
        long tableId = tableInfo.getTableId();
        int numBuckets = tableInfo.getNumBuckets();
        List<TableBucket> buckets = new ArrayList<>();

        if (!tableInfo.isPartitioned()) {
            for (int b = 0; b < numBuckets; b++) {
                buckets.add(new TableBucket(tableId, b));
            }
        } else {
            // Ensure partition metadata is current before iterating.
            metadataUpdater.updateTableOrPartitionMetadata(tablePath, null);
            // TODO: getPartitionIdByPath() iterates all partitions for all tables in the cluster.
            //  Replace with a targeted per-table lookup once the API is available.
            Map<PhysicalTablePath, Long> partitionIdByPath =
                    metadataUpdater.getCluster().getPartitionIdByPath();
            for (Map.Entry<PhysicalTablePath, Long> entry : partitionIdByPath.entrySet()) {
                if (tablePath.equals(entry.getKey().getTablePath())) {
                    long partitionId = entry.getValue();
                    for (int b = 0; b < numBuckets; b++) {
                        buckets.add(new TableBucket(tableId, partitionId, b));
                    }
                }
            }
        }
        return buckets;
    }

    /** Chains through all per-bucket {@link KvBatchScanner}s as a single lazy iterator. */
    private class AllBucketsIterator implements CloseableIterator<InternalRow> {

        private final List<TableBucket> buckets;
        private int bucketIndex = 0;
        @Nullable private KvBatchScanner currentScanner = null;
        @Nullable private CloseableIterator<InternalRow> currentBatch = null;
        private boolean exhausted = false;

        AllBucketsIterator(List<TableBucket> buckets) {
            this.buckets = buckets;
        }

        @Override
        public boolean hasNext() {
            if (exhausted) {
                return false;
            }
            if (currentBatch != null && currentBatch.hasNext()) {
                return true;
            }
            return fillNextBatch();
        }

        /**
         * Advances {@link #currentBatch} to the next non-empty batch, opening the next scanner when
         * the current one is exhausted. Returns {@code true} once a row is available.
         */
        private boolean fillNextBatch() {
            while (true) {
                if (currentScanner == null) {
                    if (bucketIndex >= buckets.size()) {
                        exhausted = true;
                        return false;
                    }
                    currentScanner =
                            new KvBatchScanner(
                                    tableInfo,
                                    buckets.get(bucketIndex++),
                                    schemaGetter,
                                    metadataUpdater);
                }
                try {
                    CloseableIterator<InternalRow> batch = currentScanner.pollBatch(POLL_TIMEOUT);
                    if (batch == null) {
                        // Bucket exhausted; server session already cleaned up.
                        currentScanner = null;
                        continue;
                    }
                    if (batch.hasNext()) {
                        currentBatch = batch;
                        return true;
                    }
                    // Empty batch (RPC not yet ready) — close and spin.
                    batch.close();
                } catch (IOException e) {
                    throw new FlussRuntimeException("Failed to poll KV scan batch", e);
                }
            }
        }

        @Override
        public InternalRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentBatch.next();
        }

        @Override
        public void close() {
            exhausted = true;
            currentBatch = null;
            if (currentScanner != null) {
                try {
                    currentScanner.close();
                } catch (IOException ignored) {
                }
                currentScanner = null;
            }
        }
    }
}
