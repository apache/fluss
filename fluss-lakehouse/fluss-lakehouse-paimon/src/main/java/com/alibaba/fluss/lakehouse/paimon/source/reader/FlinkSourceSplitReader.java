/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.lakehouse.paimon.source.reader;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.batch.BatchScanner;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.paimon.record.CdcRecord;
import com.alibaba.fluss.lakehouse.paimon.source.metrics.FlinkMetricRegistry;
import com.alibaba.fluss.lakehouse.paimon.source.metrics.FlinkSourceReaderMetrics;
import com.alibaba.fluss.lakehouse.paimon.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.lakehouse.paimon.source.split.LogSplit;
import com.alibaba.fluss.lakehouse.paimon.source.split.SnapshotSplit;
import com.alibaba.fluss.lakehouse.paimon.source.split.SourceSplitBase;
import com.alibaba.fluss.lakehouse.paimon.source.utils.FlussRowToFlinkRowConverter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * An implementation of {@link SplitReader} for reading splits into {@link
 * MultiplexCdcRecordAndPos}.
 *
 * <p>It'll first read the {@link SnapshotSplit}s if any and then switch to read {@link LogSplit}s .
 */
public class FlinkSourceSplitReader
        implements SplitReader<MultiplexCdcRecordAndPos, SourceSplitBase> {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(3000L);

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceSplitReader.class);

    private final Connection connection;
    private final FlinkSourceReaderMetrics flinkSourceReaderMetrics;
    private final FlinkMetricRegistry flinkMetricRegistry;

    private final Map<Long, Queue<SnapshotSplit>> tableSnapshotSplits = new HashMap<>();
    private final Map<Long, Table> tablesByTableId = new HashMap<>();
    private final Map<Long, TablePath> tablesPathByTableId = new HashMap<>();
    private final Map<Long, FlussRowToFlinkRowConverter> rowConvertByTableId = new HashMap<>();
    private final Map<Long, LogScanner> logScannersByTableId = new HashMap<>();
    private final Map<Long, Set<TableBucket>> subscribedBucketsByTableId = new HashMap<>();
    private final Map<TableBucket, String> splitIdByTableBucket = new HashMap<>();

    @Nullable private SnapshotReader currentSnapshotReader;
    @Nullable private SnapshotSplit currentSnapshotSplit;
    private @Nullable Iterator<LogScanner> currentLogScannerIterator;

    public FlinkSourceSplitReader(
            Configuration flussConf, FlinkSourceReaderMetrics flinkSourceReaderMetrics) {
        this.flinkMetricRegistry =
                new FlinkMetricRegistry(flinkSourceReaderMetrics.getSourceReaderMetricGroup());
        this.flinkSourceReaderMetrics = flinkSourceReaderMetrics;
        this.connection = ConnectionFactory.createConnection(flussConf, flinkMetricRegistry);
    }

    @Override
    public RecordsWithSplitIds<MultiplexCdcRecordAndPos> fetch() {
        checkSnapshotSplitOrStartNext();
        if (currentSnapshotReader != null) {
            return fetchSnapshotRecords();
        } else {
            return fetchLogRecords();
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<SourceSplitBase> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        for (SourceSplitBase sourceSplitBase : splitsChange.splits()) {
            LOG.info("add split {}", sourceSplitBase.splitId());

            // may need to get fluss's table
            long tableId = sourceSplitBase.getTableBucket().getTableId();
            TablePath tablePath = sourceSplitBase.getTablePath();
            tablesPathByTableId.computeIfAbsent(tableId, id -> tablePath);
            Table table =
                    tablesByTableId.computeIfAbsent(tableId, id -> connection.getTable(tablePath));
            rowConvertByTableId.computeIfAbsent(
                    tableId,
                    id ->
                            new FlussRowToFlinkRowConverter(
                                    table.getDescriptor().getSchema().toRowType()));

            if (sourceSplitBase instanceof HybridSnapshotLogSplit) {
                // snapshot part
                HybridSnapshotLogSplit hybridSnapshotLogSplit =
                        (HybridSnapshotLogSplit) sourceSplitBase;
                if (!hybridSnapshotLogSplit.isSnapshotFinished()) {
                    Queue<SnapshotSplit> snapshotSplits =
                            tableSnapshotSplits.computeIfAbsent(
                                    sourceSplitBase.getTableBucket().getTableId(),
                                    k -> new java.util.LinkedList<>());
                    snapshotSplits.add(hybridSnapshotLogSplit);
                }
                // for log part, may need to subscribe it
                subscribeLog(sourceSplitBase, hybridSnapshotLogSplit.getLogStartingOffset());
            } else if (sourceSplitBase instanceof LogSplit) {
                LogSplit logSplit = (LogSplit) sourceSplitBase;
                subscribeLog(logSplit, logSplit.getStartingOffset());
            } else {
                throw new UnsupportedOperationException(
                        "unsupported split type: " + sourceSplitBase);
            }
        }
    }

    private void subscribeLog(SourceSplitBase split, long startingOffset) {
        TableBucket tableBucket = split.getTableBucket();
        long tableId = tableBucket.getTableId();
        TablePath tablePath = split.getTablePath();
        LogScanner logScanner = logScannersByTableId.get(tableId);
        if (logScanner == null) {
            logScanner = connection.getTable(tablePath).newScan().createLogScanner();
            logScannersByTableId.put(tableId, logScanner);
            resetCurrentLogScannerIterator();
        }

        // assign bucket offset dynamically
        int bucket = tableBucket.getBucket();
        if (tableBucket.getPartitionId() == null) {
            logScanner.subscribe(bucket, startingOffset);
        } else {
            logScanner.subscribe(
                    checkNotNull(tableBucket.getPartitionId(), "partition id must be not null"),
                    bucket,
                    startingOffset);
        }
        LOG.info("Subscribe to read log for split {} from offset {}.", split, startingOffset);
        // track the split id of the table bucket
        splitIdByTableBucket.put(tableBucket, split.splitId());
        // Track the new bucket in metrics
        flinkSourceReaderMetrics.registerTableBucket(tableBucket);
        subscribedBucketsByTableId.computeIfAbsent(tableId, k -> new HashSet<>()).add(tableBucket);
    }

    private RecordsWithSplitIds<MultiplexCdcRecordAndPos> fetchSnapshotRecords() {
        CloseableIterator<MultiplexCdcRecordAndPos> recordIterator =
                currentSnapshotReader.readBatch();
        if (recordIterator == null) {
            LOG.info("snapshot phase for split {} is finished", currentSnapshotSplit.splitId());
            return finishCurrentSnapshotSplit();
        } else {
            return forSnapshotRecords(currentSnapshotSplit, recordIterator);
        }
    }

    private RecordsWithSplitIds<MultiplexCdcRecordAndPos> fetchLogRecords() {
        if (currentLogScannerIterator == null) {
            return forLogRecords(ScanRecords.EMPTY);
        }

        if (currentLogScannerIterator.hasNext()) {
            LogScanner logScanner = currentLogScannerIterator.next();
            return forLogRecords(logScanner.poll(POLL_TIMEOUT));
        } else {
            // maybe has moved to the end of the iterator, need to reset the iterator
            // to move to the start of the iterator
            resetCurrentLogScannerIterator();
            return fetchLogRecords();
        }
    }

    private void checkSnapshotSplitOrStartNext() {
        if (currentSnapshotReader != null) {
            return;
        }

        if (tableSnapshotSplits.isEmpty()) {
            return;
        }

        // pick one table to scan
        Iterator<Long> tableIdIterator = tableSnapshotSplits.keySet().iterator();
        if (tableIdIterator.hasNext()) {
            // get the table id
            Long tableId = tableIdIterator.next();

            // get snapshot splits to read
            Queue<SnapshotSplit> snapshotSplits = tableSnapshotSplits.get(tableId);
            // get next snapshot split
            SnapshotSplit nextSplit = snapshotSplits.poll();
            if (nextSplit == null) {
                // no any remain snapshot splits for the table, remove it
                tableIdIterator.remove();
            } else {
                // start to scan the split
                Table table = tablesByTableId.get(tableId);
                // start to read next snapshot split
                currentSnapshotSplit = nextSplit;
                BatchScanner snapshotScanner =
                        table.newScan()
                                .createBatchScanner(
                                        nextSplit.getTableBucket(), nextSplit.getSnapshotId());
                currentSnapshotReader =
                        new SnapshotReader(
                                currentSnapshotSplit.getTablePath(),
                                currentSnapshotSplit.getTableBucket(),
                                snapshotScanner,
                                rowConvertByTableId.get(tableId),
                                nextSplit.recordsToSkip());
            }
        }
    }

    private FlinkRecordsWithSplitIds forLogRecords(ScanRecords scanRecords) {
        // For calculating the currentFetchEventTimeLag
        long fetchTimestamp = System.currentTimeMillis();
        long maxConsumerRecordTimestampInFetch = -1;

        Map<String, CloseableIterator<MultiplexCdcRecordAndPos>> splitRecords = new HashMap<>();
        List<TableBucket> tableScanBuckets = new ArrayList<>(scanRecords.buckets().size());
        Map<TableBucket, String> currentSplitIdByTableBucket = new HashMap<>();
        for (TableBucket scanBucket : scanRecords.buckets()) {
            tableScanBuckets.add(scanBucket);
            long tableId = scanBucket.getTableId();

            // the table bucket should already be unsubscribed
            if (!tablesPathByTableId.containsKey(tableId)
                    || !splitIdByTableBucket.containsKey(scanBucket)) {
                continue;
            }

            currentSplitIdByTableBucket.put(scanBucket, splitIdByTableBucket.get(scanBucket));

            List<ScanRecord> bucketScanRecords = scanRecords.records(scanBucket);
            if (!bucketScanRecords.isEmpty()) {
                final ScanRecord lastRecord = bucketScanRecords.get(bucketScanRecords.size() - 1);
                // We keep the maximum message timestamp in the fetch for calculating lags
                maxConsumerRecordTimestampInFetch =
                        Math.max(maxConsumerRecordTimestampInFetch, lastRecord.timestamp());
            }

            splitRecords.put(
                    splitIdByTableBucket.get(scanBucket),
                    toRecordAndPos(
                            tablesPathByTableId.get(tableId),
                            scanBucket,
                            bucketScanRecords.iterator()));
        }
        Iterator<TableBucket> buckets = tableScanBuckets.iterator();
        Iterator<String> splitIterator =
                new Iterator<String>() {

                    @Override
                    public boolean hasNext() {
                        return buckets.hasNext();
                    }

                    @Override
                    public String next() {
                        return currentSplitIdByTableBucket.get(buckets.next());
                    }
                };

        // We use the timestamp on ScanRecord as the event time to calculate the
        // currentFetchEventTimeLag. This is not totally accurate as the event time could be
        // overridden by user's custom TimestampAssigner configured in source operator.
        if (maxConsumerRecordTimestampInFetch > 0) {
            flinkSourceReaderMetrics.reportRecordEventTime(
                    fetchTimestamp - maxConsumerRecordTimestampInFetch);
        }

        return new FlinkRecordsWithSplitIds(
                splitRecords, splitIterator, tableScanBuckets.iterator(), flinkSourceReaderMetrics);
    }

    private FlinkRecordsWithSplitIds forSnapshotRecords(
            final SnapshotSplit snapshotSplit,
            final CloseableIterator<MultiplexCdcRecordAndPos> recordsForSplit) {
        return new FlinkRecordsWithSplitIds(
                snapshotSplit.splitId(),
                snapshotSplit.getTableBucket(),
                recordsForSplit,
                flinkSourceReaderMetrics);
    }

    public FlinkRecordsWithSplitIds finishCurrentSnapshotSplit() {
        Set<String> finishedSplits =
                currentSnapshotSplit instanceof HybridSnapshotLogSplit
                        // is hybrid split, not to finish this split
                        // since it remains log to read
                        ? Collections.emptySet()
                        : Collections.singleton(currentSnapshotSplit.splitId());

        final FlinkRecordsWithSplitIds finishRecords =
                new FlinkRecordsWithSplitIds(finishedSplits, flinkSourceReaderMetrics);
        closeCurrentSnapshotSplit();
        return finishRecords;
    }

    private void closeCurrentSnapshotSplit() {
        currentSnapshotReader = null;
        currentSnapshotSplit = null;
    }

    public Set<TableBucket> removeTables(Map<Long, TablePath> removedTables) {
        // todo, may consider to close the current snapshot reader if
        // the current snapshot split is in the partition buckets

        Set<TableBucket> unsubscribedBuckets = new HashSet<>();
        for (Map.Entry<Long, TablePath> tableIdAndPath : removedTables.entrySet()) {
            long tableId = tableIdAndPath.getKey();
            TablePath tablePath = tableIdAndPath.getValue();
            tableSnapshotSplits.remove(tableId);
            LOG.info("Cancel to read snapshot splits for table {} with id {}.", tablePath, tableId);
            LogScanner logScanner = logScannersByTableId.remove(tableId);
            if (logScanner != null) {
                try {
                    logScanner.close();
                    LOG.info(
                            "Cancel to read log splits for table {} with id {}.",
                            tablePath,
                            tableId);
                } catch (Exception e) {
                    LOG.error(
                            "Failed to close log scanner for table {} with id {}.",
                            tablePath,
                            tableId,
                            e);
                }
            }
            resetCurrentLogScannerIterator();

            Table table = tablesByTableId.remove(tableId);
            if (table != null) {
                try {
                    table.close();
                } catch (Exception e) {
                    LOG.error("Failed to close table {} with id {}.", tablePath, tableId, e);
                }
            }
            Set<TableBucket> subscribedBuckets = subscribedBucketsByTableId.remove(tableId);
            if (subscribedBuckets != null) {
                unsubscribedBuckets.addAll(subscribedBuckets);
            }
            tablesPathByTableId.remove(tableId);
            rowConvertByTableId.remove(tableId);
        }
        removeBuckets(unsubscribedBuckets);
        return unsubscribedBuckets;
    }

    public Set<TableBucket> removePartitions(
            Map<Long, Map<Long, String>> removedPartitionsByTableId) {
        Set<TableBucket> unsubscribedBuckets = new HashSet<>();
        for (Map.Entry<Long, Map<Long, String>> removedPartitionsByTableIdEntry :
                removedPartitionsByTableId.entrySet()) {
            long tableId = removedPartitionsByTableIdEntry.getKey();
            Map<Long, String> removedPartitions = removedPartitionsByTableIdEntry.getValue();
            unsubscribedBuckets.addAll(removePartitions(tableId, removedPartitions));
        }
        return unsubscribedBuckets;
    }

    public Set<TableBucket> removePartitions(long tableId, Map<Long, String> removedPartitions) {
        // todo, may consider to close the current snapshot reader if
        // the current snapshot split is in the partition buckets
        Queue<SnapshotSplit> snapshotSplits = tableSnapshotSplits.get(tableId);
        if (snapshotSplits == null) {
            return Collections.emptySet();
        }
        // may remove from pending snapshot splits
        Set<TableBucket> unsubscribedTableBuckets = new HashSet<>();
        Iterator<SnapshotSplit> snapshotSplitIterator = snapshotSplits.iterator();
        while (snapshotSplitIterator.hasNext()) {
            SnapshotSplit split = snapshotSplitIterator.next();
            TableBucket tableBucket = split.getTableBucket();
            if (removedPartitions.containsKey(tableBucket.getPartitionId())) {
                snapshotSplitIterator.remove();
                unsubscribedTableBuckets.add(tableBucket);
                LOG.info(
                        "Cancel to read snapshot split {} for non-existed partition {}.",
                        split.splitId(),
                        removedPartitions.get(tableBucket.getPartitionId()));
            }
        }

        // unsubscribe from log scanner
        LogScanner logScanner = logScannersByTableId.get(tableId);
        Set<TableBucket> subscribedBuckets = subscribedBucketsByTableId.get(tableId);
        Iterator<TableBucket> subscribeTableBucketIterator = subscribedBuckets.iterator();
        while (subscribeTableBucketIterator.hasNext()) {
            TableBucket tableBucket = subscribeTableBucketIterator.next();
            if (removedPartitions.containsKey(tableBucket.getPartitionId())) {
                logScanner.unsubscribe(
                        checkNotNull(tableBucket.getPartitionId(), "partition id must be not null"),
                        tableBucket.getBucket());
                subscribeTableBucketIterator.remove();
                unsubscribedTableBuckets.add(tableBucket);
                LOG.info(
                        "Unsubscribe to read split {} for non-existed partition {}.",
                        splitIdByTableBucket.get(tableBucket),
                        removedPartitions.get(tableBucket.getPartitionId()));
            }
        }

        removeBuckets(unsubscribedTableBuckets);
        return unsubscribedTableBuckets;
    }

    private void removeBuckets(Set<TableBucket> removedBuckets) {
        splitIdByTableBucket.keySet().removeAll(removedBuckets);
        flinkSourceReaderMetrics.unRegisterTableBucket(removedBuckets);
    }

    private void resetCurrentLogScannerIterator() {
        // no any table to scan log, set the iterator to null
        if (logScannersByTableId.isEmpty()) {
            currentLogScannerIterator = null;
        } else {
            currentLogScannerIterator = logScannersByTableId.values().iterator();
        }
    }

    private CloseableIterator<MultiplexCdcRecordAndPos> toRecordAndPos(
            TablePath tablePath,
            TableBucket tableBucket,
            Iterator<ScanRecord> recordAndPosIterator) {
        return new CloseableIterator<MultiplexCdcRecordAndPos>() {

            @Override
            public boolean hasNext() {
                return recordAndPosIterator.hasNext();
            }

            @Override
            public MultiplexCdcRecordAndPos next() {
                return toMultiplexCdcRecordAndPos(
                        tablePath,
                        tableBucket,
                        rowConvertByTableId.get(tableBucket.getTableId()),
                        recordAndPosIterator.next());
            }

            @Override
            public void close() {
                // do nothing
            }
        };
    }

    @Override
    public void wakeUp() {
        // TODO: we should wakeup snapshot reader as well when it supports.
        for (LogScanner logScanner : logScannersByTableId.values()) {
            logScanner.wakeup();
        }
    }

    @Override
    public void close() throws Exception {
        if (currentSnapshotReader != null) {
            currentSnapshotReader.close();
        }
        for (LogScanner logScanner : logScannersByTableId.values()) {
            logScanner.close();
        }
        for (Table table : tablesByTableId.values()) {
            table.close();
        }
        connection.close();
        flinkMetricRegistry.close();
    }

    private MultiplexCdcRecordAndPos toMultiplexCdcRecordAndPos(
            TablePath tablePath,
            TableBucket tableBucket,
            FlussRowToFlinkRowConverter converter,
            ScanRecord scanRecord) {
        RowData rowData = converter.toFlinkRowData(scanRecord);
        return new MultiplexCdcRecordAndPos(
                tablePath,
                tableBucket,
                new CdcRecord(scanRecord.logOffset(), scanRecord.timestamp(), rowData));
    }
}
