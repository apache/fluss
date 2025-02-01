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

package com.alibaba.fluss.connector.flink.source.reader;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.batch.BatchScanner;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.lakehouse.LakeSplitReaderGenerator;
import com.alibaba.fluss.connector.flink.metrics.FlinkMetricRegistry;
import com.alibaba.fluss.connector.flink.source.metrics.FlinkSourceReaderMetrics;
import com.alibaba.fluss.connector.flink.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.connector.flink.source.split.LogSplit;
import com.alibaba.fluss.connector.flink.source.split.SnapshotSplit;
import com.alibaba.fluss.connector.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * An implementation of {@link SplitReader} for reading splits into {@link RecordAndPos}.
 *
 * <p>It'll first read the {@link SnapshotSplit}s if any and then switch to read {@link LogSplit}s .
 */
public class FlinkSourceSplitReader implements SplitReader<RecordAndPos, SourceSplitBase> {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(10000L);

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceSplitReader.class);
    private final RowType sourceOutputType;
    private final TablePath tablePath;

    // boundedSplits, kv snapshot split or lake snapshot split
    private final Queue<SourceSplitBase> boundedSplits;

    // map from subscribed table bucket to split id
    private final Map<TableBucket, String> subscribedBuckets;

    @Nullable private final int[] projectedFields;
    private final FlinkSourceReaderMetrics flinkSourceReaderMetrics;

    @Nullable private BoundedSplitReader currentBoundedSplitReader;
    @Nullable private SourceSplitBase currentBoundedSplit;

    private final LogScanner logScanner;

    private final Connection connection;
    private final Table table;
    private final FlinkMetricRegistry flinkMetricRegistry;

    // table id, will be null when haven't received any split
    private Long tableId;

    private final Map<TableBucket, Long> stoppingOffsets;
    private LakeSplitReaderGenerator lakeSplitReaderGenerator;

    private final Set<String> emptyLogSplits;

    public FlinkSourceSplitReader(
            Configuration flussConf,
            TablePath tablePath,
            RowType sourceOutputType,
            @Nullable int[] projectedFields,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics) {
        this.flinkMetricRegistry =
                new FlinkMetricRegistry(flinkSourceReaderMetrics.getSourceReaderMetricGroup());
        this.connection = ConnectionFactory.createConnection(flussConf, flinkMetricRegistry);
        this.table = connection.getTable(tablePath);
        this.sourceOutputType = sourceOutputType;
        this.tablePath = tablePath;
        this.boundedSplits = new ArrayDeque<>();
        this.subscribedBuckets = new HashMap<>();
        this.projectedFields = projectedFields;
        this.flinkSourceReaderMetrics = flinkSourceReaderMetrics;
        sanityCheck(table.getDescriptor().getSchema().toRowType(), projectedFields);
        this.logScanner = table.newScan().project(projectedFields).createLogScanner();
        this.stoppingOffsets = new HashMap<>();
        this.emptyLogSplits = new HashSet<>();
    }

    @Override
    public RecordsWithSplitIds<RecordAndPos> fetch() throws IOException {
        checkSnapshotSplitOrStartNext();
        if (currentBoundedSplitReader != null) {
            CloseableIterator<RecordAndPos> recordIterator = currentBoundedSplitReader.readBatch();
            if (recordIterator == null) {
                LOG.info("split {} is finished", currentBoundedSplit.splitId());
                return finishCurrentBoundedSplit();
            } else {
                return forBoundedSplitRecords(currentBoundedSplit, recordIterator);
            }
        } else {
            // may need to finish empty log splits
            if (!emptyLogSplits.isEmpty()) {
                FlinkRecordsWithSplitIds records =
                        new FlinkRecordsWithSplitIds(
                                new HashSet<>(emptyLogSplits), flinkSourceReaderMetrics);
                emptyLogSplits.clear();
                return records;
            } else {
                // if not subscribe any buckets, just return empty records
                if (subscribedBuckets.isEmpty()) {
                    return FlinkRecordsWithSplitIds.emptyRecords(flinkSourceReaderMetrics);
                }
                ScanRecords scanRecords = logScanner.poll(POLL_TIMEOUT);
                return forLogRecords(scanRecords);
            }
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<SourceSplitBase> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }
        for (SourceSplitBase sourceSplitBase : splitsChanges.splits()) {
            LOG.info("add split {}", sourceSplitBase.splitId());
            // init table id
            if (tableId == null) {
                tableId = sourceSplitBase.getTableBucket().getTableId();
            } else {
                checkArgument(
                        tableId.equals(sourceSplitBase.getTableBucket().getTableId()),
                        "table id not equal across splits {}",
                        splitsChanges.splits());
            }

            if (sourceSplitBase.isHybridSnapshotLogSplit()) {
                HybridSnapshotLogSplit hybridSnapshotLogSplit =
                        sourceSplitBase.asHybridSnapshotLogSplit();

                // if snapshot is not finished, add to pending snapshot splits
                if (!hybridSnapshotLogSplit.isSnapshotFinished()) {
                    boundedSplits.add(sourceSplitBase);
                }
                // still need to subscribe log
                subscribeLog(sourceSplitBase, hybridSnapshotLogSplit.getLogStartingOffset());
            } else if (sourceSplitBase.isLogSplit()) {
                subscribeLog(sourceSplitBase, sourceSplitBase.asLogSplit().getStartingOffset());
            } else if (sourceSplitBase.isLakeSplit()) {
                getLakeSplitReader().addSplit(sourceSplitBase, boundedSplits);
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "The split type of %s is not supported.",
                                sourceSplitBase.getClass()));
            }
        }
    }

    private LakeSplitReaderGenerator getLakeSplitReader() {
        if (lakeSplitReaderGenerator == null) {
            lakeSplitReaderGenerator =
                    new LakeSplitReaderGenerator(table, connection, tablePath, projectedFields);
        }
        return lakeSplitReaderGenerator;
    }

    private void subscribeLog(SourceSplitBase split, long startingOffset) {
        // assign bucket offset dynamically
        TableBucket tableBucket = split.getTableBucket();
        boolean isEmptyLogSplit = false;
        if (split instanceof LogSplit) {
            LogSplit logSplit = split.asLogSplit();
            Optional<Long> stoppingOffsetOpt = logSplit.getStoppingOffset();
            if (stoppingOffsetOpt.isPresent()) {
                Long stoppingOffset = stoppingOffsetOpt.get();
                if (startingOffset >= stoppingOffset) {
                    // is empty log splits as no log record can be fetched
                    emptyLogSplits.add(split.splitId());
                    isEmptyLogSplit = true;
                } else if (stoppingOffset >= 0) {
                    stoppingOffsets.put(tableBucket, stoppingOffset);
                } else {
                    // This should not happen.
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Invalid stopping offset %d for bucket %s",
                                    stoppingOffset, tableBucket));
                }
            }
        }

        if (isEmptyLogSplit) {
            LOG.info(
                    "Skip to read log for split {} since the split is empty with starting offset {}, stopping offset {}.",
                    split.splitId(),
                    startingOffset,
                    split.asLogSplit().getStoppingOffset().get());
        } else {
            Long partitionId = tableBucket.getPartitionId();
            int bucket = tableBucket.getBucket();
            if (partitionId != null) {
                logScanner.subscribe(partitionId, bucket, startingOffset);
            } else {
                logScanner.subscribe(bucket, startingOffset);
            }

            LOG.info(
                    "Subscribe to read log for split {} from offset {}.",
                    split.splitId(),
                    startingOffset);

            // Track the new bucket in metrics
            flinkSourceReaderMetrics.registerTableBucket(tableBucket);
            subscribedBuckets.put(tableBucket, split.splitId());
        }
    }

    public Set<TableBucket> removePartitions(Map<Long, String> removedPartitions) {
        // todo, may consider to close the current snapshot reader if
        // the current snapshot split is in the partition buckets

        // may remove from pending snapshot splits
        Set<TableBucket> unsubscribedTableBuckets = new HashSet<>();
        Iterator<SourceSplitBase> snapshotSplitIterator = boundedSplits.iterator();
        while (snapshotSplitIterator.hasNext()) {
            SourceSplitBase split = snapshotSplitIterator.next();
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
        Iterator<Map.Entry<TableBucket, String>> subscribeTableBucketIterator =
                subscribedBuckets.entrySet().iterator();
        while (subscribeTableBucketIterator.hasNext()) {
            Map.Entry<TableBucket, String> tableBucketAndSplit =
                    subscribeTableBucketIterator.next();
            TableBucket tableBucket = tableBucketAndSplit.getKey();
            if (removedPartitions.containsKey(tableBucket.getPartitionId())) {
                logScanner.unsubscribe(
                        checkNotNull(tableBucket.getPartitionId(), "partition id must be not null"),
                        tableBucket.getBucket());
                subscribeTableBucketIterator.remove();
                unsubscribedTableBuckets.add(tableBucket);
                LOG.info(
                        "Unsubscribe to read log of split {} for non-existed partition {}.",
                        tableBucketAndSplit.getValue(),
                        removedPartitions.get(tableBucket.getPartitionId()));
            }
        }

        return unsubscribedTableBuckets;
    }

    private void checkSnapshotSplitOrStartNext() {
        if (currentBoundedSplitReader != null) {
            return;
        }

        SourceSplitBase nextSplit = boundedSplits.poll();
        if (nextSplit == null) {
            return;
        }

        // start to read next snapshot split
        currentBoundedSplit = nextSplit;
        if (currentBoundedSplit.isHybridSnapshotLogSplit()) {
            SnapshotSplit snapshotSplit = currentBoundedSplit.asHybridSnapshotLogSplit();
            BatchScanner batchScanner =
                    table.newScan()
                            .project(projectedFields)
                            .createBatchScanner(
                                    snapshotSplit.getTableBucket(), snapshotSplit.getSnapshotId());
            currentBoundedSplitReader =
                    new BoundedSplitReader(batchScanner, snapshotSplit.recordsToSkip());
        } else if (currentBoundedSplit.isLakeSplit()) {
            currentBoundedSplitReader =
                    getLakeSplitReader().getBoundedSplitScanner(currentBoundedSplit);
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "The split type of %s is not supported.",
                            currentBoundedSplit.getClass()));
        }
    }

    private FlinkRecordsWithSplitIds forLogRecords(ScanRecords scanRecords) {
        // For calculating the currentFetchEventTimeLag
        long fetchTimestamp = System.currentTimeMillis();
        long maxConsumerRecordTimestampInFetch = -1;

        Map<String, CloseableIterator<RecordAndPos>> splitRecords = new HashMap<>();
        Map<TableBucket, Long> stoppingOffsets = new HashMap<>();
        Set<String> finishedSplits = new HashSet<>();
        Map<TableBucket, String> splitIdByTableBucket = new HashMap<>();
        List<TableBucket> tableScanBuckets = new ArrayList<>(scanRecords.buckets().size());
        for (TableBucket scanBucket : scanRecords.buckets()) {
            long stoppingOffset = getStoppingOffset(scanBucket);
            String splitId = subscribedBuckets.get(scanBucket);
            // can't find the split id for the bucket, the bucket should be unsubscribed
            if (splitId == null) {
                continue;
            }
            splitIdByTableBucket.put(scanBucket, splitId);
            tableScanBuckets.add(scanBucket);
            List<ScanRecord> bucketScanRecords = scanRecords.records(scanBucket);
            if (!bucketScanRecords.isEmpty()) {
                final ScanRecord lastRecord = bucketScanRecords.get(bucketScanRecords.size() - 1);
                // We keep the maximum message timestamp in the fetch for calculating lags
                maxConsumerRecordTimestampInFetch =
                        Math.max(maxConsumerRecordTimestampInFetch, lastRecord.timestamp());

                // After processing a record with offset of "stoppingOffset - 1", the split reader
                // should not continue fetching because the record with stoppingOffset may not
                // exist. Keep polling will just block forever
                if (lastRecord.logOffset() >= stoppingOffset - 1) {
                    stoppingOffsets.put(scanBucket, stoppingOffset);
                    finishedSplits.add(splitId);
                }
            }
            splitRecords.put(splitId, toRecordAndPos(bucketScanRecords.iterator()));
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
                        return splitIdByTableBucket.get(buckets.next());
                    }
                };

        // We use the timestamp on ScanRecord as the event time to calculate the
        // currentFetchEventTimeLag. This is not totally accurate as the event time could be
        // overridden by user's custom TimestampAssigner configured in source operator.
        if (maxConsumerRecordTimestampInFetch > 0) {
            flinkSourceReaderMetrics.reportRecordEventTime(
                    fetchTimestamp - maxConsumerRecordTimestampInFetch);
        }

        FlinkRecordsWithSplitIds recordsWithSplitIds =
                new FlinkRecordsWithSplitIds(
                        splitRecords,
                        splitIterator,
                        tableScanBuckets.iterator(),
                        finishedSplits,
                        flinkSourceReaderMetrics);
        stoppingOffsets.forEach(recordsWithSplitIds::setTableBucketStoppingOffset);
        return recordsWithSplitIds;
    }

    private CloseableIterator<RecordAndPos> toRecordAndPos(
            Iterator<ScanRecord> recordAndPosIterator) {
        return new CloseableIterator<RecordAndPos>() {

            @Override
            public boolean hasNext() {
                return recordAndPosIterator.hasNext();
            }

            @Override
            public RecordAndPos next() {
                return new RecordAndPos(recordAndPosIterator.next());
            }

            @Override
            public void close() {
                // do nothing
            }
        };
    }

    private FlinkRecordsWithSplitIds forBoundedSplitRecords(
            final SourceSplitBase snapshotSplit,
            final CloseableIterator<RecordAndPos> recordsForSplit) {
        return new FlinkRecordsWithSplitIds(
                snapshotSplit.splitId(),
                snapshotSplit.getTableBucket(),
                recordsForSplit,
                flinkSourceReaderMetrics);
    }

    private long getStoppingOffset(TableBucket tableBucket) {
        return stoppingOffsets.getOrDefault(tableBucket, Long.MAX_VALUE);
    }

    public FlinkRecordsWithSplitIds finishCurrentBoundedSplit() throws IOException {
        Set<String> finishedSplits =
                currentBoundedSplit instanceof HybridSnapshotLogSplit
                        // is hybrid split, not to finish this split
                        // since it remains log to read
                        ? Collections.emptySet()
                        : Collections.singleton(currentBoundedSplit.splitId());
        final FlinkRecordsWithSplitIds finishRecords =
                new FlinkRecordsWithSplitIds(finishedSplits, flinkSourceReaderMetrics);
        closeCurrentBoundedSplit();
        return finishRecords;
    }

    private void closeCurrentBoundedSplit() throws IOException {
        try {
            currentBoundedSplitReader.close();
        } catch (Exception e) {
            throw new IOException("Fail to close current snapshot split.", e);
        }
        currentBoundedSplitReader = null;
        currentBoundedSplit = null;
    }

    @Override
    public void wakeUp() {
        // TODO: we should wakeup snapshot reader as well when it supports.
        if (logScanner != null) {
            logScanner.wakeup();
        }
    }

    @Override
    public void close() throws Exception {
        if (currentBoundedSplitReader != null) {
            currentBoundedSplitReader.close();
        }
        if (logScanner != null) {
            logScanner.close();
        }
        table.close();
        connection.close();
        flinkMetricRegistry.close();
    }

    private void sanityCheck(RowType flussTableRowType, @Nullable int[] projectedFields) {
        RowType tableRowType =
                projectedFields != null
                        ? flussTableRowType.project(projectedFields)
                        : flussTableRowType;
        if (!sourceOutputType.copy(false).equals(tableRowType.copy(false))) {
            // The default nullability of Flink row type and Fluss row type might be not the same,
            // thus we need to compare the row type without nullability here.

            final String flussSchemaMsg;
            if (projectedFields == null) {
                flussSchemaMsg = "\nFluss table schema: " + tableRowType;
            } else {
                flussSchemaMsg =
                        "\nFluss table schema: "
                                + tableRowType
                                + " (projection "
                                + Arrays.toString(projectedFields)
                                + ")";
            }
            // Throw exception if the schema is the not same, this should rarely happen because we
            // only allow fluss tables derived from fluss catalog. But this can happen if an ALTER
            // TABLE command executed on the fluss table, after the job is submitted but before the
            // SinkFunction is opened.
            throw new ValidationException(
                    "The Flink query schema is not matched to Fluss table schema. "
                            + "\nFlink query schema: "
                            + sourceOutputType
                            + flussSchemaMsg);
        }
    }
}
