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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.flink.lake.LakeSplitReaderGenerator;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.metrics.FlinkMetricRegistry;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SnapshotSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * An implementation of {@link SplitReader} for reading splits into {@link RecordAndPos}.
 *
 * <p>It'll first read the {@link SnapshotSplit}s if any and then switch to read {@link LogSplit}s .
 */
public class FlinkSourceSplitReader implements SplitReader<RecordAndPos, SourceSplitBase> {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(10000L);

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceSplitReader.class);
    private final RowType sourceOutputType;

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

    @Nullable private LakeSource<LakeSplit> lakeSource;

    // table id, will be null when haven't received any split
    private Long tableId;

    private final Map<TableBucket, Long> stoppingOffsets;
    private LakeSplitReaderGenerator lakeSplitReaderGenerator;

    private final Set<String> emptyLogSplits;
    // track split IDs corresponding to removed partitions
    private final Set<String> removedSplits = new HashSet<>();
    // Set to collect table buckets that are unsubscribed.
    private Set<TableBucket> unsubscribedTableBuckets = new HashSet<>();

    public FlinkSourceSplitReader(
            Configuration flussConf,
            TablePath tablePath,
            RowType sourceOutputType,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics,
            @Nullable LakeSource<LakeSplit> lakeSource) {
        this.flinkMetricRegistry =
                new FlinkMetricRegistry(flinkSourceReaderMetrics.getSourceReaderMetricGroup());
        this.connection = ConnectionFactory.createConnection(flussConf, flinkMetricRegistry);
        this.table = connection.getTable(tablePath);
        this.sourceOutputType = sourceOutputType;
        this.boundedSplits = new ArrayDeque<>();
        this.subscribedBuckets = new HashMap<>();
        this.flinkSourceReaderMetrics = flinkSourceReaderMetrics;
        this.projectedFields =
                reCalculateProjectedFields(sourceOutputType, table.getTableInfo().getRowType());
        this.logScanner = table.newScan().project(projectedFields).createLogScanner();
        this.stoppingOffsets = new HashMap<>();
        this.emptyLogSplits = new HashSet<>();
        this.lakeSource = lakeSource;
        LOG.info(
                "fluss table schema: {}, flink table output type:{}",
                table.getTableInfo().getSchema(),
                sourceOutputType);
    }

    @Override
    public RecordsWithSplitIds<RecordAndPos> fetch() throws IOException {
        if (!removedSplits.isEmpty()) {
            FlinkRecordsWithSplitIds records =
                    new FlinkRecordsWithSplitIds(
                            new HashSet<>(removedSplits), flinkSourceReaderMetrics);
            removedSplits.clear();
            return records;
        }
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
                if (sourceSplitBase instanceof LakeSnapshotAndFlussLogSplit) {
                    LakeSnapshotAndFlussLogSplit lakeSnapshotAndFlussLogSplit =
                            (LakeSnapshotAndFlussLogSplit) sourceSplitBase;
                    if (lakeSnapshotAndFlussLogSplit.isStreaming()) {
                        // is streaming split which has no stopping offset, we need also
                        // subscribe
                        // change log
                        subscribeLog(
                                lakeSnapshotAndFlussLogSplit,
                                lakeSnapshotAndFlussLogSplit.getStartingOffset());
                    }
                }
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
                    new LakeSplitReaderGenerator(table, projectedFields, checkNotNull(lakeSource));
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
                // Try to subscribe using the partition id.
                try {
                    logScanner.subscribe(partitionId, bucket, startingOffset);
                } catch (Exception e) {
                    // the PartitionNotExistException may still happens when partition is removed
                    // but Flink source reader failover before aware of it
                    // Traverse the exception chain to check for PartitionNotExistException.
                    boolean partitionNotExist =
                            ExceptionUtils.findThrowable(e, PartitionNotExistException.class)
                                    .isPresent();
                    if (partitionNotExist) {
                        // mark the not exist partition to be removed
                        removedSplits.add(split.splitId());
                        // mark the table bucket to be unsubscribed
                        unsubscribedTableBuckets.add(tableBucket);
                        LOG.warn(
                                "Partition {} does not exist when subscribing to log for split {}. Skipping subscription.",
                                partitionId,
                                split.splitId());
                        return;
                    }
                }
            } else {
                // If no partition id, subscribe by bucket only.
                logScanner.subscribe(bucket, startingOffset);
            }

            LOG.info(
                    "Subscribe to read log for split {} from offset {}.",
                    split.splitId(),
                    startingOffset);
            // Track the new bucket in metrics and internal state.
            flinkSourceReaderMetrics.registerTableBucket(tableBucket);
            subscribedBuckets.put(tableBucket, split.splitId());
        }
    }

    public Set<TableBucket> removePartitions(Map<Long, String> removedPartitions) {
        // First, if the current active bounded split belongs to a removed partition,
        // finish it so it will not be restored.
        if (currentBoundedSplit != null) {
            TableBucket currentBucket = currentBoundedSplit.getTableBucket();
            if (removedPartitions.containsKey(currentBucket.getPartitionId())) {
                try {
                    // Mark the current split as finished.
                    removedSplits.add(currentBoundedSplit.splitId());
                    closeCurrentBoundedSplit();
                    unsubscribedTableBuckets.add(currentBucket);
                    LOG.info(
                            "Mark current bounded split {} as finished for removed partition {}.",
                            currentBucket,
                            removedPartitions.get(currentBucket.getPartitionId()));
                } catch (IOException e) {
                    LOG.warn(
                            "Failed to close current bounded split for removed partition {}.",
                            removedPartitions.get(currentBucket.getPartitionId()),
                            e);
                }
            }
        }

        // Remove pending snapshot splits whose table buckets belong to removed partitions.
        Iterator<SourceSplitBase> snapshotSplitIterator = boundedSplits.iterator();
        while (snapshotSplitIterator.hasNext()) {
            SourceSplitBase split = snapshotSplitIterator.next();
            TableBucket tableBucket = split.getTableBucket();
            if (removedPartitions.containsKey(tableBucket.getPartitionId())) {
                removedSplits.add(split.splitId());
                snapshotSplitIterator.remove();
                unsubscribedTableBuckets.add(tableBucket);
                LOG.info(
                        "Cancel reading snapshot split {} for removed partition {}.",
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
                removedSplits.add(tableBucketAndSplit.getValue());
                subscribeTableBucketIterator.remove();
                unsubscribedTableBuckets.add(tableBucket);
                LOG.info(
                        "Unsubscribe to read log of split {} for non-existed partition {}.",
                        tableBucketAndSplit.getValue(),
                        removedPartitions.get(tableBucket.getPartitionId()));
            }
        }

        Set<TableBucket> currentUnsubscribedTableBuckets = this.unsubscribedTableBuckets;
        this.unsubscribedTableBuckets = new HashSet<>();
        return currentUnsubscribedTableBuckets;
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

    private FlinkRecordsWithSplitIds finishCurrentBoundedSplit() throws IOException {
        Set<String> finishedSplits =
                currentBoundedSplit instanceof HybridSnapshotLogSplit
                                || (currentBoundedSplit instanceof LakeSnapshotAndFlussLogSplit
                                        && ((LakeSnapshotAndFlussLogSplit) currentBoundedSplit)
                                                .isStreaming())
                        // is hybrid split, or is lakeAndFlussLog split in streaming mode,
                        // not to finish this split
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

    /**
     * The projected fields for the fluss table from the source output types. Mapping based on
     * column name rather thn column id.
     */
    private static int[] reCalculateProjectedFields(
            RowType sourceOutputType, RowType flussRowType) {
        if (sourceOutputType.copy(false).equals(flussRowType.copy(false))) {
            return null;
        }

        List<String> fieldNames = sourceOutputType.getFieldNames();
        int[] projectedFlussFields = new int[fieldNames.size()];
        for (int i = 0; i < fieldNames.size(); i++) {
            int fieldIndex = flussRowType.getFieldIndex(fieldNames.get(i));
            if (fieldIndex == -1) {
                throw new ValidationException(
                        String.format(
                                "The field %s is not found in the fluss table.",
                                fieldNames.get(i)));
            }
            projectedFlussFields[i] = fieldIndex;
        }
        return projectedFlussFields;
    }

    @VisibleForTesting
    public int[] getProjectedFields() {
        return projectedFields;
    }
}
