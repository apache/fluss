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

import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.source.deserializer.DeserializerInitContextImpl;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.event.FinishedKvSnapshotConsumeEvent;
import org.apache.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import org.apache.fluss.flink.source.event.PartitionsRemovedEvent;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkSourceReader}. */
class FlinkSourceReaderTest extends FlinkTestBase {

    @Test
    void testCheckpointHybridSnapshotFinishedBeforeFirstLogRecord() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_checkpoint_hybrid_snapshot_finished");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DEFAULT_PK_TABLE_SCHEMA)
                        .distributedBy(1, "id")
                        .build();
        long tableId = createTable(tablePath, tableDescriptor);

        UpsertResult seedResult = upsert(tablePath, row(1, "snapshot"));
        TableBucket tableBucket = new TableBucket(tableId, 0);
        assertThat(seedResult.getBucket()).isEqualTo(tableBucket);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        KvSnapshots snapshots = admin.getLatestKvSnapshots(tablePath).get();
        long snapshotId = snapshots.getSnapshotId(0).getAsLong();
        long logStartingOffset = snapshots.getLogOffset(0).getAsLong();
        assertThat(logStartingOffset).isEqualTo(seedResult.getLogEndOffset());
        HybridSnapshotLogSplit split =
                new HybridSnapshotLogSplit(
                        tableBucket,
                        null,
                        snapshotId,
                        0,
                        false,
                        logStartingOffset,
                        LogSplit.NO_STOPPING_OFFSET,
                        false);

        TestingReaderContext readerContext = new TestingReaderContext();
        TestingReaderOutput<RowData> snapshotOutput = new TestingReaderOutput<>();
        HybridSnapshotLogSplit checkpointSplit;
        try (FlinkSourceReader<RowData> reader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        null)) {
            reader.addSplits(Collections.singletonList(split));

            // Poll until the snapshot EOF marker reaches the split state. There is no incremental
            // record yet, so this checkpoint must preserve the snapshot-to-log boundary itself.
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(snapshotOutput);
                        List<SourceSplitBase> checkpoint = reader.snapshotState(1L);
                        assertThat(checkpoint).hasSize(1);
                        assertThat(checkpoint.get(0)).isInstanceOf(HybridSnapshotLogSplit.class);
                        assertThat(
                                        ((HybridSnapshotLogSplit) checkpoint.get(0))
                                                .isSnapshotFinished())
                                .isTrue();
                    });

            List<SourceSplitBase> checkpoint = reader.snapshotState(2L);
            assertThat(checkpoint).hasSize(1);
            checkpointSplit = (HybridSnapshotLogSplit) checkpoint.get(0);
        }

        assertThat(checkpointSplit.isSnapshotFinished()).isTrue();
        assertThat(checkpointSplit.getSnapshotId()).isEqualTo(snapshotId);
        assertThat(checkpointSplit.getLogStartingOffset()).isEqualTo(logStartingOffset);
        assertThat(checkpointSplit.recordsToSkip()).isEqualTo(1L);
        assertThat(snapshotOutput.getEmittedRecords()).hasSize(1);
        assertThat(snapshotOutput.getEmittedRecords().get(0).getInt(0)).isEqualTo(1);
        assertThat(snapshotOutput.getEmittedRecords().get(0).getString(1).toString())
                .isEqualTo("snapshot");
        assertThat(readerContext.getSentEvents())
                .containsExactly(
                        new FinishedKvSnapshotConsumeEvent(1L, Collections.singleton(tableBucket)));

        // Restore with an invalid snapshot ID to model a snapshot released after the checkpoint.
        // Since the checkpoint marks the snapshot complete, recovery must subscribe only to log.
        UpsertResult newLogResult = upsert(tablePath, row(2, "new-log"));
        assertThat(newLogResult.getBucket()).isEqualTo(tableBucket);
        HybridSnapshotLogSplit restoringSplit =
                new HybridSnapshotLogSplit(
                        tableBucket,
                        null,
                        Long.MAX_VALUE,
                        checkpointSplit.recordsToSkip(),
                        checkpointSplit.isSnapshotFinished(),
                        checkpointSplit.getLogStartingOffset(),
                        checkpointSplit.getLogStoppingOffset().orElse(LogSplit.NO_STOPPING_OFFSET),
                        checkpointSplit.isBatch());
        TestingReaderOutput<RowData> restoredOutput = new TestingReaderOutput<>();
        try (FlinkSourceReader<RowData> restoredReader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        new TestingReaderContext(),
                        null)) {
            restoredReader.addSplits(Collections.singletonList(restoringSplit));

            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        restoredReader.pollNext(restoredOutput);
                        assertThat(restoredOutput.getEmittedRecords()).hasSize(1);
                    });

            assertThat(restoredOutput.getEmittedRecords()).hasSize(1);
            assertThat(restoredOutput.getEmittedRecords().get(0).getInt(0)).isEqualTo(2);
            assertThat(restoredOutput.getEmittedRecords().get(0).getString(1).toString())
                    .isEqualTo("new-log");

            List<SourceSplitBase> restoredCheckpoint = restoredReader.snapshotState(2L);
            assertThat(restoredCheckpoint).hasSize(1);
            HybridSnapshotLogSplit restoredCheckpointSplit =
                    (HybridSnapshotLogSplit) restoredCheckpoint.get(0);
            assertThat(restoredCheckpointSplit.isSnapshotFinished()).isTrue();
            assertThat(restoredCheckpointSplit.getSnapshotId()).isEqualTo(Long.MAX_VALUE);
            assertThat(restoredCheckpointSplit.recordsToSkip()).isEqualTo(1L);
            assertThat(restoredCheckpointSplit.getLogStartingOffset())
                    .isEqualTo(newLogResult.getLogEndOffset());
        }
    }

    @Test
    void testCheckpointLakeSplitFinishedBeforeFirstLogRecord() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_checkpoint_lake_split_finished");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DEFAULT_PK_TABLE_SCHEMA)
                        .distributedBy(1, "id")
                        .build();
        long tableId = createTable(tablePath, tableDescriptor);

        UpsertResult seedResult = upsert(tablePath, row(1, "lake"));
        TableBucket tableBucket = new TableBucket(tableId, 0);
        assertThat(seedResult.getBucket()).isEqualTo(tableBucket);
        long startingOffset = seedResult.getLogEndOffset();

        // Mirror the seed row in the lake snapshot. Since the log starts at its LEO, this row must
        // be emitted only from the lake.
        TrackingLakeSource lakeSource =
                new TrackingLakeSource(
                        Collections.singletonList(
                                new GenericRecord(-1L, -1L, ChangeType.INSERT, row(1, "lake"))));
        LakeSnapshotAndFlussLogSplit split =
                new LakeSnapshotAndFlussLogSplit(
                        tableBucket,
                        null,
                        Collections.singletonList(new TestingLakeSplit(0, Collections.emptyList())),
                        startingOffset,
                        LogSplit.NO_STOPPING_OFFSET);

        TestingReaderOutput<RowData> lakeOutput = new TestingReaderOutput<>();
        LakeSnapshotAndFlussLogSplit checkpointSplit;
        try (FlinkSourceReader<RowData> reader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        new TestingReaderContext(),
                        lakeSource)) {
            reader.addSplits(Collections.singletonList(split));

            // Poll until the lake EOF marker reaches the split state. No incremental record exists
            // yet, so the checkpoint captures the lake-to-log boundary.
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(lakeOutput);
                        List<SourceSplitBase> checkpoint = reader.snapshotState(1L);
                        assertThat(checkpoint).hasSize(1);
                        assertThat(checkpoint.get(0))
                                .isInstanceOf(LakeSnapshotAndFlussLogSplit.class);
                        assertThat(
                                        ((LakeSnapshotAndFlussLogSplit) checkpoint.get(0))
                                                .isLakeSplitFinished())
                                .isTrue();
                    });

            List<SourceSplitBase> checkpoint = reader.snapshotState(2L);
            assertThat(checkpoint).hasSize(1);
            checkpointSplit = (LakeSnapshotAndFlussLogSplit) checkpoint.get(0);
            assertThat(checkpointSplit.isLakeSplitFinished()).isTrue();
            assertThat(checkpointSplit.getStartingOffset()).isEqualTo(startingOffset);
            assertThat(checkpointSplit.getRecordsToSkip()).isEqualTo(1L);
            assertThat(checkpointSplit.getCurrentLakeSplitIndex()).isZero();
            assertThat(lakeOutput.getEmittedRecords()).hasSize(1);
            assertThat(lakeOutput.getEmittedRecords().get(0).getInt(0)).isEqualTo(1);
            assertThat(lakeOutput.getEmittedRecords().get(0).getString(1).toString())
                    .isEqualTo("lake");
        }
        assertThat(lakeSource.getCreateLakeRecordReaderCount()).isEqualTo(1);

        // Append the first incremental record after the boundary checkpoint. Recovery must read it
        // through LogScanner without reopening the finished lake RecordReader.
        UpsertResult newLogResult = upsert(tablePath, row(2, "new-log"));
        assertThat(newLogResult.getBucket()).isEqualTo(tableBucket);
        TrackingLakeSource restoringLakeSource = new TrackingLakeSource(Collections.emptyList());
        TestingReaderOutput<RowData> restoredOutput = new TestingReaderOutput<>();
        try (FlinkSourceReader<RowData> restoredReader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        new TestingReaderContext(),
                        restoringLakeSource)) {
            restoredReader.addSplits(Collections.singletonList(checkpointSplit));

            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        restoredReader.pollNext(restoredOutput);
                        assertThat(restoredOutput.getEmittedRecords()).hasSize(1);
                    });

            assertThat(restoringLakeSource.getCreateLakeRecordReaderCount()).isZero();
            assertThat(restoredOutput.getEmittedRecords().get(0).getInt(0)).isEqualTo(2);
            assertThat(restoredOutput.getEmittedRecords().get(0).getString(1).toString())
                    .isEqualTo("new-log");

            List<SourceSplitBase> restoredCheckpoint = restoredReader.snapshotState(3L);
            assertThat(restoredCheckpoint).hasSize(1);
            LakeSnapshotAndFlussLogSplit restoredCheckpointSplit =
                    (LakeSnapshotAndFlussLogSplit) restoredCheckpoint.get(0);
            assertThat(restoredCheckpointSplit.isLakeSplitFinished()).isTrue();
            assertThat(restoredCheckpointSplit.getStartingOffset())
                    .isEqualTo(newLogResult.getLogEndOffset());
            assertThat(restoredCheckpointSplit.getRecordsToSkip()).isEqualTo(1L);
            assertThat(restoredCheckpointSplit.getCurrentLakeSplitIndex()).isZero();
        }
    }

    @Test
    void testHandlePartitionsRemovedEvent() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_partitioned_table");

        TableDescriptor tableDescriptor = DEFAULT_AUTO_PARTITIONED_PK_TABLE_DESCRIPTOR;
        long tableId = createTable(tablePath, tableDescriptor);

        // wait until partitions are created
        ZooKeeperClient zooKeeperClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        Map<Long, String> partitionNameByIds = waitUntilPartitions(zooKeeperClient, tablePath);

        // now, write rows to the table
        Map<Long, List<String>> partitionWrittenRows = new HashMap<>();
        for (Map.Entry<Long, String> partitionIdAndName : partitionNameByIds.entrySet()) {
            partitionWrittenRows.put(
                    partitionIdAndName.getKey(),
                    writeRowsToPartition(
                            conn, tablePath, Collections.singleton(partitionIdAndName.getValue())));
        }

        // try to write some rows to the table
        TestingReaderContext readerContext = new TestingReaderContext();
        try (final FlinkSourceReader<RowData> reader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        null)) {

            // first of all, add all splits of all partitions to the reader
            Map<Long, Set<TableBucket>> assignedBuckets = new HashMap<>();
            for (Long partitionId : partitionNameByIds.keySet()) {
                for (int i = 0; i < DEFAULT_BUCKET_NUM; i++) {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
                    reader.addSplits(
                            Collections.singletonList(
                                    new LogSplit(
                                            tableBucket, partitionNameByIds.get(partitionId), 0)));
                    assignedBuckets
                            .computeIfAbsent(partitionId, k -> new HashSet<>())
                            .add(tableBucket);
                }
            }

            // then, mock partition removed;
            Map<Long, String> removedPartitions = new HashMap<>();
            Set<TableBucket> unsubscribedBuckets = new HashSet<>();
            Set<Long> removedPartitionIds = new HashSet<>();
            int numberOfRemovedPartitions = 2;
            Iterator<Long> partitionIdIterator = partitionNameByIds.keySet().iterator();
            for (int i = 0; i < numberOfRemovedPartitions; i++) {
                long partitionId = partitionIdIterator.next();
                removedPartitions.put(partitionId, partitionNameByIds.get(partitionId));
                removedPartitionIds.add(partitionId);
                unsubscribedBuckets.addAll(assignedBuckets.get(partitionId));
            }
            // reader receives the partition removed event
            reader.handleSourceEvents(new PartitionsRemovedEvent(removedPartitions));

            retry(
                    Duration.ofMinutes(2),
                    () -> {
                        // check the ack event
                        PartitionBucketsUnsubscribedEvent expectedEvent =
                                new PartitionBucketsUnsubscribedEvent(unsubscribedBuckets);
                        List<SourceEvent> gotSourceEvents = readerContext.getSentEvents();
                        assertThat(gotSourceEvents).hasSize(1);
                        assertThat(gotSourceEvents).contains(expectedEvent);
                    });

            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();

            // shouldn't read the rows from the partition that is removed
            List<String> expectRows = new ArrayList<>();
            for (Map.Entry<Long, List<String>> partitionIdAndWrittenRows :
                    partitionWrittenRows.entrySet()) {
                // isn't removed, should read the rows
                if (!removedPartitionIds.contains(partitionIdAndWrittenRows.getKey())) {
                    expectRows.addAll(partitionIdAndWrittenRows.getValue());
                }
            }

            while (output.getEmittedRecords().size() < expectRows.size()) {
                reader.pollNext(output);
            }

            // get the actual rows, the row format will be +I(x,x,x)
            // we need to convert to +I[x, x, x] to match the expected rows format
            List<String> actualRows =
                    output.getEmittedRecords().stream()
                            .map(Object::toString)
                            .map(row -> row.replace("(", "[").replace(")", "]").replace(",", ", "))
                            .collect(Collectors.toList());
            assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectRows);
        }
    }

    private UpsertResult upsert(TablePath tablePath, InternalRow row) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            CompletableFuture<UpsertResult> result = writer.upsert(row);
            writer.flush();
            return result.get();
        }
    }

    private FlinkSourceReader<RowData> createReader(
            Configuration flussConf,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context,
            LakeSource<LakeSplit> lakeSource)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        RowDataDeserializationSchema deserializationSchema = new RowDataDeserializationSchema();
        deserializationSchema.open(
                new DeserializerInitContextImpl(
                        context.metricGroup().addGroup("deserializer"),
                        context.getUserCodeClassLoader(),
                        sourceOutputType));
        FlinkRecordEmitter<RowData> recordEmitter = new FlinkRecordEmitter<>(deserializationSchema);

        return new FlinkSourceReader<>(
                elementsQueue,
                flussConf,
                tablePath,
                sourceOutputType,
                context,
                null,
                null,
                new FlinkSourceReaderMetrics(context.metricGroup()),
                recordEmitter,
                lakeSource);
    }

    private static final class TrackingLakeSource extends TestingLakeSource {

        private final List<LogRecord> records;
        private final AtomicInteger createLakeRecordReaderCount = new AtomicInteger();

        private TrackingLakeSource(List<LogRecord> records) {
            this.records = records;
        }

        @Override
        public RecordReader createRecordReader(ReaderContext<LakeSplit> context)
                throws IOException {
            createLakeRecordReaderCount.incrementAndGet();
            return () -> CloseableIterator.wrap(records.iterator());
        }

        private int getCreateLakeRecordReaderCount() {
            return createLakeRecordReaderCount.get();
        }
    }
}
