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

import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.OffsetCommitCallback;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.lake.state.LakeSnapshotAndFlussLogSplitState;
import org.apache.fluss.flink.source.deserializer.DeserializerInitContextImpl;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.event.FinishedKvSnapshotConsumeEvent;
import org.apache.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import org.apache.fluss.flink.source.event.PartitionsRemovedEvent;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.flink.source.split.SourceSplitState;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkSourceReader}. */
class FlinkSourceReaderTest extends FlinkTestBase {

    @Test
    void testCommitOffsetOnCheckpointRequiresConsumerGroup() throws Exception {
        Configuration sourceConf = new Configuration(clientConf);
        sourceConf.setBoolean(FlinkConnectorOptions.SCAN_COMMIT_OFFSET_ON_CHECKPOINT.key(), true);

        assertThatThrownBy(
                        () ->
                                createReader(
                                        sourceConf,
                                        tableNames.tablePath(DEFAULT_DB, "missing_commit_group"),
                                        DEFAULT_LOG_TABLE_SCHEMA.getRowType(),
                                        new TestingReaderContext(),
                                        null))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessageContaining("Consumer group id");

        sourceConf.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), "  ");
        assertThatThrownBy(
                        () ->
                                createReader(
                                        sourceConf,
                                        tableNames.tablePath(DEFAULT_DB, "empty_commit_group"),
                                        DEFAULT_LOG_TABLE_SCHEMA.getRowType(),
                                        new TestingReaderContext(),
                                        null))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessageContaining("ASCII letters");

        sourceConf.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), "__reserved");
        assertThatThrownBy(
                        () ->
                                createReader(
                                        sourceConf,
                                        tableNames.tablePath(DEFAULT_DB, "reserved_commit_group"),
                                        DEFAULT_LOG_TABLE_SCHEMA.getRowType(),
                                        new TestingReaderContext(),
                                        null))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessageContaining("must not start with '__'");
    }

    @Test
    void testCommitOffsetsOnlyAfterCheckpointCompletion() throws Exception {
        TablePath tablePath = tableNames.tablePath(DEFAULT_DB, "commit_on_checkpoint");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DEFAULT_LOG_TABLE_SCHEMA).distributedBy(1).build();
        long tableId = createTable(tablePath, tableDescriptor);
        TableBucket bucket = new TableBucket(tableId, 0);
        String groupId = tablePath.getTableName();
        writeRows(conn, tablePath, Arrays.asList(row(1, "one"), row(2, "two")), true);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(admin.listConsumerGroupOffsets(groupId))
                                .succeedsWithin(Duration.ofSeconds(10))
                                .isEqualTo(Collections.emptyMap()));
        // The consumer group coordinator leadership may still be moving between tablet servers
        // when the group metadata becomes visible, so wait until the alter succeeds.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        admin.alterConsumerGroupOffsets(
                                                        groupId,
                                                        Collections.singletonMap(
                                                                bucket, new OffsetMetadata(0L)))
                                                .all())
                                .succeedsWithin(Duration.ofSeconds(10)));

        Configuration sourceConf = new Configuration(clientConf);
        sourceConf.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), groupId);
        sourceConf.set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_INTERVAL, Duration.ZERO);

        // The reader context may not expose the job's programmatic checkpoint interval.
        TestingReaderContext readerContext = new TestingReaderContext();
        try (FlinkSourceReader<RowData> reader =
                createReader(
                        sourceConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        null)) {
            reader.addSplits(Collections.singletonList(new LogSplit(bucket, null, 0L)));
            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(output);
                        assertThat(output.getEmittedRecords()).hasSize(2);
                    });

            List<SourceSplitBase> checkpoint = reader.snapshotState(1L);
            assertThat(checkpoint).hasSize(1);
            assertThat(checkpoint.get(0).asLogSplit().getStartingOffset()).isEqualTo(2L);
            assertThat(admin.listConsumerGroupOffsets(groupId, Collections.singleton(bucket)).get())
                    .containsEntry(bucket, new OffsetMetadata(0L));

            reader.notifyCheckpointComplete(1L);
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(output);
                        assertThat(
                                        admin.listConsumerGroupOffsets(
                                                groupId, Collections.singleton(bucket)))
                                .succeedsWithin(Duration.ofSeconds(10))
                                .isEqualTo(
                                        Collections.singletonMap(bucket, new OffsetMetadata(2L)));
                    });
        }
    }

    @Test
    void testFailedCommitKeepsPendingOffsetsUntilLaterSuccess() throws Exception {
        TablePath tablePath = tableNames.tablePath(DEFAULT_DB, "commit_failure_then_success");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DEFAULT_LOG_TABLE_SCHEMA).distributedBy(1).build();
        long tableId = createTable(tablePath, tableDescriptor);
        TableBucket bucket = new TableBucket(tableId, 0);
        String groupId = tablePath.getTableName();
        writeRows(conn, tablePath, Arrays.asList(row(1, "one"), row(2, "two")), true);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(admin.listConsumerGroupOffsets(groupId))
                                .succeedsWithin(Duration.ofSeconds(10))
                                .isEqualTo(Collections.emptyMap()));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        admin.alterConsumerGroupOffsets(
                                                        groupId,
                                                        Collections.singletonMap(
                                                                bucket, new OffsetMetadata(0L)))
                                                .all())
                                .succeedsWithin(Duration.ofSeconds(10)));

        Configuration sourceConf = new Configuration(clientConf);
        sourceConf.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), groupId);
        sourceConf.set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_INTERVAL, Duration.ZERO);

        TestingReaderContext readerContext = new TestingReaderContext();
        try (TestingFlinkSourceReader<RowData> reader =
                createReader(
                        sourceConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        null)) {
            reader.addSplits(Collections.singletonList(new LogSplit(bucket, null, 0L)));
            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(output);
                        assertThat(output.getEmittedRecords()).hasSize(2);
                    });

            reader.snapshotState(1L);
            reader.failNextCommitWith(new RuntimeException("injected commit failure"));
            reader.notifyCheckpointComplete(1L);
            assertThat(reader.getOffsetsToCommitForTest()).containsKey(1L);
            assertThat(admin.listConsumerGroupOffsets(groupId, Collections.singleton(bucket)).get())
                    .containsEntry(bucket, new OffsetMetadata(0L));

            reader.snapshotState(2L);
            reader.notifyCheckpointComplete(2L);
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(output);
                        assertThat(
                                        admin.listConsumerGroupOffsets(
                                                groupId, Collections.singleton(bucket)))
                                .succeedsWithin(Duration.ofSeconds(10))
                                .isEqualTo(
                                        Collections.singletonMap(bucket, new OffsetMetadata(2L)));
                        assertThat(reader.getOffsetsToCommitForTest()).doesNotContainKeys(1L, 2L);
                    });
        }
    }

    @Test
    void testHybridSnapshotBoundaryCommitsOnlyAfterSnapshotFinished() throws Exception {
        TablePath tablePath = tableNames.tablePath(DEFAULT_DB, "hybrid_snapshot_commit_boundary");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DEFAULT_PK_TABLE_SCHEMA)
                        .distributedBy(1, "id")
                        .build();
        long tableId = createTable(tablePath, tableDescriptor);

        UpsertResult seedResult = upsert(tablePath, row(1, "snapshot"));
        TableBucket tableBucket = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);
        KvSnapshots snapshots = admin.getLatestKvSnapshots(tablePath).get();
        HybridSnapshotLogSplit split =
                new HybridSnapshotLogSplit(
                        tableBucket,
                        null,
                        snapshots.getSnapshotId(0).getAsLong(),
                        0,
                        false,
                        snapshots.getLogOffset(0).getAsLong(),
                        LogSplit.NO_STOPPING_OFFSET,
                        false);

        Configuration sourceConf = new Configuration(clientConf);
        sourceConf.setString(
                ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), tablePath.getTableName());
        try (TestingFlinkSourceReader<RowData> reader =
                createReader(
                        sourceConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        new TestingReaderContext(),
                        null)) {
            reader.addSplits(Collections.singletonList(split));

            List<SourceSplitBase> beforeSnapshotCompletion = reader.snapshotState(1L);
            assertThat(beforeSnapshotCompletion).hasSize(1);
            assertThat(reader.getOffsetsToCommitForTest().get(1L)).isEmpty();

            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(output);
                        assertThat(output.getEmittedRecords()).hasSize(1);
                        // The emitted record can reach the output before the split state records
                        // the snapshot EOF, so wait for the finished state here.
                        assertThat(
                                        reader.snapshotState(2L)
                                                .get(0)
                                                .asHybridSnapshotLogSplit()
                                                .isSnapshotFinished())
                                .isTrue();
                    });

            List<SourceSplitBase> afterSnapshotCompletion = reader.snapshotState(2L);
            assertThat(afterSnapshotCompletion).hasSize(1);
            assertThat(
                            afterSnapshotCompletion
                                    .get(0)
                                    .asHybridSnapshotLogSplit()
                                    .isSnapshotFinished())
                    .isTrue();
            assertThat(reader.getOffsetsToCommitForTest().get(2L))
                    .containsEntry(
                            tableBucket, new OffsetMetadata(snapshots.getLogOffset(0).getAsLong()));
        }
    }

    @Test
    void testCheckpointHybridSnapshotFinishedBeforeFirstLogRecord() throws Exception {
        TablePath tablePath =
                tableNames.tablePath(DEFAULT_DB, "test_checkpoint_hybrid_snapshot_finished");
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
        TablePath tablePath =
                tableNames.tablePath(DEFAULT_DB, "test_checkpoint_lake_split_finished");
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
        Configuration sourceConf = new Configuration(clientConf);
        sourceConf.setString(
                ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), tablePath.getTableName());
        LakeSnapshotAndFlussLogSplit split =
                new LakeSnapshotAndFlussLogSplit(
                        tableBucket,
                        null,
                        Collections.singletonList(new TestingLakeSplit(0, Collections.emptyList())),
                        startingOffset,
                        LogSplit.NO_STOPPING_OFFSET);

        TestingReaderOutput<RowData> lakeOutput = new TestingReaderOutput<>();
        LakeSnapshotAndFlussLogSplit checkpointSplit;
        try (TestingFlinkSourceReader<RowData> reader =
                createReader(
                        sourceConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        new TestingReaderContext(),
                        lakeSource)) {
            reader.addSplits(Collections.singletonList(split));

            List<SourceSplitBase> beforeLakeCompletion = reader.snapshotState(1L);
            assertThat(beforeLakeCompletion).hasSize(1);
            assertThat(
                            ((LakeSnapshotAndFlussLogSplit) beforeLakeCompletion.get(0))
                                    .isLakeSplitFinished())
                    .isFalse();
            assertThat(reader.getOffsetsToCommitForTest().get(1L)).isEmpty();

            // Poll until the lake EOF marker reaches the split state. No incremental record
            // exists yet, so the next checkpoint captures the lake-to-log boundary.
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(lakeOutput);
                        assertThat(lakeOutput.getEmittedRecords()).hasSize(1);
                        // The emitted record can reach the output before the split state records
                        // the lake EOF, so wait for the finished state here.
                        assertThat(
                                        ((LakeSnapshotAndFlussLogSplit)
                                                        reader.snapshotState(2L).get(0))
                                                .isLakeSplitFinished())
                                .isTrue();
                    });

            List<SourceSplitBase> checkpoint = reader.snapshotState(2L);
            assertThat(checkpoint).hasSize(1);
            checkpointSplit = (LakeSnapshotAndFlussLogSplit) checkpoint.get(0);
            assertThat(checkpointSplit.isLakeSplitFinished()).isTrue();
            // the finished lake splits are never read again, so they are not checkpointed
            assertThat(checkpointSplit.getLakeSplits()).isNull();
            assertThat(checkpointSplit.getStartingOffset()).isEqualTo(startingOffset);
            assertThat(checkpointSplit.getRecordsToSkip()).isEqualTo(1L);
            assertThat(checkpointSplit.getCurrentLakeSplitIndex()).isZero();
            assertThat(reader.getOffsetsToCommitForTest().get(2L))
                    .containsEntry(tableBucket, new OffsetMetadata(startingOffset));
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
    void testFinishedLakeHybridSplitRecordsOnlyEligibleOffset() throws Exception {
        TablePath tablePath =
                tableNames.tablePath(DEFAULT_DB, "finished_lake_hybrid_split_offsets");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DEFAULT_PK_TABLE_SCHEMA)
                        .distributedBy(1, "id")
                        .build();
        long tableId = createTable(tablePath, tableDescriptor);
        TableBucket unfinishedBucket = new TableBucket(tableId, 0);
        TableBucket finishedBucket = new TableBucket(tableId, 1);
        TableBucket batchBucket = new TableBucket(tableId, 2);

        LakeSnapshotAndFlussLogSplit unfinishedSplit =
                new LakeSnapshotAndFlussLogSplit(
                        unfinishedBucket,
                        null,
                        Collections.singletonList(new TestingLakeSplit(0, Collections.emptyList())),
                        100L,
                        LogSplit.NO_STOPPING_OFFSET);
        LakeSnapshotAndFlussLogSplit finishedSplit =
                new LakeSnapshotAndFlussLogSplit(
                        finishedBucket, null, null, 200L, LogSplit.NO_STOPPING_OFFSET);
        LakeSnapshotAndFlussLogSplit batchSplit =
                new LakeSnapshotAndFlussLogSplit(
                        batchBucket,
                        null,
                        Collections.singletonList(new TestingLakeSplit(0, Collections.emptyList())),
                        300L,
                        400L);

        Map<String, SourceSplitState> finishedSplits = new HashMap<>();
        finishedSplits.put(
                unfinishedSplit.splitId(), new LakeSnapshotAndFlussLogSplitState(unfinishedSplit));
        finishedSplits.put(
                finishedSplit.splitId(), new LakeSnapshotAndFlussLogSplitState(finishedSplit));
        finishedSplits.put(batchSplit.splitId(), new LakeSnapshotAndFlussLogSplitState(batchSplit));

        Configuration sourceConf = new Configuration(clientConf);
        sourceConf.setString(
                ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), tablePath.getTableName());
        try (TestingFlinkSourceReader<RowData> reader =
                createReader(
                        sourceConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        new TestingReaderContext(),
                        new TestingLakeSource())) {
            reader.onSplitFinished(finishedSplits);
            List<SourceSplitBase> checkpoint = reader.snapshotState(1L);
            assertThat(checkpoint).isEmpty();
            assertThat(reader.getOffsetsToCommitForTest().get(1L))
                    .containsEntry(finishedBucket, new OffsetMetadata(200L))
                    .hasSize(1);
        }
    }

    @Test
    void testDelayedCommitDoesNotRemoveNewerFinishedOffset() throws Exception {
        TablePath tablePath = tableNames.tablePath(DEFAULT_DB, "delayed_commit_finished_offset");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DEFAULT_LOG_TABLE_SCHEMA).distributedBy(1).build();
        long tableId = createTable(tablePath, tableDescriptor);
        TableBucket bucket = new TableBucket(tableId, 0);
        String groupId = tablePath.getTableName();
        writeRows(conn, tablePath, Arrays.asList(row(1, "one"), row(2, "two")), true);
        // Wait for the consumer group coordinator leadership; a commit sent while replicas are
        // still installing leadership fails and, by design, is not retried by the reader.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(admin.listConsumerGroupOffsets(groupId))
                                .succeedsWithin(Duration.ofSeconds(10))
                                .isEqualTo(Collections.emptyMap()));
        LogSplit split = new LogSplit(bucket, null, 0L);

        Configuration sourceConf = new Configuration(clientConf);
        sourceConf.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), groupId);
        sourceConf.set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_INTERVAL, Duration.ZERO);

        try (TestingFlinkSourceReader<RowData> reader =
                createReader(
                        sourceConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        new TestingReaderContext(),
                        null)) {
            reader.addSplits(Collections.singletonList(split));
            List<SourceSplitBase> checkpoint = reader.snapshotState(1L);
            assertThat(checkpoint).hasSize(1);
            assertThat(checkpoint.get(0).asLogSplit().getStartingOffset()).isEqualTo(0L);
            assertThat(reader.getOffsetsToCommitForTest().get(1L))
                    .containsEntry(bucket, new OffsetMetadata(0L));

            LogSplitState finishedState = new LogSplitState(split);
            finishedState.setNextOffset(2L);
            reader.onSplitFinished(Collections.singletonMap(split.splitId(), finishedState));

            reader.delayNextCommit();
            reader.notifyCheckpointComplete(1L);
            reader.completeDelayedCommit(null);
            assertThat(reader.getOffsetsToCommitForTest()).doesNotContainKey(1L);

            List<SourceSplitBase> laterCheckpoint = reader.snapshotState(2L);
            assertThat(laterCheckpoint).hasSize(1);
            assertThat(reader.getOffsetsToCommitForTest().get(2L))
                    .containsEntry(bucket, new OffsetMetadata(2L));

            reader.notifyCheckpointComplete(2L);
            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(output);
                        assertThat(
                                        admin.listConsumerGroupOffsets(
                                                groupId, Collections.singleton(bucket)))
                                .succeedsWithin(Duration.ofSeconds(10))
                                .isEqualTo(
                                        Collections.singletonMap(bucket, new OffsetMetadata(2L)));
                        assertThat(reader.getOffsetsToCommitForTest()).doesNotContainKeys(1L, 2L);
                    });
        }
    }

    @Test
    void testRestoreHybridLakeSplitAcrossBusinessPartitions() throws Exception {
        TablePath tablePath =
                tableNames.tablePath(DEFAULT_DB, "test_restore_multi_partition_lake_split");
        // Model the historical bucket's shared log with one physical bucket. Its lake baseline
        // contains separate splits for two business partitions, with overlapping primary-key IDs.
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("dt", DataTypes.STRING())
                                        .column("name", DataTypes.STRING())
                                        .primaryKey("id", "dt")
                                        .build())
                        .distributedBy(1, "id")
                        .build();
        long tableId = createTable(tablePath, tableDescriptor);
        RowType rowType = tableDescriptor.getSchema().getRowType();
        TableBucket tableBucket = new TableBucket(tableId, 0);
        List<InternalRow> baseline =
                Arrays.asList(
                        row(1, "20240101", "Alice"),
                        row(2, "20240101", "Bob"),
                        row(1, "20240102", "Carol"),
                        row(2, "20240102", "Dave"),
                        row(3, "20240102", "Eve"));
        Map<String, List<LogRecord>> lakeRecords = new HashMap<>();
        long startingOffset = 0;
        for (InternalRow record : baseline) {
            UpsertResult result = upsert(tablePath, record);
            assertThat(result.getBucket()).isEqualTo(tableBucket);
            startingOffset = result.getLogEndOffset();
            lakeRecords
                    .computeIfAbsent(record.getString(1).toString(), ignored -> new ArrayList<>())
                    .add(new GenericRecord(-1L, -1L, ChangeType.INSERT, record));
        }
        TestingLakeSource lakeSource =
                new TestingLakeSource() {
                    @Override
                    public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
                        List<LogRecord> records =
                                lakeRecords.get(context.lakeSplit().partition().get(0));
                        return () -> CloseableIterator.wrap(records.iterator());
                    }
                };
        LakeSnapshotAndFlussLogSplit split =
                new LakeSnapshotAndFlussLogSplit(
                        tableBucket,
                        null,
                        Arrays.asList(
                                new TestingLakeSplit(0, Collections.singletonList("20240101")),
                                new TestingLakeSplit(0, Collections.singletonList("20240102"))),
                        startingOffset,
                        LogSplit.NO_STOPPING_OFFSET);

        TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
        SourceSplitSerializer serializer = new SourceSplitSerializer(lakeSource);
        SourceSplitBase restoredSplit;
        try (FlinkSourceReader<RowData> reader =
                createReader(
                        clientConf, tablePath, rowType, new TestingReaderContext(), lakeSource)) {
            reader.addSplits(Collections.singletonList(split));
            // Finish the first business partition and emit only the first row of the second.
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(output);
                        assertThat(output.getEmittedRecords()).hasSize(3);
                    });
            assertThat(output.getEmittedRecords())
                    .extracting(Object::toString)
                    .containsExactly(
                            "+I(1,20240101,Alice)", "+I(2,20240101,Bob)", "+I(1,20240102,Carol)");
            List<SourceSplitBase> checkpoint = reader.snapshotState(1L);
            assertThat(checkpoint).hasSize(1);
            LakeSnapshotAndFlussLogSplit checkpointSplit =
                    (LakeSnapshotAndFlussLogSplit) checkpoint.get(0);
            assertThat(checkpointSplit.isLakeSplitFinished()).isFalse();
            assertThat(checkpointSplit.getStartingOffset()).isEqualTo(startingOffset);
            // Include the real serializer so both business partitions and the resume position
            // must survive checkpointing, rather than reusing the in-memory split.
            restoredSplit =
                    serializer.deserialize(
                            serializer.getVersion(), serializer.serialize(checkpointSplit));
        }

        // Change rows whose baseline has not been emitted yet. Recovery must finish the lake
        // baseline before emitting these updates from the shared log, without replaying its seeds.
        upsert(tablePath, row(2, "20240102", "Dave-updated"));
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.delete(row(3, "20240102", "Eve")).get();
        }
        TestingReaderOutput<RowData> restoredOutput = new TestingReaderOutput<>();
        try (FlinkSourceReader<RowData> reader =
                createReader(
                        clientConf, tablePath, rowType, new TestingReaderContext(), lakeSource)) {
            reader.addSplits(Collections.singletonList(restoredSplit));
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(restoredOutput);
                        assertThat(restoredOutput.getEmittedRecords()).hasSize(5);
                    });
            assertThat(restoredOutput.getEmittedRecords())
                    .extracting(Object::toString)
                    .containsExactly(
                            "+I(2,20240102,Dave)",
                            "+I(3,20240102,Eve)",
                            "-U(2,20240102,Dave)",
                            "+U(2,20240102,Dave-updated)",
                            "-D(3,20240102,Eve)");
        }
    }

    @Test
    void testRestorePartiallyConsumedLakeSnapshotSplit() throws Exception {
        TablePath tablePath =
                tableNames.tablePath(
                        DEFAULT_DB, "test_restore_partially_consumed_lake_snapshot_split");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DEFAULT_LOG_TABLE_SCHEMA).distributedBy(1).build();
        long tableId = createTable(tablePath, tableDescriptor);
        RowType rowType = tableDescriptor.getSchema().getRowType();

        List<LogRecord> lakeRecords = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            lakeRecords.add(new GenericRecord(-1L, -1L, ChangeType.APPEND_ONLY, row(i, "v" + i)));
        }
        LakeSnapshotSplit split =
                new LakeSnapshotSplit(
                        new TableBucket(tableId, 0),
                        null,
                        new TestingLakeSplit(0, Collections.emptyList()),
                        0);

        // consume the first record of the lake split, then checkpoint
        TrackingLakeSource lakeSource = new TrackingLakeSource(lakeRecords);
        SourceSplitSerializer splitSerializer = new SourceSplitSerializer(lakeSource);
        TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
        SourceSplitBase restoredSplit;
        try (FlinkSourceReader<RowData> reader =
                createReader(
                        clientConf, tablePath, rowType, new TestingReaderContext(), lakeSource)) {
            reader.addSplits(Collections.singletonList(split));
            // each poll emits at most one record
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(output);
                        assertThat(output.getEmittedRecords()).hasSize(1);
                    });

            List<SourceSplitBase> checkpoint = reader.snapshotState(1L);
            assertThat(checkpoint).hasSize(1);
            LakeSnapshotSplit checkpointSplit = (LakeSnapshotSplit) checkpoint.get(0);
            assertThat(checkpointSplit.getRecordsToSkip()).isEqualTo(1L);

            // round trip through the split serializer, as Flink does when checkpointing
            restoredSplit =
                    splitSerializer.deserialize(
                            splitSerializer.getVersion(),
                            splitSerializer.serialize(checkpointSplit));
        }
        assertThat(((LakeSnapshotSplit) restoredSplit).getRecordsToSkip()).isEqualTo(1L);

        // the restored reader must continue with the remaining records only
        TestingReaderOutput<RowData> restoredOutput = new TestingReaderOutput<>();
        try (FlinkSourceReader<RowData> restoredReader =
                createReader(
                        clientConf,
                        tablePath,
                        rowType,
                        new TestingReaderContext(),
                        new TrackingLakeSource(lakeRecords))) {
            restoredReader.addSplits(Collections.singletonList(restoredSplit));
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        restoredReader.pollNext(restoredOutput);
                        assertThat(restoredOutput.getEmittedRecords()).hasSize(2);
                    });
        }

        assertThat(output.getEmittedRecords())
                .extracting(record -> record.getInt(0))
                .containsExactly(1);
        assertThat(restoredOutput.getEmittedRecords())
                .extracting(record -> record.getInt(0))
                .containsExactly(2, 3);
    }

    @Test
    void testHandlePartitionsRemovedEvent() throws Exception {
        TablePath tablePath = tableNames.tablePath(DEFAULT_DB, "test_partitioned_table");

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

    @Test
    void testPartitionDeletionRetainsFinalOffsetUntilCheckpointCommit() throws Exception {
        TablePath tablePath = tableNames.tablePath(DEFAULT_DB, "partition_deletion_final_offset");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .column("pt", DataTypes.STRING())
                                        .build())
                        .distributedBy(1, "id")
                        .partitionedBy("pt")
                        .build();
        long tableId = createTable(tablePath, tableDescriptor);
        PartitionSpec partitionSpec = new PartitionSpec(Collections.singletonMap("pt", "deleted"));
        admin.createPartition(tablePath, partitionSpec, false).get();
        long partitionId =
                admin.listPartitionInfos(tablePath, partitionSpec).get().get(0).getPartitionId();
        TableBucket bucket = new TableBucket(tableId, partitionId, 0);
        String groupId = tablePath.getTableName();
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        admin.alterConsumerGroupOffsets(
                                                        groupId,
                                                        Collections.singletonMap(
                                                                bucket, new OffsetMetadata(0L)))
                                                .all())
                                .succeedsWithin(Duration.ofSeconds(10)));
        writeRows(
                conn,
                tablePath,
                Arrays.asList(row(1, "one", "deleted"), row(2, "two", "deleted")),
                true);

        Configuration sourceConf = new Configuration(clientConf);
        sourceConf.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), groupId);
        sourceConf.set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_INTERVAL, Duration.ZERO);
        TestingReaderContext readerContext = new TestingReaderContext();
        try (TestingFlinkSourceReader<RowData> reader =
                createReader(
                        sourceConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        null)) {
            reader.addSplits(Collections.singletonList(new LogSplit(bucket, "deleted", 0L)));
            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(output);
                        assertThat(output.getEmittedRecords()).hasSize(2);
                    });

            List<SourceSplitBase> beforeDeletion = reader.snapshotState(1L);
            assertThat(beforeDeletion).hasSize(1);
            assertThat(beforeDeletion.get(0).asLogSplit().getStartingOffset()).isEqualTo(2L);

            reader.handleSourceEvents(
                    new PartitionsRemovedEvent(Collections.singletonMap(partitionId, "deleted")));
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        reader.pollNext(output);
                        assertThat(reader.getNumberOfCurrentlyAssignedSplits()).isZero();
                        assertThat(readerContext.getSentEvents())
                                .contains(
                                        new PartitionBucketsUnsubscribedEvent(
                                                Collections.singleton(bucket)));
                    });

            assertThat(admin.listConsumerGroupOffsets(groupId, Collections.singleton(bucket)).get())
                    .containsEntry(bucket, new OffsetMetadata(0L));

            List<SourceSplitBase> afterDeletion = reader.snapshotState(2L);
            assertThat(afterDeletion).isEmpty();
            assertThat(reader.getOffsetsToCommitForTest().get(2L))
                    .containsEntry(bucket, new OffsetMetadata(2L));

            reader.notifyCheckpointComplete(2L);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            admin.listConsumerGroupOffsets(
                                                            groupId, Collections.singleton(bucket))
                                                    .get())
                                    .containsEntry(bucket, new OffsetMetadata(2L)));
        }
    }

    @Test
    void testPendingRecordsMetricAfterFetcherRecreation() throws Exception {
        TablePath tablePath =
                tableNames.tablePath(DEFAULT_DB, "test_pending_records_after_fetcher_recreation");
        TableDescriptor tableDescriptor = DEFAULT_AUTO_PARTITIONED_LOG_TABLE_DESCRIPTOR;
        long tableId = createTable(tablePath, tableDescriptor);

        Map<Long, String> partitionNameByIds =
                waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath);
        assertThat(partitionNameByIds).hasSizeGreaterThanOrEqualTo(2);
        Iterator<Map.Entry<Long, String>> partitionIterator =
                partitionNameByIds.entrySet().iterator();
        Map.Entry<Long, String> firstPartition = partitionIterator.next();
        Map.Entry<Long, String> secondPartition = partitionIterator.next();

        appendRowsToPartition(tablePath, firstPartition.getValue(), 100);
        appendRowsToPartition(tablePath, secondPartition.getValue(), 100);

        Configuration sourceConf = new Configuration(clientConf);
        sourceConf.setInt(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS, 1);
        sourceConf.setString(
                ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET.key(), "1b");

        MetricListener metricListener = new MetricListener();
        TestingReaderContext readerContext =
                new TestingReaderContext(
                        new org.apache.flink.configuration.Configuration(),
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
        FlinkSourceReaderMetrics sourceReaderMetrics =
                new FlinkSourceReaderMetrics(readerContext.metricGroup());

        try (TestingFlinkSourceReader<RowData> reader =
                createReader(
                        sourceConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        null,
                        sourceReaderMetrics)) {
            assertThat(metricListener.getGauge(MetricNames.PENDING_RECORDS)).isEmpty();

            reader.addSplits(createLogSplits(tableId, firstPartition));
            Optional<Gauge<Long>> pendingRecords =
                    metricListener.getGauge(MetricNames.PENDING_RECORDS);
            assertThat(pendingRecords).isPresent();
            Gauge<Long> registeredPendingRecords = pendingRecords.get();
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat((long) registeredPendingRecords.getValue()).isPositive());

            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
            removePartitionAndWaitForFetcherShutdown(reader, firstPartition, output);
            assertThat((long) registeredPendingRecords.getValue()).isZero();

            reader.addSplits(createLogSplits(tableId, secondPartition));
            assertThat(reader.getNumAliveFetchers()).isOne();
            assertThat(metricListener.getGauge(MetricNames.PENDING_RECORDS).get())
                    .isSameAs(registeredPendingRecords);
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat((long) registeredPendingRecords.getValue()).isPositive());

            // The recreated fetcher has a monotonically increasing ID. Removing its partition
            // must target the currently running fetcher rather than assuming the original ID.
            removePartitionAndWaitForFetcherShutdown(reader, secondPartition, output);
            assertThat((long) registeredPendingRecords.getValue()).isZero();
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

    private void appendRowsToPartition(TablePath tablePath, String partitionName, int numRows)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            rows.add(row(i, partitionName));
        }
        writeRows(conn, tablePath, rows, true);
    }

    private List<SourceSplitBase> createLogSplits(long tableId, Map.Entry<Long, String> partition) {
        List<SourceSplitBase> splits = new ArrayList<>(DEFAULT_BUCKET_NUM);
        for (int bucket = 0; bucket < DEFAULT_BUCKET_NUM; bucket++) {
            splits.add(
                    new LogSplit(
                            new TableBucket(tableId, partition.getKey(), bucket),
                            partition.getValue(),
                            0L));
        }
        return splits;
    }

    private void removePartitionAndWaitForFetcherShutdown(
            TestingFlinkSourceReader<RowData> reader,
            Map.Entry<Long, String> partition,
            TestingReaderOutput<RowData> output)
            throws Exception {
        reader.handleSourceEvents(
                new PartitionsRemovedEvent(
                        Collections.singletonMap(partition.getKey(), partition.getValue())));
        retry(
                Duration.ofMinutes(1),
                () -> {
                    reader.pollNext(output);
                    assertThat(reader.getNumberOfCurrentlyAssignedSplits()).isZero();
                    assertThat(reader.getNumAliveFetchers()).isZero();
                });
    }

    private TestingFlinkSourceReader<RowData> createReader(
            Configuration flussConf,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context,
            LakeSource<LakeSplit> lakeSource)
            throws Exception {
        return createReader(
                flussConf,
                tablePath,
                sourceOutputType,
                context,
                lakeSource,
                new FlinkSourceReaderMetrics(context.metricGroup()));
    }

    private TestingFlinkSourceReader<RowData> createReader(
            Configuration flussConf,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context,
            LakeSource<LakeSplit> lakeSource,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics)
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

        return new TestingFlinkSourceReader<>(
                elementsQueue,
                flussConf,
                FlinkConnectorOptionsUtils.commitOffsetsOnCheckpoint(flussConf),
                false,
                tablePath,
                sourceOutputType,
                context,
                flinkSourceReaderMetrics,
                recordEmitter,
                lakeSource);
    }

    private static final class TestingFlinkSourceReader<OUT> extends FlinkSourceReader<OUT> {

        private Exception nextCommitFailure;
        private boolean delayNextCommit;
        private Map<TableBucket, OffsetMetadata> delayedCommitOffsets;
        private OffsetCommitCallback delayedCommitCallback;

        private TestingFlinkSourceReader(
                FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue,
                Configuration flussConfig,
                boolean commitOffsetOnCheckpoint,
                boolean batchExecution,
                TablePath tablePath,
                RowType sourceOutputType,
                SourceReaderContext context,
                FlinkSourceReaderMetrics flinkSourceReaderMetrics,
                FlinkRecordEmitter<OUT> recordEmitter,
                LakeSource<LakeSplit> lakeSource) {
            super(
                    elementsQueue,
                    flussConfig,
                    commitOffsetOnCheckpoint,
                    batchExecution,
                    tablePath,
                    sourceOutputType,
                    context,
                    null,
                    null,
                    flinkSourceReaderMetrics,
                    recordEmitter,
                    lakeSource);
        }

        private int getNumAliveFetchers() {
            return splitFetcherManager.getNumAliveFetchers();
        }

        private void failNextCommitWith(Exception failure) {
            this.nextCommitFailure = failure;
        }

        private void delayNextCommit() {
            this.delayNextCommit = true;
        }

        private void completeDelayedCommit(Exception exception) {
            if (delayedCommitCallback == null) {
                throw new IllegalStateException("delayed commit callback is not set");
            }
            OffsetCommitCallback callback = delayedCommitCallback;
            Map<TableBucket, OffsetMetadata> offsets = delayedCommitOffsets;
            delayedCommitCallback = null;
            delayedCommitOffsets = null;
            callback.onComplete(offsets, exception);
        }

        @Override
        protected void commitOffsetsAsync(
                Map<TableBucket, OffsetMetadata> offsetsToCommit, OffsetCommitCallback callback) {
            if (delayNextCommit) {
                delayNextCommit = false;
                delayedCommitOffsets = offsetsToCommit;
                delayedCommitCallback = callback;
                return;
            }
            if (nextCommitFailure != null) {
                Exception failure = nextCommitFailure;
                nextCommitFailure = null;
                callback.onComplete(offsetsToCommit, failure);
                return;
            }
            super.commitOffsetsAsync(offsetsToCommit, callback);
        }
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
