/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.FlussSourceBuilder;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests that a union read of a log table returns each bucket's rows in write order, when
 * the bucket's lake data spans several Iceberg commits.
 */
class FlinkUnionReadOrderITCase extends FlinkUnionReadTestBase {

    private static final int BUCKET_NUM = 3;
    private static final List<String> KEYS = Arrays.asList("k0", "k1", "k2", "k3");
    private static final int LAKE_ROUNDS = 5;
    private static final int ROWS_PER_ROUND = 10;
    private static final int LAKE_ROWS_PER_KEY = LAKE_ROUNDS * ROWS_PER_ROUND;
    private static final int TOTAL_ROWS_PER_KEY = LAKE_ROWS_PER_KEY + ROWS_PER_ROUND;

    /** Rows seen by {@link RecordRows}, shared across task attempts. */
    private static final Queue<EmittedRow> EMITTED_ROWS = new ConcurrentLinkedQueue<>();

    /** Set when {@link FailOnceAfterCheckpointReader} has failed the job once. */
    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @BeforeEach
    void resetSharedState() {
        EMITTED_ROWS.clear();
        FAILED_ONCE.set(false);
    }

    @Test
    void testBatchUnionReadKeepsWriteOrderWithinBucket() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "union_read_order_batch");
        prepareTableWithSeveralLakeCommits(tablePath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // One reader owns every bucket and reads their lake splits one after another.
        env.setParallelism(1);
        FlussSource<RowData> source = sourceBuilder(tablePath).setBatch().build();
        Map<String, List<Integer>> seqsByKey = new HashMap<>();
        try (CloseableIterator<RowData> rows =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "union read")
                        .executeAndCollect()) {
            while (rows.hasNext()) {
                RowData row = rows.next();
                seqsByKey
                        .computeIfAbsent(row.getString(0).toString(), k -> new ArrayList<>())
                        .add(row.getInt(1));
            }
        }

        List<Integer> expectedSeqs = seqRange(0, TOTAL_ROWS_PER_KEY);
        for (String key : KEYS) {
            assertThat(seqsByKey.get(key)).as("rows of key %s", key).isEqualTo(expectedSeqs);
        }
    }

    @Test
    void testStreamingUnionReadKeepsWriteOrderAcrossRestoreDuringLakePhase() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "union_read_order_restore");
        prepareTableWithSeveralLakeCommits(tablePath);

        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100);
        // One reader owns every bucket. The reader pauses after a few rows and fails the job once
        // a checkpoint taken during the pause completes, so the restore starts from a checkpoint
        // that holds most of the lake splits.
        env.setParallelism(1);
        env.fromSource(
                        new FailOnceAfterCheckpointSource<>(sourceBuilder(tablePath).build()),
                        WatermarkStrategy.noWatermarks(),
                        "union read")
                .map(new RecordRows())
                .sinkTo(new DiscardingSink<>());
        JobClient jobClient = env.executeAsync();
        try {
            waitUntil(
                    () ->
                            KEYS.stream()
                                    .allMatch(
                                            key -> seqsOf(1, key).contains(TOTAL_ROWS_PER_KEY - 1)),
                    Duration.ofMinutes(2),
                    "the restored job did not read every row");
        } finally {
            jobClient.cancel().get();
        }

        boolean restoredDuringLakePhase = false;
        for (String key : KEYS) {
            List<Integer> beforeFailure = seqsOf(0, key);
            List<Integer> afterRestore = seqsOf(1, key);
            // Before the failure, rows come out in write order from the start.
            assertThat(beforeFailure)
                    .as("rows of key %s before the failure", key)
                    .isEqualTo(seqRange(0, beforeFailure.size()));
            // After the restore, rows continue from the checkpointed position in write order,
            // without a gap to what was read before the failure.
            assertThat(afterRestore).as("rows of key %s after the restore", key).isNotEmpty();
            int resumedFrom = afterRestore.get(0);
            assertThat(resumedFrom)
                    .as(
                            "first row of key %s after the restore must follow the %s rows read"
                                    + " before the failure",
                            key, beforeFailure.size())
                    .isLessThanOrEqualTo(beforeFailure.size());
            assertThat(afterRestore)
                    .as("rows of key %s after the restore", key)
                    .isEqualTo(seqRange(resumedFrom, TOTAL_ROWS_PER_KEY));
            restoredDuringLakePhase |= resumedFrom > 0 && resumedFrom < LAKE_ROWS_PER_KEY;
        }
        // At least one key must resume part way through its lake rows, which means the reader
        // restored its own pending lake splits from the checkpoint.
        assertThat(restoredDuringLakePhase)
                .as("the job must restore from a checkpoint taken during the lake phase")
                .isTrue();
    }

    /**
     * Creates a log table and writes {@link #LAKE_ROUNDS} rounds of rows, waiting for each round to
     * be tiered so that every round is a separate Iceberg commit. A last round is written after
     * tiering stops, so it lives only in the Fluss log. Each key's rows carry a rising sequence
     * number starting at 0.
     */
    private void prepareTableWithSeveralLakeCommits(TablePath tablePath) throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("k", DataTypes.STRING())
                                        .column("seq", DataTypes.INT())
                                        .build())
                        .distributedBy(BUCKET_NUM, "k")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .build();
        long tableId = createTable(tablePath, descriptor);

        JobClient tieringJob = buildTieringJob(execEnv);
        try {
            for (int round = 0; round < LAKE_ROUNDS; round++) {
                writeRound(tablePath, round);
                waitUntilTieredToHighWatermark(tableId);
            }
        } finally {
            tieringJob.cancel().get();
        }
        writeRound(tablePath, LAKE_ROUNDS);

        // Every bucket that holds data must have one data file per tiering round, and the keys
        // must cover more than one bucket.
        Map<Integer, Integer> dataFilesByBucket = new HashMap<>();
        try (CloseableIterable<FileScanTask> tasks =
                icebergCatalog.loadTable(toIceberg(tablePath)).newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                dataFilesByBucket.merge(
                        task.file().partition().get(0, Integer.class), 1, Integer::sum);
            }
        }
        assertThat(dataFilesByBucket).hasSizeGreaterThan(1);
        assertThat(dataFilesByBucket.values()).allMatch(count -> count >= LAKE_ROUNDS);
    }

    private void writeRound(TablePath tablePath, int round) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < ROWS_PER_ROUND; i++) {
            for (String key : KEYS) {
                rows.add(row(key, round * ROWS_PER_ROUND + i));
            }
        }
        writeRows(tablePath, rows, true);
    }

    private void waitUntilTieredToHighWatermark(long tableId) {
        for (int bucket = 0; bucket < BUCKET_NUM; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            waitUntil(
                    () -> {
                        LogTablet logTablet = getLeaderReplica(tableBucket).getLogTablet();
                        return logTablet.getLakeLogEndOffset() >= logTablet.getHighWatermark();
                    },
                    Duration.ofMinutes(2),
                    "bucket " + tableBucket + " was not tiered");
        }
    }

    private FlussSourceBuilder<RowData> sourceBuilder(TablePath tablePath) {
        return FlussSource.<RowData>builder()
                .setBootstrapServers(
                        String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS)))
                .setDatabase(tablePath.getDatabaseName())
                .setTable(tablePath.getTableName())
                .setStartingOffsets(OffsetsInitializer.full())
                .setDeserializationSchema(new RowDataDeserializationSchema());
    }

    private static List<Integer> seqsOf(int attempt, String key) {
        return EMITTED_ROWS.stream()
                .filter(row -> row.attempt == attempt && row.key.equals(key))
                .map(row -> row.seq)
                .collect(Collectors.toList());
    }

    private static List<Integer> seqRange(int fromInclusive, int toExclusive) {
        return IntStream.range(fromInclusive, toExclusive).boxed().collect(Collectors.toList());
    }

    /** A row passed on by {@link RecordRows}, with the task attempt that saw it. */
    private static final class EmittedRow {
        private final int attempt;
        private final String key;
        private final int seq;

        private EmittedRow(int attempt, String key, int seq) {
            this.attempt = attempt;
            this.key = key;
            this.seq = seq;
        }
    }

    /** Records every row together with the task attempt that saw it. */
    private static final class RecordRows extends RichMapFunction<RowData, RowData> {

        @Override
        public RowData map(RowData row) {
            int attempt = getRuntimeContext().getTaskInfo().getAttemptNumber();
            EMITTED_ROWS.add(new EmittedRow(attempt, row.getString(0).toString(), row.getInt(1)));
            return row;
        }
    }

    /** Wraps a source so that its reader is a {@link FailOnceAfterCheckpointReader}. */
    private static final class FailOnceAfterCheckpointSource<
                    T, SplitT extends SourceSplit, EnumChkT>
            implements Source<T, SplitT, EnumChkT>, ResultTypeQueryable<T> {

        private static final long serialVersionUID = 1L;

        private final Source<T, SplitT, EnumChkT> delegate;
        private final TypeInformation<T> producedType;

        private <S extends Source<T, SplitT, EnumChkT> & ResultTypeQueryable<T>>
                FailOnceAfterCheckpointSource(S delegate) {
            this.delegate = delegate;
            this.producedType = delegate.getProducedType();
        }

        @Override
        public Boundedness getBoundedness() {
            return delegate.getBoundedness();
        }

        @Override
        public SourceReader<T, SplitT> createReader(SourceReaderContext readerContext)
                throws Exception {
            return new FailOnceAfterCheckpointReader<>(delegate.createReader(readerContext));
        }

        @Override
        public SplitEnumerator<SplitT, EnumChkT> createEnumerator(
                SplitEnumeratorContext<SplitT> enumContext) throws Exception {
            return delegate.createEnumerator(enumContext);
        }

        @Override
        public SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(
                SplitEnumeratorContext<SplitT> enumContext, EnumChkT checkpoint) throws Exception {
            return delegate.restoreEnumerator(enumContext, checkpoint);
        }

        @Override
        public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
            return delegate.getSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer() {
            return delegate.getEnumeratorCheckpointSerializer();
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return producedType;
        }
    }

    /**
     * Pauses the first task attempt after {@link #PAUSE_AFTER_ROWS} rows and fails it once a
     * checkpoint taken during the pause completes. While paused the reader reports no data, so the
     * task stays idle and checkpoints run normally. That checkpoint holds the reader's pending lake
     * splits, which the restored attempt gets back. After the failure the reader passes everything
     * through.
     */
    private static final class FailOnceAfterCheckpointReader<T, SplitT extends SourceSplit>
            implements SourceReader<T, SplitT> {

        private static final int PAUSE_AFTER_ROWS = 5;

        private final SourceReader<T, SplitT> delegate;
        private final CompletableFuture<Void> checkpointCompletedWhilePaused =
                new CompletableFuture<>();
        private boolean paused;
        private long checkpointTakenWhilePaused = -1L;

        private FailOnceAfterCheckpointReader(SourceReader<T, SplitT> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
            if (!FAILED_ONCE.get()) {
                if (checkpointCompletedWhilePaused.isDone()) {
                    FAILED_ONCE.set(true);
                    throw new RuntimeException("Expected test failure to trigger a restore.");
                }
                // The reader emits at most one row per call, so the pause starts exactly here.
                if (EMITTED_ROWS.size() >= PAUSE_AFTER_ROWS) {
                    paused = true;
                    return InputStatus.NOTHING_AVAILABLE;
                }
            }
            return delegate.pollNext(output);
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return paused && !FAILED_ONCE.get()
                    ? checkpointCompletedWhilePaused
                    : delegate.isAvailable();
        }

        @Override
        public List<SplitT> snapshotState(long checkpointId) {
            if (paused && checkpointTakenWhilePaused < 0) {
                checkpointTakenWhilePaused = checkpointId;
            }
            return delegate.snapshotState(checkpointId);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            delegate.notifyCheckpointComplete(checkpointId);
            if (checkpointTakenWhilePaused >= 0 && checkpointId >= checkpointTakenWhilePaused) {
                checkpointCompletedWhilePaused.complete(null);
            }
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {
            delegate.notifyCheckpointAborted(checkpointId);
        }

        @Override
        public void addSplits(List<SplitT> splits) {
            delegate.addSplits(splits);
        }

        @Override
        public void notifyNoMoreSplits() {
            delegate.notifyNoMoreSplits();
        }

        @Override
        public void handleSourceEvents(SourceEvent event) {
            delegate.handleSourceEvents(event);
        }

        @Override
        public void pauseOrResumeSplits(
                Collection<String> splitsToPause, Collection<String> splitsToResume) {
            delegate.pauseOrResumeSplits(splitsToPause, splitsToResume);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
