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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.FileLogProjection;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.TestData;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.KvEntry;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Value;
import org.apache.fluss.server.kv.rowmerger.DefaultRowMerger;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for WAL memory allocation, rollback and retry. */
class KvTabletWalMemoryTest {
    private static final short schemaId = 1;
    private final Configuration conf = new Configuration();
    private final RowType baseRowType = TestData.DATA1_ROW_TYPE;
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(schemaId);
    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(baseRowType);

    private @TempDir File tempLogDir;
    private @TempDir File tmpKvDir;

    private TestingSchemaGetter schemaGetter =
            new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, schemaId));
    private LogTablet logTablet;
    private KvTablet kvTablet;
    private ExecutorService executor;
    private MemorySegmentPool walMemoryPool = new TestingMemorySegmentPool(10 * 1024);
    private LogFormat walLogFormat = LogFormat.ARROW;

    @BeforeEach
    void beforeEach() {
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void afterEach() {
        executor.shutdownNow();
    }

    private void initLogTabletAndKvTablet(Schema schema, Map<String, String> tableConfig)
            throws Exception {
        PhysicalTablePath path = PhysicalTablePath.of(TablePath.of("testDb", "t1"));
        schemaGetter = new TestingSchemaGetter(new SchemaInfo(schema, schemaId));
        logTablet = createLogTablet(tempLogDir, 0L, path);
        kvTablet =
                createKvTablet(
                        path,
                        logTablet.getTableBucket(),
                        logTablet,
                        tmpKvDir,
                        schemaGetter,
                        tableConfig,
                        RowMerger.create(
                                new TableConfig(Configuration.fromMap(tableConfig)),
                                KvFormat.COMPACTED,
                                schemaGetter));
    }

    private LogTablet createLogTablet(File tempLogDir, long tableId, PhysicalTablePath tablePath)
            throws Exception {
        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempLogDir, tablePath.getDatabaseName(), tableId, tablePath.getTableName());
        return LogTablet.create(
                tempLogDir,
                tablePath,
                logTabletDir,
                conf,
                new AtomicBoolean(
                        conf.get(ConfigOptions.LOG_RETENTION_ROLL_ACTIVE_SEGMENT_ENABLED)),
                TestingMetricGroups.TABLET_SERVER_METRICS,
                0,
                new FlussScheduler(1),
                walLogFormat,
                1,
                true,
                SystemClock.getInstance(),
                true);
    }

    private KvTablet createKvTablet(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File tmpKvDir,
            SchemaGetter schemaGetter,
            Map<String, String> tableConfig,
            RowMerger rowMerger)
            throws Exception {
        TableConfig tableConf = new TableConfig(Configuration.fromMap(tableConfig));
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        schemaGetter,
                        tablePath.getTablePath(),
                        new TableConfig(new Configuration()),
                        new TestingSequenceGeneratorFactory());
        return KvTablet.create(
                tablePath,
                tableBucket,
                logTablet,
                tmpKvDir,
                conf,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                new RootAllocator(Long.MAX_VALUE),
                walMemoryPool,
                KvFormat.COMPACTED,
                rowMerger,
                DEFAULT_COMPRESSION,
                schemaGetter,
                tableConf.getChangelogImage(),
                KvManager.getDefaultRateLimiter(),
                autoIncrementManager,
                SystemClock.getInstance(),
                tableConf);
    }

    @ParameterizedTest
    @ValueSource(strings = {"COMPACTED", "ARROW"})
    void testOversizedWalRollsBackAndSubsequentWriteSucceeds(String format) throws Exception {
        Configuration poolConfig = new Configuration();
        poolConfig.set(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE, MemorySize.parse("8kb"));
        poolConfig.set(ConfigOptions.SERVER_BUFFER_PAGE_SIZE, MemorySize.parse("4kb"));
        poolConfig.set(
                ConfigOptions.SERVER_BUFFER_PER_REQUEST_MEMORY_SIZE, MemorySize.parse("4kb"));
        walLogFormat = LogFormat.valueOf(format);
        try (LazyMemorySegmentPool pool =
                LazyMemorySegmentPool.createServerBufferPool(poolConfig)) {
            walMemoryPool = pool;
            initLogTabletAndKvTablet(
                    DATA1_SCHEMA_PK, Collections.singletonMap("table.changelog.image", "FULL"));
            KvTablet tablet = kvTablet;
            LogTablet log = logTablet;
            try {
                byte[] key = "k1".getBytes();
                KvRecordBatch initial =
                        kvRecordBatchFactory.ofRecords(
                                Collections.singletonList(
                                        kvRecordFactory.ofRecord(key, new Object[] {1, "initial"})),
                                100L,
                                0);
                tablet.putAsLeader(initial, null);
                Value original = tablet.getKvPreWriteBuffer().get(Key.of(key));
                assertThat(original).isNotNull();
                long offset = log.localLogEndOffset();
                List<KvRecord> oversized = new ArrayList<>();
                Random random = new Random(42);
                for (int i = 0; i < 1000; i++) {
                    oversized.add(
                            kvRecordFactory.ofRecord(
                                    key,
                                    new Object[] {
                                        1,
                                        "updated-" + i + "-" + Long.toHexString(random.nextLong())
                                    }));
                }
                assertThatThrownBy(
                                () ->
                                        tablet.putAsLeader(
                                                kvRecordBatchFactory.ofRecords(oversized, 100L, 1),
                                                null))
                        .isInstanceOf(RecordTooLargeException.class);
                assertThat(log.localLogEndOffset()).isEqualTo(offset);
                assertThat(tablet.getKvPreWriteBuffer().get(Key.of(key))).isEqualTo(original);
                assertThat(pool.availableMemory()).isEqualTo(pool.totalSize());

                KvRecordBatch next =
                        kvRecordBatchFactory.ofRecords(
                                Collections.singletonList(
                                        kvRecordFactory.ofRecord(key, new Object[] {1, "next"})),
                                100L,
                                1);
                assertThat(tablet.putAsLeader(next, null).duplicated()).isFalse();
                assertThat(log.localLogEndOffset()).isEqualTo(offset + 2);
                LogRecordBatch written =
                        readLogRecords(log, offset, null).batches().iterator().next();
                assertThat(written.writerId()).isEqualTo(100L);
                assertThat(written.batchSequence()).isEqualTo(1);
                assertThat(written.isValid()).isTrue();
                try (LogRecordReadContext context =
                                walLogFormat == LogFormat.COMPACTED
                                        ? LogRecordReadContext.createCompactedRowReadContext(
                                                baseRowType, schemaId, schemaGetter)
                                        : LogRecordReadContext.createArrowReadContext(
                                                baseRowType, schemaId, schemaGetter);
                        CloseableIterator<LogRecord> records = written.records(context)) {
                    LogRecord before = records.next();
                    assertThat(before.getChangeType()).isEqualTo(ChangeType.UPDATE_BEFORE);
                    assertThat(before.getRow().getInt(0)).isEqualTo(1);
                    assertThat(before.getRow().getString(1).toString()).isEqualTo("initial");
                    LogRecord after = records.next();
                    assertThat(after.getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
                    assertThat(after.getRow().getInt(0)).isEqualTo(1);
                    assertThat(after.getRow().getString(1).toString()).isEqualTo("next");
                    assertThat(records.hasNext()).isFalse();
                }
                assertThat(pool.availableMemory()).isEqualTo(pool.totalSize());
            } finally {
                try {
                    tablet.close();
                } finally {
                    log.close();
                }
            }
        }
    }

    @Test
    void testConcurrentWalAllocationRollsBackAndRetries() throws Exception {
        Configuration poolConfig = new Configuration();
        poolConfig.set(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE, MemorySize.parse("2kb"));
        poolConfig.set(ConfigOptions.SERVER_BUFFER_PAGE_SIZE, MemorySize.parse("1kb"));
        poolConfig.set(
                ConfigOptions.SERVER_BUFFER_PER_REQUEST_MEMORY_SIZE, MemorySize.parse("1kb"));
        walLogFormat = LogFormat.COMPACTED;
        List<KvTablet> tablets = new ArrayList<>();
        List<LogTablet> logs = new ArrayList<>();
        try (LazyMemorySegmentPool pool =
                LazyMemorySegmentPool.createServerBufferPool(poolConfig)) {
            AtomicBoolean coordinate = new AtomicBoolean();
            CountDownLatch firstNeedsMoreMemory = new CountDownLatch(1);
            CountDownLatch bothNeedMoreMemory = new CountDownLatch(2);
            walMemoryPool = pool;

            byte[] key = "k1".getBytes();
            KvRecordBatch initial =
                    kvRecordBatchFactory.ofRecords(
                            Collections.singletonList(
                                    kvRecordFactory.ofRecord(key, new Object[] {1, "initial"})),
                            100L,
                            0);
            List<String> values =
                    Arrays.asList(
                            "first-" + String.join("", Collections.nCopies(200, "a")),
                            "second-" + String.join("", Collections.nCopies(200, "b")),
                            "third-" + String.join("", Collections.nCopies(200, "c")));
            List<KvRecord> updates = new ArrayList<>();
            for (String value : values) {
                updates.add(kvRecordFactory.ofRecord(key, new Object[] {1, value}));
            }
            KvRecordBatch batch = kvRecordBatchFactory.ofRecords(updates, 100L, 1);
            try {
                for (int i = 0; i < 2; i++) {
                    PhysicalTablePath path =
                            PhysicalTablePath.of(TablePath.of("testDb", "competing" + i));
                    LogTablet log = createLogTablet(tempLogDir, i, path);
                    logs.add(log);
                    final int tabletIndex = i;
                    RowMerger merger =
                            new DefaultRowMerger(KvFormat.COMPACTED, null) {
                                @Override
                                public BinaryValue merge(
                                        @Nullable BinaryValue oldValue, BinaryValue newValue) {
                                    if (coordinate.get()
                                            && newValue.row
                                                    .getString(1)
                                                    .toString()
                                                    .equals(values.get(2))) {
                                        // Pause after two updates, while the WAL still fits in one
                                        // page.
                                        assertThat(
                                                        tablets.get(tabletIndex)
                                                                .getKvPreWriteBuffer()
                                                                .getMaxLSN())
                                                .isGreaterThan(
                                                        logs.get(tabletIndex).localLogEndOffset());
                                        bothNeedMoreMemory.countDown();
                                        firstNeedsMoreMemory.countDown();
                                        try {
                                            assertThat(
                                                            bothNeedMoreMemory.await(
                                                                    10, TimeUnit.SECONDS))
                                                    .isTrue();
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            throw new AssertionError(
                                                    "Interrupted while coordinating WAL writes", e);
                                        }
                                    }
                                    return super.merge(oldValue, newValue);
                                }
                            };
                    KvTablet tablet =
                            createKvTablet(
                                    path,
                                    log.getTableBucket(),
                                    log,
                                    new File(tmpKvDir, "tablet" + i),
                                    new TestingSchemaGetter(
                                            new SchemaInfo(DATA1_SCHEMA_PK, schemaId)),
                                    Collections.singletonMap("table.changelog.image", "FULL"),
                                    merger);
                    tablets.add(tablet);
                    tablet.putAsLeader(initial, null);
                }
                Value original = tablets.get(1).getKvPreWriteBuffer().get(Key.of(key));
                assertThat(original).isNotNull();
                List<KvEntry> originalEntries =
                        new ArrayList<>(tablets.get(1).getKvPreWriteBuffer().getAllKvEntries());
                long offset = logs.get(1).localLogEndOffset();
                coordinate.set(true);
                Future<LogAppendInfo> older =
                        executor.submit(() -> tablets.get(0).putAsLeader(batch, null));
                assertThat(firstNeedsMoreMemory.await(10, TimeUnit.SECONDS)).isTrue();
                Future<LogAppendInfo> younger =
                        executor.submit(() -> tablets.get(1).putAsLeader(batch, null));
                assertThatThrownBy(() -> younger.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(TimeoutException.class)
                        .hasStackTraceContaining("cannot make progress");
                assertThat(older.get(10, TimeUnit.SECONDS).duplicated()).isFalse();
                assertThat(logs.get(1).localLogEndOffset()).isEqualTo(offset);
                assertThat(tablets.get(1).getKvPreWriteBuffer().get(Key.of(key)))
                        .isEqualTo(original);
                assertThat(pool.availableMemory()).isEqualTo(pool.totalSize());

                // Retry the exact failed batch, retaining its writer id, sequence and payload.
                assertThat(tablets.get(1).getKvPreWriteBuffer().getAllKvEntries())
                        .containsExactlyElementsOf(originalEntries);
                coordinate.set(false);
                assertThat(tablets.get(1).putAsLeader(batch, null).duplicated()).isFalse();
                for (int i = 0; i < 2; i++) {
                    assertThat(logs.get(i).localLogEndOffset()).isEqualTo(offset + 6);
                    assertThat(tablets.get(i).getKvPreWriteBuffer().get(Key.of(key)).get())
                            .isEqualTo(
                                    ValueEncoder.encodeValue(
                                            schemaId,
                                            compactedRow(
                                                    baseRowType, new Object[] {1, values.get(2)})));
                    LogRecordBatch written =
                            readLogRecords(logs.get(i), offset, null).batches().iterator().next();
                    assertThat(written.writerId()).isEqualTo(100L);
                    assertThat(written.batchSequence()).isEqualTo(1);
                    assertThat(written.isValid()).isTrue();
                    try (LogRecordReadContext context =
                                    LogRecordReadContext.createCompactedRowReadContext(
                                            baseRowType, schemaId, schemaGetter);
                            CloseableIterator<LogRecord> records = written.records(context)) {
                        String previous = "initial";
                        for (String value : values) {
                            LogRecord before = records.next();
                            assertThat(before.getChangeType()).isEqualTo(ChangeType.UPDATE_BEFORE);
                            assertThat(before.getRow().getString(1).toString()).isEqualTo(previous);
                            LogRecord after = records.next();
                            assertThat(after.getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
                            assertThat(after.getRow().getString(1).toString()).isEqualTo(value);
                            previous = value;
                        }
                        assertThat(records.hasNext()).isFalse();
                    }
                }
                assertThat(pool.availableMemory()).isEqualTo(pool.totalSize());
                assertThat(pool.queued()).isZero();
            } finally {
                // Unblock allocations even when an assertion detects a deadlock regression.
                pool.close();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
                for (KvTablet tablet : tablets) {
                    tablet.close();
                }
                for (LogTablet log : logs) {
                    log.close();
                }
            }
        }
    }

    private LogRecords readLogRecords(
            LogTablet logTablet, long startOffset, @Nullable FileLogProjection projection)
            throws Exception {
        return logTablet
                .read(
                        startOffset,
                        Integer.MAX_VALUE,
                        FetchIsolation.LOG_END,
                        false,
                        projection,
                        null)
                .getRecords();
    }
}
