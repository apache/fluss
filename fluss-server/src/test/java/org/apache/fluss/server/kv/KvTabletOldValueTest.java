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
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestData;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.Statistics;
import io.github.fluss_contrib.rocksdb.TickerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests batched old-value reads in {@link KvTablet}. */
class KvTabletOldValueTest {
    private static final short SCHEMA_ID = 1;
    private final Configuration conf = new Configuration();
    private final RowType rowType = TestData.DATA1_ROW_TYPE;
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(rowType);

    private @TempDir File tempLogDir;
    private @TempDir File tmpKvDir;
    private TestingSchemaGetter schemaGetter;
    private LogTablet logTablet;
    private KvTablet kvTablet;

    @AfterEach
    void afterEach() throws Exception {
        IOUtils.closeAll(
                () -> {
                    if (kvTablet != null) {
                        kvTablet.close();
                    }
                },
                () -> {
                    if (logTablet != null) {
                        logTablet.close();
                    }
                });
    }

    private void initLogTabletAndKvTablet(Schema schema, Map<String, String> tableConfig)
            throws Exception {
        PhysicalTablePath path = PhysicalTablePath.of(TablePath.of("testDb", "t1"));
        schemaGetter = new TestingSchemaGetter(new SchemaInfo(schema, SCHEMA_ID));
        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempLogDir, path.getDatabaseName(), 0L, path.getTableName());
        logTablet =
                LogTablet.create(
                        tempLogDir,
                        path,
                        logTabletDir,
                        conf,
                        new AtomicBoolean(
                                conf.get(ConfigOptions.LOG_RETENTION_ROLL_ACTIVE_SEGMENT_ENABLED)),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0,
                        new FlussScheduler(1),
                        LogFormat.ARROW,
                        1,
                        true,
                        SystemClock.getInstance(),
                        true);
        TableBucket tableBucket = logTablet.getTableBucket();
        TableConfig tableConf = new TableConfig(Configuration.fromMap(tableConfig));
        RowMerger rowMerger = RowMerger.create(tableConf, KvFormat.COMPACTED, schemaGetter);
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        schemaGetter,
                        path.getTablePath(),
                        new TableConfig(new Configuration()),
                        new TestingSequenceGeneratorFactory());
        kvTablet =
                KvTablet.create(
                        path,
                        tableBucket,
                        logTablet,
                        tmpKvDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        new RootAllocator(Long.MAX_VALUE),
                        new TestingMemorySegmentPool(10 * 1024),
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
    @ValueSource(booleans = {true, false})
    void testBatchOldValuesPreserveOrderedChangelogAndBufferedTombstones(
            boolean batchOldValueLookupEnabled) throws Exception {
        if (!batchOldValueLookupEnabled) {
            conf.set(ConfigOptions.KV_WRITE_BATCH_OLD_VALUE_LOOKUP_ENABLED, false);
        }
        initLogTabletAndKvTablet(DATA1_SCHEMA_PK, new HashMap<>());
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1", new Object[] {1, "v1"}),
                        kvRecordFactory.ofRecord("k2", new Object[] {2, "v2"})),
                null);
        flushAndWait(kvTablet, Long.MAX_VALUE);
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k2", null),
                        kvRecordFactory.ofRecord("k3", new Object[] {3, "v3"})),
                null);
        long offset = logTablet.localLogEndOffset();
        Statistics statistics = kvTablet.getRocksDBKv().getStatistics();
        long gets = statistics.getTickerCount(TickerType.NUMBER_KEYS_READ);
        long calls = statistics.getTickerCount(TickerType.NUMBER_MULTIGET_CALLS);
        long keys = statistics.getTickerCount(TickerType.NUMBER_MULTIGET_KEYS_READ);
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1", new Object[] {1, "v11"}),
                        kvRecordFactory.ofRecord("k1", null),
                        kvRecordFactory.ofRecord("k1", new Object[] {1, "v12"}),
                        kvRecordFactory.ofRecord("missing", null),
                        kvRecordFactory.ofRecord("missing", null),
                        kvRecordFactory.ofRecord("k2", new Object[] {2, "v22"}),
                        kvRecordFactory.ofRecord("k3", new Object[] {3, "v33"}),
                        kvRecordFactory.ofRecord("k3", null)),
                null);
        assertThat(statistics.getTickerCount(TickerType.NUMBER_MULTIGET_CALLS) - calls)
                .isEqualTo(batchOldValueLookupEnabled ? 1 : 0);
        assertThat(statistics.getTickerCount(TickerType.NUMBER_MULTIGET_KEYS_READ) - keys)
                .isEqualTo(batchOldValueLookupEnabled ? 2 : 0);
        // MultiGet re-reads the "missing" miss with a point read to tell it from a read error.
        assertThat(statistics.getTickerCount(TickerType.NUMBER_KEYS_READ) - gets)
                .isEqualTo(batchOldValueLookupEnabled ? 1 : 3);
        checkEqual(
                readLogRecords(offset),
                Collections.singletonList(
                        logRecords(
                                offset,
                                Arrays.asList(
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER,
                                        ChangeType.DELETE,
                                        ChangeType.INSERT,
                                        ChangeType.INSERT,
                                        ChangeType.UPDATE_BEFORE,
                                        ChangeType.UPDATE_AFTER,
                                        ChangeType.DELETE),
                                Arrays.asList(
                                        new Object[] {1, "v1"},
                                        new Object[] {1, "v11"},
                                        new Object[] {1, "v11"},
                                        new Object[] {1, "v12"},
                                        new Object[] {2, "v22"},
                                        new Object[] {3, "v3"},
                                        new Object[] {3, "v33"},
                                        new Object[] {3, "v33"}))));
    }

    @ParameterizedTest
    @CsvSource({"7, 1, 0", "8, 0, 1"})
    void testWalBatchLookupAfterStreamingUpserts(
            int batchSize, int expectedGets, int expectedMultiGetCalls) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("table.changelog.image", "WAL");
        initLogTabletAndKvTablet(DATA1_SCHEMA_PK, config);
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("new0", new Object[] {0, "old"}),
                        kvRecordFactory.ofRecord("k2", new Object[] {2, "v2"})),
                null);
        flushAndWait(kvTablet, Long.MAX_VALUE);
        List<KvRecord> records = new ArrayList<>();
        List<Object[]> expectedRows = new ArrayList<>();
        for (int i = 0; i < batchSize - 2; i++) {
            records.add(kvRecordFactory.ofRecord("new" + i, new Object[] {i, "new"}));
            expectedRows.add(new Object[] {i, "new"});
        }
        records.add(kvRecordFactory.ofRecord("new0", null));
        records.add(kvRecordFactory.ofRecord("k2", null));
        expectedRows.add(new Object[] {0, "new"});
        expectedRows.add(new Object[] {2, "v2"});
        List<ChangeType> expectedChanges =
                new ArrayList<>(Collections.nCopies(batchSize - 2, ChangeType.UPDATE_AFTER));
        expectedChanges.add(ChangeType.DELETE);
        expectedChanges.add(ChangeType.DELETE);
        long offset = logTablet.localLogEndOffset();
        Statistics statistics = kvTablet.getRocksDBKv().getStatistics();
        long gets = statistics.getTickerCount(TickerType.NUMBER_KEYS_READ);
        long calls = statistics.getTickerCount(TickerType.NUMBER_MULTIGET_CALLS);
        long keys = statistics.getTickerCount(TickerType.NUMBER_MULTIGET_KEYS_READ);
        kvTablet.putAsLeader(kvRecordBatchFactory.ofRecords(records), null);
        assertThat(statistics.getTickerCount(TickerType.NUMBER_KEYS_READ) - gets)
                .isEqualTo(expectedGets);
        assertThat(statistics.getTickerCount(TickerType.NUMBER_MULTIGET_CALLS) - calls)
                .isEqualTo(expectedMultiGetCalls);
        assertThat(statistics.getTickerCount(TickerType.NUMBER_MULTIGET_KEYS_READ) - keys)
                .isEqualTo(expectedMultiGetCalls);
        checkEqual(
                readLogRecords(offset),
                Collections.singletonList(logRecords(offset, expectedChanges, expectedRows)));
    }

    @Test
    void testSkipUnchangedUpsertsUsePrefetchedOldValues() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(ConfigOptions.TABLE_UPSERT_SKIP_UNCHANGED.key(), "true");
        initLogTabletAndKvTablet(DATA1_SCHEMA_PK, config);
        List<KvRecord> initialRecords = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            initialRecords.add(kvRecordFactory.ofRecord("k" + i, new Object[] {i, "v" + i}));
        }
        kvTablet.putAsLeader(kvRecordBatchFactory.ofRecords(initialRecords), null);
        flushAndWait(kvTablet, Long.MAX_VALUE);

        // Rewrite every flushed row unchanged except k1, then repeat k1's new value so the
        // second write is compared against the buffered value instead of the prefetched one.
        List<KvRecord> records = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            records.add(
                    kvRecordFactory.ofRecord(
                            "k" + i, new Object[] {i, i == 1 ? "changed" : "v" + i}));
        }
        records.add(kvRecordFactory.ofRecord("k1", new Object[] {1, "changed"}));
        long offset = logTablet.localLogEndOffset();
        Statistics statistics = kvTablet.getRocksDBKv().getStatistics();
        long calls = statistics.getTickerCount(TickerType.NUMBER_MULTIGET_CALLS);
        long keys = statistics.getTickerCount(TickerType.NUMBER_MULTIGET_KEYS_READ);
        kvTablet.putAsLeader(kvRecordBatchFactory.ofRecords(records), null);

        assertThat(statistics.getTickerCount(TickerType.NUMBER_MULTIGET_CALLS) - calls)
                .isEqualTo(1);
        assertThat(statistics.getTickerCount(TickerType.NUMBER_MULTIGET_KEYS_READ) - keys)
                .isEqualTo(8);
        checkEqual(
                readLogRecords(offset),
                Collections.singletonList(
                        logRecords(
                                offset,
                                Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER),
                                Arrays.asList(
                                        new Object[] {1, "v1"}, new Object[] {1, "changed"}))));
    }

    @Test
    void testPrefetchReadErrorFailsBatchAndRollsBackAppliedRecords() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("table.changelog.image", "WAL");
        initLogTabletAndKvTablet(DATA1_SCHEMA_PK, config);
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1", new Object[] {1, "v1"})),
                null);
        flushAndWait(kvTablet, Long.MAX_VALUE);
        try (FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
            kvTablet.getRocksDBKv().getDb().flush(options);
        }
        // Corrupt the data block before it is read, so the read is not served from the cache.
        try (Stream<Path> files = Files.walk(tmpKvDir.toPath())) {
            Path sst = files.filter(path -> path.toString().endsWith(".sst")).findFirst().get();
            try (RandomAccessFile file = new RandomAccessFile(sst.toFile(), "rw")) {
                int firstByte = file.readUnsignedByte();
                file.seek(0);
                file.writeByte(firstByte ^ 0xff);
            }
        }
        // Full-row WAL upserts do not read old values, so k2 stays buffered without a read.
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k2", new Object[] {2, "v2"})),
                null);

        // The buffered k2 upserts are applied before deleting the flushed k1 starts the
        // prefetch, whose read hits the corrupted block.
        List<KvRecord> records = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            records.add(kvRecordFactory.ofRecord("k2", new Object[] {2, "v2-" + i}));
        }
        records.add(kvRecordFactory.ofRecord("k1", null));
        long offset = logTablet.localLogEndOffset();
        assertThatThrownBy(
                        () -> kvTablet.putAsLeader(kvRecordBatchFactory.ofRecords(records), null))
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining("checksum");
        assertThat(logTablet.localLogEndOffset()).isEqualTo(offset);

        // Deleting k2 must emit its last committed value, not one from the failed batch.
        kvTablet.putAsLeader(
                kvRecordBatchFactory.ofRecords(kvRecordFactory.ofRecord("k2", null)), null);
        checkEqual(
                readLogRecords(offset),
                Collections.singletonList(
                        logRecords(
                                offset,
                                Collections.singletonList(ChangeType.DELETE),
                                Collections.singletonList(new Object[] {2, "v2"}))));
    }

    private LogRecords readLogRecords(long startOffset) throws Exception {
        return logTablet
                .read(startOffset, Integer.MAX_VALUE, FetchIsolation.LOG_END, false, null, null)
                .getRecords();
    }

    private MemoryLogRecords logRecords(long offset, List<ChangeType> changes, List<Object[]> rows)
            throws Exception {
        return createBasicMemoryLogRecords(
                rowType,
                DEFAULT_SCHEMA_ID,
                offset,
                -1L,
                CURRENT_LOG_MAGIC_VALUE,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                changes,
                rows,
                LogFormat.ARROW,
                DEFAULT_COMPRESSION);
    }

    private void checkEqual(LogRecords actual, List<MemoryLogRecords> expected) {
        LogTestBase.assertLogRecordsListEquals(expected, actual, rowType, schemaGetter);
    }
}
