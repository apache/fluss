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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ChunkedAllocationManager;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.Projection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.record.TestData.DATA2;
import static org.apache.fluss.record.TestData.DATA2_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA2_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.apache.fluss.record.TestData.DATA2_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA2_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA2_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.genLogFile;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentDir;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.client.table.scanner.log.RemoteCompletedFetch}. */
class RemoteCompletedFetchTest {
    private LogScannerStatus logScannerStatus;
    private LogRecordReadContext remoteReadContext;
    private @TempDir File tempDir;
    private TableInfo tableInfo;
    private SchemaGetter schemaGetter;

    @BeforeEach
    void beforeEach() {
        tableInfo = DATA2_TABLE_INFO;
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 0), 0L);
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 1), 0L);
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 2), 0L);
        logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);
        schemaGetter = new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA2_SCHEMA);
        remoteReadContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA2_ROW_TYPE, DEFAULT_SCHEMA_ID, schemaGetter);
    }

    @AfterEach
    void afterEach() {
        if (remoteReadContext != null) {
            remoteReadContext.close();
            remoteReadContext = null;
        }
    }

    @Test
    void testSimple() throws Exception {
        long fetchOffset = 0L;
        TableBucket tableBucket = new TableBucket(DATA2_TABLE_ID, 0);
        AtomicBoolean recycleCalled = new AtomicBoolean(false);
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        tableBucket, DATA2_PHYSICAL_TABLE_PATH, DATA2, LogFormat.ARROW);
        RemoteCompletedFetch completedFetch =
                makeCompletedFetch(
                        tableBucket,
                        fileLogRecords,
                        fetchOffset,
                        null,
                        () -> recycleCalled.set(true));

        List<ScanRecord> scanRecords = completedFetch.fetchRecords(8);
        assertThat(scanRecords.size()).isEqualTo(8);
        assertThat(scanRecords.get(0).logOffset()).isEqualTo(0L);

        assertThat(recycleCalled.get()).isFalse();
        scanRecords = completedFetch.fetchRecords(8);
        assertThat(scanRecords.size()).isEqualTo(2);
        assertThat(scanRecords.get(0).logOffset()).isEqualTo(8L);
        // when read finish, the file channel should be closed.
        assertThat(fileLogRecords.channel().isOpen()).isFalse();
        // and recycle should be called.
        assertThat(recycleCalled.get()).isTrue();

        // no more records can be read
        scanRecords = completedFetch.fetchRecords(8);
        assertThat(scanRecords.size()).isEqualTo(0);
    }

    @Test
    void testFetchForPartitionTable() throws Exception {
        long fetchOffset = 0L;
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, (long) 0, 0);
        AtomicBoolean recycleCalled = new AtomicBoolean(false);
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        tb,
                        PhysicalTablePath.of(DATA2_TABLE_PATH, "20240904"),
                        DATA2,
                        LogFormat.ARROW);
        RemoteCompletedFetch completedFetch =
                makeCompletedFetch(
                        tb, fileLogRecords, fetchOffset, null, () -> recycleCalled.set(true));

        List<ScanRecord> scanRecords = completedFetch.fetchRecords(8);
        assertThat(scanRecords.size()).isEqualTo(8);
        assertThat(scanRecords.get(0).logOffset()).isEqualTo(0L);

        assertThat(recycleCalled.get()).isFalse();
        scanRecords = completedFetch.fetchRecords(8);
        assertThat(scanRecords.size()).isEqualTo(2);
        assertThat(scanRecords.get(0).logOffset()).isEqualTo(8L);
        // when read finish, the file channel should be closed.
        assertThat(fileLogRecords.channel().isOpen()).isFalse();
        // and recycle should be called.
        assertThat(recycleCalled.get()).isTrue();

        // no more records can be read
        scanRecords = completedFetch.fetchRecords(8);
        assertThat(scanRecords.size()).isEqualTo(0);
    }

    @Test
    void testNegativeFetchCount() throws Exception {
        long fetchOffset = 0L;
        TableBucket tableBucket = new TableBucket(DATA2_TABLE_ID, 0);
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        tableBucket, DATA2_PHYSICAL_TABLE_PATH, DATA2, LogFormat.ARROW);
        RemoteCompletedFetch completedFetch =
                makeCompletedFetch(tableBucket, fileLogRecords, fetchOffset, null);

        List<ScanRecord> scanRecords = completedFetch.fetchRecords(-10);
        assertThat(scanRecords.size()).isEqualTo(0);
    }

    @Test
    void testNoRecordsInFetch() throws Exception {
        long fetchOffset = 0L;
        TableBucket tableBucket = new TableBucket(DATA2_TABLE_ID, 0);
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        tableBucket,
                        DATA2_PHYSICAL_TABLE_PATH,
                        Collections.emptyList(),
                        LogFormat.ARROW);
        RemoteCompletedFetch completedFetch =
                makeCompletedFetch(tableBucket, fileLogRecords, fetchOffset, null);

        List<ScanRecord> scanRecords = completedFetch.fetchRecords(10);
        assertThat(scanRecords.size()).isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEXED", "ARROW"})
    void testProjection(String format) throws Exception {
        LogFormat logFormat = LogFormat.fromString(format);
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .withComment("a is first column")
                        .column("b", DataTypes.STRING())
                        .withComment("b is second column")
                        .column("c", DataTypes.STRING())
                        .withComment("c is adding column")
                        .build();
        tableInfo =
                TableInfo.of(
                        DATA2_TABLE_PATH,
                        DATA2_TABLE_ID,
                        1,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(3)
                                .logFormat(logFormat)
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        long fetchOffset = 0L;
        TableBucket tableBucket = new TableBucket(DATA2_TABLE_ID, 0);
        FileLogRecords fileLogRecords =
                createFileLogRecords(tableBucket, DATA2_PHYSICAL_TABLE_PATH, DATA2, logFormat);
        RemoteCompletedFetch completedFetch =
                makeCompletedFetch(
                        tableBucket, fileLogRecords, fetchOffset, Projection.of(new int[] {0, 2}));

        List<ScanRecord> scanRecords = completedFetch.fetchRecords(8);
        List<Object[]> expectedObjects =
                Arrays.asList(
                        new Object[] {1, "hello"},
                        new Object[] {2, "hi"},
                        new Object[] {3, "nihao"},
                        new Object[] {4, "hello world"},
                        new Object[] {5, "hi world"},
                        new Object[] {6, "nihao world"},
                        new Object[] {7, "hello world2"},
                        new Object[] {8, "hi world2"});
        assertThat(scanRecords.size()).isEqualTo(8);
        for (int i = 0; i < scanRecords.size(); i++) {
            Object[] expectObject = expectedObjects.get(i);
            ScanRecord actualRecord = scanRecords.get(i);
            assertThat(actualRecord.logOffset()).isEqualTo(i);
            assertThat(actualRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
            InternalRow row = actualRecord.getRow();
            assertThat(row.getInt(0)).isEqualTo(expectObject[0]);
            assertThat(row.getString(1).toString()).isEqualTo(expectObject[1]);
        }

        completedFetch =
                makeCompletedFetch(
                        tableBucket, fileLogRecords, fetchOffset, Projection.of(new int[] {2, 0}));
        scanRecords = completedFetch.fetchRecords(8);
        assertThat(scanRecords.size()).isEqualTo(8);
        for (int i = 0; i < scanRecords.size(); i++) {
            Object[] expectObject = expectedObjects.get(i);
            ScanRecord actualRecord = scanRecords.get(i);
            assertThat(actualRecord.logOffset()).isEqualTo(i);
            assertThat(actualRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
            InternalRow row = actualRecord.getRow();
            assertThat(row.getString(0).toString()).isEqualTo(expectObject[1]);
            assertThat(row.getInt(1)).isEqualTo(expectObject[0]);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testArrowProjection(boolean schemaEvolution) throws Exception {
        Schema schema =
                schemaEvolution
                        ? Schema.newBuilder()
                                .fromColumns(DATA2_SCHEMA.getColumns())
                                .column("d", DataTypes.STRING())
                                .build()
                        : DATA2_SCHEMA;
        int schemaId = schemaEvolution ? DEFAULT_SCHEMA_ID + 1 : DEFAULT_SCHEMA_ID;
        TestingSchemaGetter schemas = new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA2_SCHEMA);
        schemas.updateLatestSchemaInfo(new SchemaInfo(schema, schemaId));
        TableInfo targetTableInfo =
                TableInfo.of(
                        DATA2_TABLE_PATH,
                        DATA2_TABLE_ID,
                        schemaId,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(3)
                                .logFormat(LogFormat.ARROW)
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        int[][] projections =
                schemaEvolution ? new int[][] {{3}, {3, 2, 1}} : new int[][] {{2}, {2, 1}};
        TableBucket tableBucket = new TableBucket(DATA2_TABLE_ID, 0);
        for (int[] selectedFields : projections) {
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createReadContext(
                            targetTableInfo,
                            true,
                            LogRecordReadContext.SchemaResolution.TARGET,
                            Projection.of(selectedFields),
                            schemas,
                            new ChunkedAllocationManager.ChunkedFactory())) {
                FileLogRecords fileLogRecords =
                        createFileLogRecords(
                                tableBucket, DATA2_PHYSICAL_TABLE_PATH, DATA2, LogFormat.ARROW);
                RemoteCompletedFetch fetch =
                        new RemoteCompletedFetch(
                                tableBucket,
                                DATA2_TABLE_PATH,
                                fileLogRecords,
                                DATA2.size(),
                                readContext,
                                logScannerStatus,
                                true,
                                2L,
                                () -> {});
                try (ArrowScanRecords records =
                        new ArrowScanRecords(
                                Collections.singletonMap(
                                        tableBucket, fetch.fetchArrowBatches(100)))) {
                    int count = 2;
                    assertThat(records.count()).isEqualTo(DATA2.size() - count);
                    for (ArrowBatchData batch : records) {
                        assertThat(batch.getVectorSchemaRoot().getSchema().getFields())
                                .extracting(field -> field.getName())
                                .containsExactlyElementsOf(
                                        schema.getRowType()
                                                .project(selectedFields)
                                                .getFieldNames());
                        for (int rowId = 0; rowId < batch.getRecordCount(); rowId++) {
                            assertThat(batch.getBaseLogOffset() + rowId).isEqualTo(count);
                            assertThat(batch.getChangeType(rowId))
                                    .isEqualTo(ChangeType.APPEND_ONLY);
                            for (int column = 0; column < selectedFields.length; column++) {
                                Object value =
                                        batch.getVectorSchemaRoot()
                                                .getVector(column)
                                                .getObject(rowId);
                                if (selectedFields[column] == 3) {
                                    assertThat(value).isNull();
                                } else {
                                    assertThat(value.toString())
                                            .isEqualTo(DATA2.get(count)[selectedFields[column]]);
                                }
                            }
                            count++;
                        }
                    }
                    assertThat(count).isEqualTo(DATA2.size());
                } finally {
                    fetch.drain();
                }
            }
        }
    }

    private FileLogRecords createFileLogRecords(
            TableBucket tableBucket,
            PhysicalTablePath physicalTablePath,
            List<Object[]> objects,
            LogFormat logFormat)
            throws Exception {
        UUID segmentId = UUID.randomUUID();
        RemoteLogSegment remoteLogSegment =
                RemoteLogSegment.Builder.builder()
                        .tableBucket(tableBucket)
                        .physicalTablePath(physicalTablePath)
                        .remoteLogSegmentId(segmentId)
                        .remoteLogStartOffset(0L)
                        .remoteLogEndOffset(9L)
                        .segmentSizeInBytes(Integer.MAX_VALUE)
                        .build();
        File logFile =
                genRemoteLogSegmentFile(
                        DATA2_ROW_TYPE, tempDir, remoteLogSegment, objects, 0L, logFormat);
        return FileLogRecords.open(logFile, false);
    }

    private RemoteCompletedFetch makeCompletedFetch(
            TableBucket tableBucket,
            FileLogRecords fileLogRecords,
            long fetchOffset,
            @Nullable Projection projection,
            Runnable recycle) {
        return new RemoteCompletedFetch(
                tableBucket,
                DATA2_TABLE_PATH,
                fileLogRecords,
                10L,
                LogRecordReadContext.createReadContext(
                        tableInfo,
                        true,
                        LogRecordReadContext.SchemaResolution.DYNAMIC,
                        projection,
                        schemaGetter,
                        new ChunkedAllocationManager.ChunkedFactory()),
                logScannerStatus,
                true,
                fetchOffset,
                recycle);
    }

    private RemoteCompletedFetch makeCompletedFetch(
            TableBucket tableBucket,
            FileLogRecords fileLogRecords,
            long fetchOffset,
            @Nullable Projection projection) {
        return makeCompletedFetch(tableBucket, fileLogRecords, fetchOffset, projection, () -> {});
    }

    private static File genRemoteLogSegmentFile(
            RowType rowType,
            File remoteLogTabletDir,
            RemoteLogSegment remoteLogSegment,
            List<Object[]> objects,
            long baseOffset,
            LogFormat logFormat)
            throws Exception {
        FsPath remoteLogSegmentDir =
                remoteLogSegmentDir(
                        new FsPath(remoteLogTabletDir.getAbsolutePath()),
                        remoteLogSegment.remoteLogSegmentId());
        return genLogFile(
                rowType, new File(remoteLogSegmentDir.toString()), objects, baseOffset, logFormat);
    }
}
