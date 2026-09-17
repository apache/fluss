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

import org.apache.fluss.client.arrow.ArrowBatch;
import org.apache.fluss.client.arrow.ArrowBatchReader;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ArrowIpcBatch;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ChunkedAllocationManager;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.Projection;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.fluss.record.TestData.DATA2;
import static org.apache.fluss.record.TestData.DATA2_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA2_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.apache.fluss.record.TestData.DATA2_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA2_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.genLogFile;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentDir;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.client.table.scanner.log.RemoteCompletedFetch}. */
class RemoteArrowCompletedFetchTest {
    private LogScannerStatus logScannerStatus;
    private @TempDir File tempDir;

    @BeforeEach
    void beforeEach() {
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 0), 0L);
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 1), 0L);
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 2), 0L);
        logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);
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
                try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                    ArrowScanRecords records =
                            new ArrowScanRecords(
                                    Collections.singletonMap(
                                            tableBucket, fetch.fetchArrowBatches(100)));
                    int count = 2;
                    assertThat(records.count()).isEqualTo(DATA2.size() - count);
                    for (ArrowIpcBatch ipcBatch : records) {
                        try (ArrowBatch batch = ArrowBatchReader.read(ipcBatch, allocator)) {
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
                                                .isEqualTo(
                                                        DATA2.get(count)[selectedFields[column]]);
                                    }
                                }
                                count++;
                            }
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
