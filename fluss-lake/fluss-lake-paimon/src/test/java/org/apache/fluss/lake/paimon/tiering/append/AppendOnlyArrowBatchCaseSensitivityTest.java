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

package org.apache.fluss.lake.paimon.tiering.append;

import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.UnshadedArrowReadUtils;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.FileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests case-sensitive Arrow batch mapping for append-only Paimon tables. */
class AppendOnlyArrowBatchCaseSensitivityTest {

    private RootAllocator allocator;
    private FileStoreTable fileStoreTable;
    private TableWriteImpl<InternalRow> tableWrite;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void beforeEach() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        fileStoreTable = mock(FileStoreTable.class);
        tableWrite = mock(TableWriteImpl.class);
        FileStore<?> fileStore = mock(FileStore.class);
        when(fileStore.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        doReturn(fileStore).when(fileStoreTable).store();
    }

    @AfterEach
    void afterEach() {
        allocator.close();
    }

    @ParameterizedTest(name = "legacyTable = {0}")
    @ValueSource(booleans = {true, false})
    void testMixedCaseColumnNames(boolean legacyTable) throws Exception {
        RowType flussRowType =
                RowType.builder()
                        .field("event_key", DataTypes.STRING())
                        .field("eventId", DataTypes.STRING())
                        .field("sentAtMs", DataTypes.BIGINT())
                        .build();
        org.apache.paimon.types.RowType paimonRowType =
                paimonRowType(
                        legacyTable,
                        new String[] {"event_key", "eventId", "sentAtMs"},
                        org.apache.paimon.types.DataTypes.STRING(),
                        org.apache.paimon.types.DataTypes.STRING(),
                        org.apache.paimon.types.DataTypes.BIGINT());

        try (VectorSchemaRoot root = createRoot(flussRowType)) {
            VarCharVector eventKey = (VarCharVector) root.getVector(0);
            VarCharVector eventId = (VarCharVector) root.getVector(1);
            BigIntVector sentAtMs = (BigIntVector) root.getVector(2);
            eventKey.allocateNew(1);
            eventId.allocateNew(1);
            sentAtMs.allocateNew(1);
            eventKey.setSafe(0, "k".getBytes(StandardCharsets.UTF_8));
            eventId.setSafe(0, "e".getBytes(StandardCharsets.UTF_8));
            sentAtMs.set(0, 1000L);
            setSingleRow(root);

            assertThat(writeAndRead(root, paimonRowType, legacyTable))
                    .containsExactly("k", "e", 1000L);
        }
    }

    @ParameterizedTest(name = "legacyTable = {0}")
    @ValueSource(booleans = {true, false})
    void testColumnNamesDifferingOnlyInCase(boolean legacyTable) throws Exception {
        RowType flussRowType =
                RowType.builder().field("v", DataTypes.INT()).field("V", DataTypes.INT()).build();
        org.apache.paimon.types.RowType paimonRowType =
                paimonRowType(
                        legacyTable,
                        new String[] {"v", "V"},
                        org.apache.paimon.types.DataTypes.INT(),
                        org.apache.paimon.types.DataTypes.INT());

        try (VectorSchemaRoot root = createRoot(flussRowType)) {
            IntVector lower = (IntVector) root.getVector(0);
            IntVector upper = (IntVector) root.getVector(1);
            lower.allocateNew(1);
            upper.allocateNew(1);
            lower.set(0, 1);
            upper.set(0, 100);
            setSingleRow(root);

            assertThat(writeAndRead(root, paimonRowType, legacyTable)).containsExactly(1, 100);
        }
    }

    private VectorSchemaRoot createRoot(RowType rowType) {
        return VectorSchemaRoot.create(UnshadedArrowReadUtils.toArrowSchema(rowType), allocator);
    }

    private Object[] writeAndRead(
            VectorSchemaRoot root,
            org.apache.paimon.types.RowType paimonRowType,
            boolean legacyTable)
            throws Exception {
        try (AppendOnlyArrowBatchHelper helper =
                        new AppendOnlyArrowBatchHelper(
                                fileStoreTable, tableWrite, paimonRowType, 0, legacyTable);
                ArrowBatchData batch =
                        new ArrowBatchData(root.slice(0, root.getRowCount()), 0L, 1L, 1)) {
            helper.writeArrowBatch(batch, BinaryRow.EMPTY_ROW, false);

            ArgumentCaptor<BundleRecords> captor = ArgumentCaptor.forClass(BundleRecords.class);
            verify(tableWrite).writeBundle(eq(BinaryRow.EMPTY_ROW), eq(0), captor.capture());
            Iterator<InternalRow> rows = captor.getValue().iterator();
            assertThat(rows.hasNext()).isTrue();
            InternalRow row = rows.next();
            int fieldCount = root.getFieldVectors().size();
            Object[] values = new Object[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                values[i] = getFieldValue(row, i, paimonRowType.getTypeAt(i));
            }
            return values;
        }
    }

    private org.apache.paimon.types.RowType paimonRowType(
            boolean legacyTable, String[] fieldNames, DataType... fieldTypes) {
        org.apache.paimon.types.RowType.Builder builder =
                org.apache.paimon.types.RowType.builder().fields(fieldTypes, fieldNames);
        if (legacyTable) {
            builder.field(BUCKET_COLUMN_NAME, org.apache.paimon.types.DataTypes.INT());
            builder.field(OFFSET_COLUMN_NAME, org.apache.paimon.types.DataTypes.BIGINT());
            builder.field(
                    TIMESTAMP_COLUMN_NAME,
                    org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS());
        }
        return builder.build();
    }

    private void setSingleRow(VectorSchemaRoot root) {
        root.getFieldVectors().forEach(vector -> vector.setValueCount(1));
        root.setRowCount(1);
    }

    private Object getFieldValue(InternalRow row, int pos, DataType type) {
        switch (type.getTypeRoot()) {
            case INTEGER:
                return row.getInt(pos);
            case BIGINT:
                return row.getLong(pos);
            case CHAR:
            case VARCHAR:
                return row.getString(pos).toString();
            default:
                throw new UnsupportedOperationException("Unexpected type " + type);
        }
    }
}
