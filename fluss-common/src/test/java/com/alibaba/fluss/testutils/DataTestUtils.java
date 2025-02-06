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

package com.alibaba.fluss.testutils;

import com.alibaba.fluss.compression.ArrowCompressionInfo;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.memory.ManagedPagedOutputView;
import com.alibaba.fluss.memory.TestingMemorySegmentPool;
import com.alibaba.fluss.memory.UnmanagedPagedOutputView;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.DefaultLogRecordBatch;
import com.alibaba.fluss.record.FileLogRecords;
import com.alibaba.fluss.record.KvRecord;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.KvRecordTestUtils;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.MemoryLogRecordsArrowBuilder;
import com.alibaba.fluss.record.MemoryLogRecordsIndexedBuilder;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.arrow.ArrowWriter;
import com.alibaba.fluss.row.arrow.ArrowWriterPool;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.encode.CompactedRowEncoder;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.row.encode.RowEncoder;
import com.alibaba.fluss.row.encode.ValueEncoder;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.BytesUtils;
import com.alibaba.fluss.utils.CloseableIterator;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.types.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.LogRecordBatch.NO_BATCH_SEQUENCE;
import static com.alibaba.fluss.record.LogRecordBatch.NO_WRITER_ID;
import static com.alibaba.fluss.record.LogRecordReadContext.createArrowReadContext;
import static com.alibaba.fluss.record.TestData.BASE_OFFSET;
import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_KEY_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA_PK;
import static com.alibaba.fluss.record.TestData.DEFAULT_MAGIC;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.testutils.LogRecordBatchAssert.assertThatLogRecordBatch;
import static com.alibaba.fluss.utils.FlussPaths.remoteLogDir;
import static com.alibaba.fluss.utils.FlussPaths.remoteLogSegmentDir;
import static com.alibaba.fluss.utils.FlussPaths.remoteLogTabletDir;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Utils for data related test. like create {@link IndexedRow} and create {@link MemoryLogRecords}.
 */
public class DataTestUtils {
    public static CompactedRow compactedRow(RowType rowType, Object[] objects) {
        return genCompacted(rowType, objects);
    }

    /**
     * Input objects with key and value, this method will generate an indexed row only contains key.
     */
    public static IndexedRow keyRow(Schema schema, Object[] objects) {
        int[] pkIndex = schema.getPrimaryKeyIndexes();
        RowType rowType = schema.getRowType();
        return row(rowType, objects).projectRow(pkIndex);
    }

    public static IndexedRow row(RowType rowType, Object[] data) {
        DataType[] dataTypes = rowType.getChildren().toArray(new DataType[0]);
        assertThat(dataTypes.length).isEqualTo(data.length);
        RowEncoder rowEncoder = RowEncoder.create(KvFormat.INDEXED, dataTypes);
        rowEncoder.startNewRow();
        for (int i = 0; i < dataTypes.length; i++) {
            rowEncoder.encodeField(
                    i,
                    data[i] instanceof String
                            ? BinaryString.fromString((String) data[i])
                            : data[i]);
        }
        return (IndexedRow) rowEncoder.finishRow();
    }

    private static CompactedRow genCompacted(RowType rowType, Object[] data) {
        DataType[] dataTypes = rowType.getChildren().toArray(new DataType[0]);
        assertThat(dataTypes.length).isEqualTo(data.length);
        RowEncoder rowEncoder = new CompactedRowEncoder(dataTypes);
        rowEncoder.startNewRow();
        for (int i = 0; i < dataTypes.length; i++) {
            rowEncoder.encodeField(
                    i,
                    data[i] instanceof String
                            ? BinaryString.fromString((String) data[i])
                            : data[i]);
        }
        return (CompactedRow) rowEncoder.finishRow();
    }

    public static MemoryLogRecords genMemoryLogRecordsByObject(List<Object[]> objects)
            throws Exception {
        return createRecordsWithoutBaseLogOffset(
                DATA1_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                0,
                System.currentTimeMillis(),
                objects,
                LogFormat.ARROW);
    }

    public static MemoryLogRecords genMemoryLogRecordsWithWriterId(
            List<Object[]> objects, long writerId, int batchSequence, long baseOffset)
            throws Exception {
        List<RowKind> rowKinds =
                objects.stream().map(row -> RowKind.APPEND_ONLY).collect(Collectors.toList());
        return createBasicMemoryLogRecords(
                DATA1_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                baseOffset,
                System.currentTimeMillis(),
                writerId,
                batchSequence,
                rowKinds,
                objects,
                LogFormat.ARROW);
    }

    public static MemoryLogRecords genIndexedMemoryLogRecords(List<IndexedRow> rows)
            throws Exception {
        List<RowKind> rowKinds =
                rows.stream().map(row -> RowKind.APPEND_ONLY).collect(Collectors.toList());
        return createIndexedMemoryLogRecords(
                BASE_OFFSET,
                System.currentTimeMillis(),
                DEFAULT_SCHEMA_ID,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                rowKinds,
                rows);
    }

    public static MemoryLogRecords genMemoryLogRecordsWithBaseOffset(
            long offsetBase, List<Object[]> objects) throws Exception {
        return createRecordsWithoutBaseLogOffset(
                DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, offsetBase, -1L, objects, LogFormat.ARROW);
    }

    public static MemoryLogRecords genLogRecordsWithBaseOffsetAndTimestamp(
            long offsetBase, long maxTimestamp, List<Object[]> objects) throws Exception {
        return createRecordsWithoutBaseLogOffset(
                DATA1_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                offsetBase,
                maxTimestamp,
                objects,
                LogFormat.ARROW);
    }

    public static KvRecordBatch genKvRecordBatch(List<Tuple2<Object[], Object[]>> keyAndValues)
            throws Exception {
        return genKvRecordBatch(DATA1_KEY_TYPE, DATA1_ROW_TYPE, keyAndValues);
    }

    public static KvRecordBatch genKvRecordBatch(
            RowType keyType, RowType valueType, List<Tuple2<Object[], Object[]>> keyAndValues)
            throws Exception {
        return genKvRecordBatchWithWriterId(
                keyAndValues, keyType, valueType, NO_WRITER_ID, NO_BATCH_SEQUENCE);
    }

    public static KvRecordBatch genKvRecordBatchWithWriterId(
            List<Tuple2<Object[], Object[]>> keyAndValues,
            RowType keyType,
            RowType valueType,
            long writerId,
            int batchSequence)
            throws Exception {
        KeyEncoder keyEncoder = new KeyEncoder(keyType);
        KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID);
        KvRecordTestUtils.KvRecordFactory kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(valueType);
        List<KvRecord> records = new ArrayList<>();
        for (Tuple2<Object[], Object[]> keyAndValue : keyAndValues) {
            records.add(
                    kvRecordFactory.ofRecord(
                            keyEncoder.encode(row(keyType, keyAndValue.f0)), keyAndValue.f1));
        }
        return kvRecordBatchFactory.ofRecords(records, writerId, batchSequence);
    }

    @SafeVarargs
    public static KvRecordBatch genKvRecordBatch(Tuple2<String, Object[]>... keyAndValues)
            throws Exception {
        KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID);
        List<KvRecord> records = genKvRecords(keyAndValues);
        return kvRecordBatchFactory.ofRecords(records);
    }

    /**
     * Generate a KvRecord batch from the values only, whose key will be extracted from the value.
     */
    public static KvRecordBatch genKvRecordBatch(Object[]... values) throws Exception {
        KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID);
        return kvRecordBatchFactory.ofRecords(genKvRecords(values));
    }

    public static KvRecordBatch toKvRecordBatch(List<KvRecord> records) throws Exception {
        KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID);
        return kvRecordBatchFactory.ofRecords(records);
    }

    @SafeVarargs
    public static List<KvRecord> genKvRecords(Tuple2<String, Object[]>... keyAndValues) {
        KvRecordTestUtils.KvRecordFactory kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(DATA1_ROW_TYPE);
        List<KvRecord> records = new ArrayList<>();
        for (Tuple2<String, Object[]> keyAndValue : keyAndValues) {
            records.add(kvRecordFactory.ofRecord(keyAndValue.f0, keyAndValue.f1));
        }
        return records;
    }

    public static void genRemoteLogSegmentFile(
            TableBucket tableBucket,
            PhysicalTablePath physicalTablePath,
            Configuration conf,
            RemoteLogSegment remoteLogSegment,
            long baseOffset)
            throws Exception {
        FsPath remoteLogTabletDir =
                remoteLogTabletDir(remoteLogDir(conf), physicalTablePath, tableBucket);
        FsPath remoteLogSegmentDir =
                remoteLogSegmentDir(remoteLogTabletDir, remoteLogSegment.remoteLogSegmentId());
        genLogFile(
                DATA1_ROW_TYPE,
                new File(remoteLogSegmentDir.toString()),
                DATA1,
                baseOffset,
                LogFormat.ARROW);
    }

    public static File genLogFile(
            RowType rowType,
            File segmentDir,
            List<Object[]> objects,
            long baseOffset,
            LogFormat logFormat)
            throws Exception {
        if (!segmentDir.exists()) {
            segmentDir.mkdirs();
        }

        File logFile = FlussPaths.logFile(segmentDir, baseOffset);
        FileLogRecords fileLogRecords = FileLogRecords.open(logFile, false, 1024 * 1024, false);
        fileLogRecords.append(
                createRecordsWithoutBaseLogOffset(
                        rowType,
                        DEFAULT_SCHEMA_ID,
                        baseOffset,
                        System.currentTimeMillis(),
                        objects,
                        logFormat));
        fileLogRecords.flush();
        fileLogRecords.close();
        return logFile;
    }

    /**
     * Generate kv records for the values whose key will be extracted from the Row constructed by
     * the values.
     */
    public static List<KvRecord> genKvRecords(Object[]... values) {
        KvRecordTestUtils.PKBasedKvRecordFactory kvRecordFactory =
                KvRecordTestUtils.PKBasedKvRecordFactory.of(
                        DATA1_SCHEMA_PK.getRowType(), DATA1_SCHEMA_PK.getPrimaryKeyIndexes());
        List<KvRecord> records = new ArrayList<>();
        for (Object[] value : values) {
            records.add(kvRecordFactory.ofRecord(value));
        }
        return records;
    }

    public static List<Tuple2<byte[], byte[]>> getKeyValuePairs(List<KvRecord> kvRecords) {
        return getKeyValuePairs(kvRecords.toArray(new KvRecord[0]));
    }

    public static List<Tuple2<byte[], byte[]>> getKeyValuePairs(KvRecord... kvRecords) {
        List<Tuple2<byte[], byte[]>> keyValuePairs = new ArrayList<>();
        for (KvRecord kvRecord : kvRecords) {
            keyValuePairs.add(
                    Tuple2.of(
                            BytesUtils.toArray(kvRecord.getKey()),
                            ValueEncoder.encodeValue(DEFAULT_SCHEMA_ID, kvRecord.getRow())));
        }
        return keyValuePairs;
    }

    public static MemoryLogRecords createRecordsWithoutBaseLogOffset(
            RowType rowType,
            int schemaId,
            long offsetBase,
            long maxTimestamp,
            List<Object[]> objects,
            LogFormat logFormat)
            throws Exception {
        List<RowKind> rowKinds =
                objects.stream().map(row -> RowKind.APPEND_ONLY).collect(Collectors.toList());
        return createBasicMemoryLogRecords(
                rowType,
                schemaId,
                offsetBase,
                maxTimestamp,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                rowKinds,
                objects,
                logFormat);
    }

    public static MemoryLogRecords createBasicMemoryLogRecords(
            RowType rowType,
            int schemaId,
            long offsetBase,
            long maxTimestamp,
            long writerId,
            int batchSequence,
            List<RowKind> rowKinds,
            List<Object[]> objects,
            LogFormat logFormat)
            throws Exception {
        return createMemoryLogRecords(
                rowType,
                schemaId,
                offsetBase,
                maxTimestamp,
                writerId,
                batchSequence,
                rowKinds,
                objects,
                logFormat);
    }

    public static MemoryLogRecords createMemoryLogRecords(
            RowType rowType,
            int schemaId,
            long offsetBase,
            long maxTimestamp,
            long writerId,
            int batchSequence,
            List<RowKind> rowKinds,
            List<Object[]> objects,
            LogFormat logFormat)
            throws Exception {
        if (logFormat == LogFormat.ARROW) {
            List<InternalRow> rows =
                    objects.stream()
                            .map(object -> row(rowType, object))
                            .collect(Collectors.toList());
            return createArrowMemoryLogRecords(
                    rowType,
                    offsetBase,
                    maxTimestamp,
                    schemaId,
                    writerId,
                    batchSequence,
                    rowKinds,
                    rows);
        } else {
            return createIndexedMemoryLogRecords(
                    offsetBase,
                    maxTimestamp,
                    schemaId,
                    writerId,
                    batchSequence,
                    rowKinds,
                    objects.stream()
                            .map(object -> row(rowType, object))
                            .collect(Collectors.toList()));
        }
    }

    private static MemoryLogRecords createIndexedMemoryLogRecords(
            long baseLogOffset,
            long maxTimestamp,
            int schemaId,
            long writerId,
            int batchSequence,
            List<RowKind> rowKinds,
            List<IndexedRow> rows)
            throws Exception {
        UnmanagedPagedOutputView outputView = new UnmanagedPagedOutputView(100);
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        baseLogOffset, schemaId, Integer.MAX_VALUE, DEFAULT_MAGIC, outputView);
        for (int i = 0; i < rowKinds.size(); i++) {
            builder.append(rowKinds.get(i), rows.get(i));
        }
        builder.setWriterState(writerId, batchSequence);
        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        memoryLogRecords.ensureValid();

        ((DefaultLogRecordBatch) memoryLogRecords.batches().iterator().next())
                .setCommitTimestamp(maxTimestamp);
        builder.close();
        return memoryLogRecords;
    }

    private static MemoryLogRecords createArrowMemoryLogRecords(
            RowType rowType,
            long baseLogOffset,
            long maxTimestamp,
            int schemaId,
            long writerId,
            int batchSequence,
            List<RowKind> rowKinds,
            List<InternalRow> rows)
            throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowWriterPool provider = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    provider.getOrCreateWriter(
                            1L,
                            schemaId,
                            Integer.MAX_VALUE,
                            rowType,
                            ArrowCompressionInfo.NO_COMPRESSION);
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            baseLogOffset,
                            schemaId,
                            writer,
                            new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024)));
            for (int i = 0; i < rowKinds.size(); i++) {
                builder.append(rowKinds.get(i), rows.get(i));
            }
            builder.setWriterState(writerId, batchSequence);
            builder.close();
            MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());

            ((DefaultLogRecordBatch) memoryLogRecords.batches().iterator().next())
                    .setCommitTimestamp(maxTimestamp);
            memoryLogRecords.ensureValid();
            return memoryLogRecords;
        }
    }

    public static void assertMemoryRecordsEquals(
            RowType rowType, LogRecords records, List<List<Object[]>> expected) {
        List<List<Tuple2<RowKind, Object[]>>> appendOnlyExpectedValue = new ArrayList<>();
        for (List<Object[]> expectedRecord : expected) {
            List<Tuple2<RowKind, Object[]>> expectedFieldAndRowKind =
                    expectedRecord.stream()
                            .map(val -> Tuple2.of(RowKind.APPEND_ONLY, val))
                            .collect(Collectors.toList());
            appendOnlyExpectedValue.add(expectedFieldAndRowKind);
        }
        assertMemoryRecordsEqualsWithRowKind(rowType, records, appendOnlyExpectedValue);
    }

    public static void assertMemoryRecordsEqualsWithRowKind(
            RowType rowType, LogRecords records, List<List<Tuple2<RowKind, Object[]>>> expected) {
        Iterator<LogRecordBatch> iterator = records.batches().iterator();
        for (List<Tuple2<RowKind, Object[]>> expectedRecord : expected) {
            assertThat(iterator.hasNext()).isTrue();
            LogRecordBatch batch = iterator.next();
            try (LogRecordReadContext readContext =
                            createArrowReadContext(rowType, DEFAULT_SCHEMA_ID);
                    CloseableIterator<LogRecord> logIterator = batch.records(readContext)) {
                for (Tuple2<RowKind, Object[]> expectedFieldAndRowKind : expectedRecord) {
                    assertThat(logIterator.hasNext()).isTrue();
                    assertLogRecordsEqualsWithRowKind(
                            rowType, logIterator.next(), expectedFieldAndRowKind);
                }
                assertThat(logIterator.hasNext()).isFalse();
            }
        }
        assertThat(iterator.hasNext()).isFalse();
    }

    public static void assertLogRecordBatchEqualsWithRowKind(
            RowType rowType,
            LogRecordBatch logRecordBatch,
            List<Tuple2<RowKind, Object[]>> expected) {
        try (LogRecordReadContext readContext = createArrowReadContext(rowType, DEFAULT_SCHEMA_ID);
                CloseableIterator<LogRecord> logIterator = logRecordBatch.records(readContext)) {
            for (Tuple2<RowKind, Object[]> expectedFieldAndRowKind : expected) {
                assertThat(logIterator.hasNext()).isTrue();
                assertLogRecordsEqualsWithRowKind(
                        rowType, logIterator.next(), expectedFieldAndRowKind);
            }
            assertThat(logIterator.hasNext()).isFalse();
        }
    }

    public static void assertLogRecordsEquals(LogRecords actual, LogRecords expected) {
        assertLogRecordsEquals(DATA1_ROW_TYPE, actual, expected);
    }

    public static void assertLogRecordsEquals(
            RowType rowType, LogRecords actual, LogRecords expected) {
        Iterator<LogRecordBatch> actualIterator = actual.batches().iterator();
        Iterator<LogRecordBatch> expectedIterator = expected.batches().iterator();

        while (actualIterator.hasNext()) {
            assertThat(expectedIterator.hasNext()).isTrue();
            LogRecordBatch actualBatch = actualIterator.next();
            LogRecordBatch expectedBatch = expectedIterator.next();
            assertLogRecordBatchEquals(rowType, actualBatch, expectedBatch);
        }
        assertThat(expectedIterator.hasNext()).isFalse();
    }

    public static void assertLogRecordBatchEquals(
            RowType rowType, LogRecordBatch actual, LogRecordBatch expected) {
        assertThatLogRecordBatch(actual).withSchema(rowType).isEqualTo(expected);
    }

    private static void assertLogRecordsEqualsWithRowKind(
            RowType rowType,
            LogRecord logRecord,
            Tuple2<RowKind, Object[]> expectedFieldAndRowKind) {
        DataType[] dataTypes = rowType.getChildren().toArray(new DataType[0]);
        InternalRow.FieldGetter[] fieldGetter = new InternalRow.FieldGetter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            fieldGetter[i] = InternalRow.createFieldGetter(dataTypes[i], i);
        }
        assertThat(logRecord.getRowKind()).isEqualTo(expectedFieldAndRowKind.f0);
        assertRowValueEquals(
                fieldGetter, dataTypes, logRecord.getRow(), expectedFieldAndRowKind.f1);
    }

    public static void assertLogRecordsEquals(
            RowType rowType, LogRecords logRecords, List<Object[]> expectedValue) {
        List<Tuple2<RowKind, Object[]>> expectedValueWithRowKind =
                expectedValue.stream()
                        .map(val -> Tuple2.of(RowKind.APPEND_ONLY, val))
                        .collect(Collectors.toList());
        assertLogRecordsEqualsWithRowKind(rowType, logRecords, expectedValueWithRowKind);
    }

    public static void assertLogRecordsEqualsWithRowKind(
            RowType rowType, LogRecords logRecords, List<Tuple2<RowKind, Object[]>> expectedValue) {
        DataType[] dataTypes = rowType.getChildren().toArray(new DataType[0]);
        InternalRow.FieldGetter[] fieldGetter = new InternalRow.FieldGetter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            fieldGetter[i] = InternalRow.createFieldGetter(dataTypes[i], i);
        }

        int i = 0;
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(rowType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch batch : logRecords.batches()) {
                try (CloseableIterator<LogRecord> iterator = batch.records(readContext)) {
                    while (iterator.hasNext()) {
                        LogRecord record = iterator.next();
                        Tuple2<RowKind, Object[]> expected = expectedValue.get(i++);
                        assertThat(record.getRowKind()).isEqualTo(expected.f0);
                        assertRowValueEquals(fieldGetter, dataTypes, record.getRow(), expected.f1);
                    }
                }
            }
            assertThat(i).isEqualTo(expectedValue.size());
        }
    }

    public static void assertRowValueEquals(RowType rowType, InternalRow row, Object[] expectVal) {
        DataType[] dataTypes = rowType.getChildren().toArray(new DataType[0]);
        InternalRow.FieldGetter[] fieldGetter = new InternalRow.FieldGetter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            fieldGetter[i] = InternalRow.createFieldGetter(dataTypes[i], i);
        }

        assertRowValueEquals(fieldGetter, dataTypes, row, expectVal);
    }

    private static void assertRowValueEquals(
            InternalRow.FieldGetter[] fieldGetter,
            DataType[] dataTypes,
            InternalRow row,
            Object[] expectVal) {
        for (int i = 0; i < dataTypes.length; i++) {
            Object field = fieldGetter[i].getFieldOrNull(row);
            if (field != null) {
                if (dataTypes[i].getTypeRoot() == DataTypeRoot.STRING) {
                    assertThat(field).isEqualTo(BinaryString.fromString((String) expectVal[i]));
                } else {
                    assertThat(field).isEqualTo(expectVal[i]);
                }
            } else {
                assertThat(expectVal[i]).isNull();
            }
        }
    }
}
