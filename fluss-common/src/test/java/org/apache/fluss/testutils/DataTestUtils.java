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

package org.apache.fluss.testutils;

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.DefaultLogRecordBatch;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.MemoryLogRecordsIndexedBuilder;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.BytesUtils;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.types.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.LogRecordReadContext.createArrowReadContext;
import static org.apache.fluss.record.TestData.BASE_OFFSET;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_KEY_TYPE;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DEFAULT_MAGIC;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.LogRecordBatchAssert.assertThatLogRecordBatch;
import static org.apache.fluss.utils.FlussPaths.remoteLogDir;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentDir;
import static org.apache.fluss.utils.FlussPaths.remoteLogTabletDir;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Utils for data related test. like create {@link IndexedRow} and create {@link MemoryLogRecords}.
 */
public class DataTestUtils {

    public static GenericRow row(Object... objects) {
        GenericRow row = new GenericRow(objects.length);
        for (int i = 0; i < objects.length; i++) {
            if (objects[i] instanceof String) {
                row.setField(i, BinaryString.fromString((String) objects[i]));
            } else {
                row.setField(i, objects[i]);
            }
        }
        return row;
    }

    public static CompactedRow compactedRow(RowType rowType, Object[] objects) {
        return genCompacted(rowType, objects);
    }

    public static IndexedRow indexedRow(RowType rowType, Object[] objects) {
        return genIndexed(rowType, objects);
    }

    /**
     * Input objects with key and value, this method will generate an indexed row only contains key.
     */
    public static IndexedRow keyRow(Schema schema, Object[] objects) {
        int[] pkIndex = schema.getPrimaryKeyIndexes();
        RowType rowType = schema.getRowType();
        return genIndexed(rowType, objects).projectRow(pkIndex);
    }

    private static IndexedRow genIndexed(RowType rowType, Object[] data) {
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

    public static MemoryLogRecords genMemoryLogRecordsByObject(byte magic, List<Object[]> objects)
            throws Exception {
        return createRecordsWithoutBaseLogOffset(
                DATA1_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                0,
                System.currentTimeMillis(),
                magic,
                objects,
                LogFormat.ARROW);
    }

    public static MemoryLogRecords genMemoryLogRecordsByObject(List<Object[]> objects)
            throws Exception {
        return genMemoryLogRecordsByObject(CURRENT_LOG_MAGIC_VALUE, objects);
    }

    public static MemoryLogRecords genMemoryLogRecordsWithWriterId(
            List<Object[]> objects, long writerId, int batchSequence, long baseOffset)
            throws Exception {
        List<ChangeType> changeTypes =
                objects.stream().map(row -> ChangeType.APPEND_ONLY).collect(Collectors.toList());
        return createBasicMemoryLogRecords(
                DATA1_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                baseOffset,
                System.currentTimeMillis(),
                CURRENT_LOG_MAGIC_VALUE,
                writerId,
                batchSequence,
                changeTypes,
                objects,
                LogFormat.ARROW,
                DEFAULT_COMPRESSION);
    }

    public static MemoryLogRecords genIndexedMemoryLogRecords(List<IndexedRow> rows)
            throws Exception {
        List<ChangeType> changeTypes =
                rows.stream().map(row -> ChangeType.APPEND_ONLY).collect(Collectors.toList());
        return createIndexedMemoryLogRecords(
                BASE_OFFSET,
                System.currentTimeMillis(),
                DEFAULT_SCHEMA_ID,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                changeTypes,
                rows);
    }

    public static MemoryLogRecords genMemoryLogRecordsWithBaseOffset(
            long offsetBase, List<Object[]> objects) throws Exception {
        return createRecordsWithoutBaseLogOffset(
                DATA1_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                offsetBase,
                -1L,
                CURRENT_LOG_MAGIC_VALUE,
                objects,
                LogFormat.ARROW);
    }

    public static MemoryLogRecords genLogRecordsWithBaseOffsetAndTimestamp(
            long offsetBase, long maxTimestamp, List<Object[]> objects) throws Exception {
        return createRecordsWithoutBaseLogOffset(
                DATA1_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                offsetBase,
                maxTimestamp,
                CURRENT_LOG_MAGIC_VALUE,
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
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(keyType);
        KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID);
        KvRecordTestUtils.KvRecordFactory kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(valueType);
        List<KvRecord> records = new ArrayList<>();
        for (Tuple2<Object[], Object[]> keyAndValue : keyAndValues) {
            records.add(
                    kvRecordFactory.ofRecord(
                            keyEncoder.encodeKey(row(keyAndValue.f0)), keyAndValue.f1));
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
                        CURRENT_LOG_MAGIC_VALUE,
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
            byte magic,
            List<Object[]> objects,
            LogFormat logFormat)
            throws Exception {
        List<ChangeType> changeTypes =
                objects.stream().map(row -> ChangeType.APPEND_ONLY).collect(Collectors.toList());
        return createBasicMemoryLogRecords(
                rowType,
                schemaId,
                offsetBase,
                maxTimestamp,
                magic,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                changeTypes,
                objects,
                logFormat,
                DEFAULT_COMPRESSION);
    }

    public static MemoryLogRecords createBasicMemoryLogRecords(
            RowType rowType,
            int schemaId,
            long offsetBase,
            long maxTimestamp,
            byte magic,
            long writerId,
            int batchSequence,
            List<ChangeType> changeTypes,
            List<Object[]> objects,
            LogFormat logFormat,
            ArrowCompressionInfo arrowCompressionInfo)
            throws Exception {
        return createMemoryLogRecords(
                rowType,
                schemaId,
                offsetBase,
                maxTimestamp,
                magic,
                writerId,
                batchSequence,
                changeTypes,
                objects,
                logFormat,
                arrowCompressionInfo);
    }

    public static MemoryLogRecords createMemoryLogRecords(
            RowType rowType,
            int schemaId,
            long offsetBase,
            long maxTimestamp,
            byte magic,
            long writerId,
            int batchSequence,
            List<ChangeType> changeTypes,
            List<Object[]> objects,
            LogFormat logFormat,
            ArrowCompressionInfo arrowCompressionInfo)
            throws Exception {
        if (logFormat == LogFormat.ARROW) {
            List<InternalRow> rows =
                    objects.stream().map(DataTestUtils::row).collect(Collectors.toList());
            return createArrowMemoryLogRecords(
                    rowType,
                    offsetBase,
                    maxTimestamp,
                    magic,
                    schemaId,
                    writerId,
                    batchSequence,
                    changeTypes,
                    rows,
                    arrowCompressionInfo);
        } else {
            return createIndexedMemoryLogRecords(
                    offsetBase,
                    maxTimestamp,
                    schemaId,
                    writerId,
                    batchSequence,
                    changeTypes,
                    objects.stream()
                            .map(object -> indexedRow(rowType, object))
                            .collect(Collectors.toList()));
        }
    }

    private static MemoryLogRecords createIndexedMemoryLogRecords(
            long baseLogOffset,
            long maxTimestamp,
            int schemaId,
            long writerId,
            int batchSequence,
            List<ChangeType> changeTypes,
            List<IndexedRow> rows)
            throws Exception {
        UnmanagedPagedOutputView outputView = new UnmanagedPagedOutputView(100);
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        baseLogOffset, schemaId, Integer.MAX_VALUE, DEFAULT_MAGIC, outputView);
        for (int i = 0; i < changeTypes.size(); i++) {
            builder.append(changeTypes.get(i), rows.get(i));
        }
        builder.setWriterState(writerId, batchSequence);
        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        memoryLogRecords.ensureValid(DEFAULT_MAGIC);

        ((DefaultLogRecordBatch) memoryLogRecords.batches().iterator().next())
                .setCommitTimestamp(maxTimestamp);
        builder.close();
        return memoryLogRecords;
    }

    private static MemoryLogRecords createArrowMemoryLogRecords(
            RowType rowType,
            long baseLogOffset,
            long maxTimestamp,
            byte magic,
            int schemaId,
            long writerId,
            int batchSequence,
            List<ChangeType> changeTypes,
            List<InternalRow> rows,
            ArrowCompressionInfo arrowCompressionInfo)
            throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowWriterPool provider = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    provider.getOrCreateWriter(
                            1L, schemaId, Integer.MAX_VALUE, rowType, arrowCompressionInfo);
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            baseLogOffset,
                            magic,
                            schemaId,
                            writer,
                            new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024)));
            for (int i = 0; i < changeTypes.size(); i++) {
                builder.append(changeTypes.get(i), rows.get(i));
            }
            builder.setWriterState(writerId, batchSequence);
            builder.close();
            MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());

            ((DefaultLogRecordBatch) memoryLogRecords.batches().iterator().next())
                    .setCommitTimestamp(maxTimestamp);
            memoryLogRecords.ensureValid(magic);
            return memoryLogRecords;
        }
    }

    public static void assertMemoryRecordsEquals(
            RowType rowType, LogRecords records, List<List<Object[]>> expected) {
        List<List<Tuple2<ChangeType, Object[]>>> appendOnlyExpectedValue = new ArrayList<>();
        for (List<Object[]> expectedRecord : expected) {
            List<Tuple2<ChangeType, Object[]>> expectedFieldAndRowKind =
                    expectedRecord.stream()
                            .map(val -> Tuple2.of(ChangeType.APPEND_ONLY, val))
                            .collect(Collectors.toList());
            appendOnlyExpectedValue.add(expectedFieldAndRowKind);
        }
        assertMemoryRecordsEqualsWithRowKind(rowType, records, appendOnlyExpectedValue);
    }

    public static void assertMemoryRecordsEqualsWithRowKind(
            RowType rowType,
            LogRecords records,
            List<List<Tuple2<ChangeType, Object[]>>> expected) {
        Iterator<LogRecordBatch> iterator = records.batches().iterator();
        for (List<Tuple2<ChangeType, Object[]>> expectedRecord : expected) {
            assertThat(iterator.hasNext()).isTrue();
            LogRecordBatch batch = iterator.next();
            try (LogRecordReadContext readContext =
                            createArrowReadContext(rowType, DEFAULT_SCHEMA_ID);
                    CloseableIterator<LogRecord> logIterator = batch.records(readContext)) {
                for (Tuple2<ChangeType, Object[]> expectedFieldAndRowKind : expectedRecord) {
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
            List<Tuple2<ChangeType, Object[]>> expected) {
        try (LogRecordReadContext readContext = createArrowReadContext(rowType, DEFAULT_SCHEMA_ID);
                CloseableIterator<LogRecord> logIterator = logRecordBatch.records(readContext)) {
            for (Tuple2<ChangeType, Object[]> expectedFieldAndRowKind : expected) {
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
            Tuple2<ChangeType, Object[]> expectedFieldAndRowKind) {
        DataType[] dataTypes = rowType.getChildren().toArray(new DataType[0]);
        InternalRow.FieldGetter[] fieldGetter = new InternalRow.FieldGetter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            fieldGetter[i] = InternalRow.createFieldGetter(dataTypes[i], i);
        }
        assertThat(logRecord.getChangeType()).isEqualTo(expectedFieldAndRowKind.f0);
        assertRowValueEquals(
                fieldGetter, dataTypes, logRecord.getRow(), expectedFieldAndRowKind.f1);
    }

    public static void assertLogRecordsEquals(
            RowType rowType, LogRecords logRecords, List<Object[]> expectedValue) {
        List<Tuple2<ChangeType, Object[]>> expectedValueWithRowKind =
                expectedValue.stream()
                        .map(val -> Tuple2.of(ChangeType.APPEND_ONLY, val))
                        .collect(Collectors.toList());
        assertLogRecordsEqualsWithRowKind(rowType, logRecords, expectedValueWithRowKind);
    }

    public static void assertLogRecordsEqualsWithRowKind(
            RowType rowType,
            LogRecords logRecords,
            List<Tuple2<ChangeType, Object[]>> expectedValue) {
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
                        Tuple2<ChangeType, Object[]> expected = expectedValue.get(i++);
                        assertThat(record.getChangeType()).isEqualTo(expected.f0);
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
