/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.compression.ArrowCompressionType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TimestampType;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocatorUtil;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.OutOfMemoryException;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.fluss.record.DefaultLogRecordBatch.APPEND_ONLY_FLAG_MASK;
import static org.apache.fluss.record.LogRecordBatchFormat.attributeOffset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Verifies fixed-column Arrow records and the lifetime of borrowed input and pooled output. */
class ArrowKafkaRecordTranscoderTest {
    private static final int MIB = 1024 * 1024;

    private final BufferAllocator allocator = BufferAllocatorUtil.createBufferAllocator(null);
    private final LazyMemorySegmentPool memorySegmentPool =
            LazyMemorySegmentPool.createServerBufferPool(new Configuration());
    private final ArrowKafkaRecordTranscoder transcoder =
            new ArrowKafkaRecordTranscoder(allocator, memorySegmentPool);

    @AfterEach
    void closeTranscoder() throws Exception {
        IOUtils.closeAll(transcoder, allocator, memorySegmentPool);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFixedColumnsWithAndWithoutSystemFields(boolean string) throws Exception {
        // The value looks like JSON but binary/string must not interpret or rescue its members.
        byte[] value = bytes("{\"_kafka_rescue\":\"消息\",\"_kafka_key\":123}");
        byte[] key = {(byte) 0xff, 0, (byte) 0xc3};
        Record record =
                record(
                        123L,
                        TimestampType.LOG_APPEND_TIME,
                        key,
                        value,
                        Arrays.asList(
                                new RecordHeader("duplicate", null),
                                new RecordHeader("nullable", bytes("discarded")),
                                new RecordHeader("duplicate", bytes("last")),
                                new RecordHeader("nullable", null),
                                new RecordHeader("", new byte[0])));
        for (boolean systemFields : new boolean[] {false, true}) {
            // Each distinct schema has its own schema ID, as required by native Fluss writes.
            TableInfo table =
                    table(
                            42L,
                            systemFields ? 1 : 0,
                            envelope(string, systemFields).toTableDescriptor());
            KafkaTableSchema contract = KafkaTableSchema.resolve(table.toTableDescriptor());
            read(
                    transcoder.transcode(7, Collections.singletonList(record), table),
                    table,
                    1,
                    row -> {
                        assertThat(row.getFieldCount())
                                .isEqualTo(table.getRowType().getFieldCount());
                        assertValue(row, contract.valueProjection().positionAt(0), string, value);
                        if (contract.keyPosition() >= 0) {
                            assertThat(row.getBytes(contract.keyPosition())).isEqualTo(key);
                        }
                        if (contract.headersPosition() >= 0) {
                            Map<String, byte[]> headers =
                                    headers(row.getMap(contract.headersPosition()));
                            assertThat(headers).hasSize(3).containsKey("nullable");
                            assertThat(headers.get("")).isEmpty();
                            assertThat(headers.get("duplicate")).isEqualTo(bytes("last"));
                            assertThat(headers.get("nullable")).isNull();
                        }
                        if (contract.metadataPosition() >= 0) {
                            assertMetadata(row, contract.metadataPosition(), 123L, 1, 7);
                        }
                        if (contract.rescuePosition() >= 0) {
                            assertThat(row.isNullAt(contract.rescuePosition())).isTrue();
                        }
                    });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testStringBytesOutliveBorrowedBuffer(boolean direct) throws Exception {
        byte[] text = bytes("消息-😀");
        byte[] expected = Arrays.copyOf(text, text.length + 2);
        // STRING preserves raw bytes, including malformed UTF-8.
        expected[text.length] = (byte) 0xc3;
        expected[text.length + 1] = 0x28;
        ByteBuf storage =
                direct
                        ? PooledByteBufAllocator.DEFAULT.directBuffer(expected.length + 2)
                        : PooledByteBufAllocator.DEFAULT.heapBuffer(expected.length + 2);
        TableInfo table = valueTable();
        EncodedRecords output;
        try {
            storage.writeByte(0xff).writeBytes(expected).writeByte(0xff);
            ByteBuffer source = storage.nioBuffer();
            source.position(1);
            source.limit(1 + expected.length);
            Record input =
                    new Record(
                            123L,
                            TimestampType.CREATE_TIME,
                            null,
                            source.asReadOnlyBuffer(),
                            Collections.emptyList());
            output = transcoder.transcode(0, Collections.singletonList(input), table);
            assertThat(source.position()).isEqualTo(1);
            assertThat(source.remaining()).isEqualTo(expected.length);
            storage.setZero(0, storage.readableBytes());
        } finally {
            storage.release();
        }
        read(output, table, 1, row -> assertThat(row.getString(0).toBytes()).isEqualTo(expected));
    }

    @Test
    void testSchemaChangeRebuildsFieldMapping() throws Exception {
        TableInfo original = envelope(false, true);
        List<Record> records =
                Collections.singletonList(
                        record(
                                123L,
                                TimestampType.CREATE_TIME,
                                bytes("key"),
                                bytes("value"),
                                Collections.singletonList(new RecordHeader("h", bytes("header")))));
        EncodedRecords originalOutput = transcoder.transcode(7, records, original);
        TableInfo changed =
                table(
                        original.getTableId(),
                        original.getSchemaId() + 1,
                        descriptor(
                                        Schema.newBuilder()
                                                .column("_kafka_value", DataTypes.BYTES())
                                                .column("_kafka_metadata", metadataType())
                                                .column("_kafka_key", DataTypes.BYTES())
                                                .column(
                                                        "_kafka_headers",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.BYTES()))
                                                .build(),
                                        false)
                                .build());

        read(
                transcoder.transcode(3, records, changed),
                changed,
                1,
                row -> {
                    assertThat(row.getFieldCount()).isEqualTo(4);
                    assertThat(row.getBytes(0)).isEqualTo(bytes("value"));
                    assertMetadata(row, 1, 123L, 0, 3);
                    assertThat(row.getBytes(2)).isEqualTo(bytes("key"));
                    assertThat(headers(row.getMap(3)).get("h")).isEqualTo(bytes("header"));
                });
        // An output produced with the earlier plan still carries its original schema and layout.
        read(
                originalOutput,
                original,
                1,
                row -> {
                    assertThat(row.getFieldCount()).isEqualTo(5);
                    assertThat(headers(row.getMap(0)).get("h")).isEqualTo(bytes("header"));
                    assertThat(row.isNullAt(1)).isTrue();
                    assertThat(row.getBytes(2)).isEqualTo(bytes("value"));
                    assertThat(row.getBytes(3)).isEqualTo(bytes("key"));
                    assertMetadata(row, 4, 123L, 0, 7);
                });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNullAndEmptyPayloadsRemainDistinct(boolean string) throws Exception {
        TableInfo table = envelope(string, true);
        List<Record> records =
                Arrays.asList(
                        record(Record.NO_TIMESTAMP, null, null, Collections.emptyList()),
                        record(0L, new byte[0], new byte[0], Collections.emptyList()));
        AtomicInteger next = new AtomicInteger();
        read(
                transcoder.transcode(0, records, table),
                table,
                2,
                row -> {
                    int value = table.getRowType().getFieldIndex("_kafka_value");
                    int key = table.getRowType().getFieldIndex("_kafka_key");
                    if (next.getAndIncrement() == 0) {
                        assertThat(row.isNullAt(value)).isTrue();
                        assertThat(row.isNullAt(key)).isTrue();
                    } else {
                        assertValue(row, value, string, new byte[0]);
                        assertThat(row.getBytes(key)).isEmpty();
                    }
                    assertThat(
                                    row.getMap(table.getRowType().getFieldIndex("_kafka_headers"))
                                            .size())
                            .isZero();
                    assertThat(row.isNullAt(table.getRowType().getFieldIndex("_kafka_rescue")))
                            .isTrue();
                });
    }

    @Test
    void testMetadataPreservesTimestampTypeAndRequestedPartition() throws Exception {
        long[] timestamps = {Record.NO_TIMESTAMP, 0L, Long.MAX_VALUE};
        TimestampType[] types = {
            TimestampType.UNKNOWN, TimestampType.CREATE_TIME, TimestampType.LOG_APPEND_TIME
        };
        List<Record> records = new ArrayList<>();
        for (TimestampType type : types) {
            for (long timestamp : timestamps) {
                records.add(record(timestamp, type, null, bytes("v"), Collections.emptyList()));
            }
        }
        TableInfo table = envelope(false, true);
        for (int partition : new int[] {7, 3}) {
            AtomicInteger next = new AtomicInteger();
            read(
                    transcoder.transcode(partition, records, table),
                    table,
                    records.size(),
                    row -> {
                        int i = next.getAndIncrement();
                        assertMetadata(
                                row,
                                4,
                                timestamps[i % timestamps.length],
                                i / timestamps.length - 1,
                                partition);
                    });
        }
    }

    @Test
    void testRejectsInvalidRequestBeforeAllocatingResources() {
        List<Record> records =
                Collections.singletonList(record(1L, null, bytes("v"), Collections.emptyList()));
        assertThatThrownBy(() -> transcoder.transcode(-1, records, valueTable()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition");
        assertThatThrownBy(() -> transcoder.transcode(0, Collections.emptyList(), valueTable()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
        TableInfo invalid =
                table(
                        descriptor(
                                        Schema.newBuilder()
                                                .column("body", DataTypes.STRING())
                                                .build(),
                                        true)
                                .build());
        assertThatThrownBy(() -> transcoder.transcode(0, records, invalid))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("_kafka_value");
        TableInfo invalidId = table(42L, Short.MAX_VALUE + 1, valueTable().toTableDescriptor());
        assertThatThrownBy(() -> transcoder.transcode(0, records, invalidId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schemaId");
        assertThat(transcoder.writerPool().freeWriters()).isEmpty();
        assertThat(allocator.getAllocatedMemory()).isZero();
        assertThat(memorySegmentPool.usedPages()).isZero();
    }

    @Test
    void testConvertsNullJsonContainer() throws Exception {
        TableInfo table =
                table(
                        descriptor(
                                        Schema.newBuilder()
                                                .column("body", DataTypes.ARRAY(DataTypes.INT()))
                                                .build(),
                                        true)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                        KafkaValueConversionType.PARSE_JSON)
                                .build());
        read(
                transcode(table, (byte[]) null),
                table,
                1,
                row -> assertThat(row.isNullAt(0)).isTrue());
    }

    @Test
    void testInputIterationFailureRecyclesWriter() throws Exception {
        IllegalStateException expected =
                new IllegalStateException("input failed after the first record");
        TableInfo table = binaryTable(1, 1, ArrowCompressionType.NONE);
        List<Record> records =
                new AbstractList<Record>() {
                    @Override
                    public Record get(int index) {
                        if (index == 0) {
                            return record(1L, null, bytes("valid"), Collections.emptyList());
                        }
                        throw expected;
                    }

                    @Override
                    public int size() {
                        return 2;
                    }
                };
        Throwable failure = catchThrowable(() -> transcoder.transcode(0, records, table));
        assertThat(failure).isInstanceOf(FlussRuntimeException.class).hasCause(expected);
        assertThat(failure.getSuppressed()).isEmpty();
        assertTranscoderRemainsUsable(table);
    }

    @ParameterizedTest
    @EnumSource(
            value = ArrowCompressionType.class,
            names = {"NONE", "ZSTD"})
    void testLargeRecordsStayInOneBatch(ArrowCompressionType compression) throws Exception {
        byte[] value = new byte[17 * MIB];
        new Random(42).nextBytes(value);
        TableInfo table = binaryTable(1, 1, compression);
        byte[] lastValue = {7};
        try (EncodedRecords output = transcode(table, value, lastValue)) {
            assertThat(output.records().sizeInBytes()).isGreaterThan(16 * MIB);
            assertRecords(output.records(), table, value, lastValue);
        }
    }

    @Test
    void testAllocationFailureRecyclesWriter() throws Exception {
        allocator.setLimit(128 * 1024);
        TableInfo table = binaryTable(1, 1, ArrowCompressionType.NONE);
        Throwable failure = catchThrowable(() -> transcode(table, new byte[MIB]));
        assertThat(failure)
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("memory limit exceeded")
                .hasCauseInstanceOf(OutOfMemoryException.class);
        assertThat(failure.getSuppressed()).isEmpty();
        assertTranscoderRemainsUsable(table);
    }

    @Test
    void testWritersAreSeparatedByTableSchemaAndCompression() throws Exception {
        TableInfo original = binaryTable(1, 1, ArrowCompressionType.NONE);
        try (EncodedRecords output = transcode(original, new byte[] {1})) {
            assertRecords(output.records(), original, new byte[] {1});
        }
        ArrowWriter originalWriter = idleWriter(transcoder);
        for (TableInfo changed :
                Arrays.asList(
                        binaryTable(2, 1, ArrowCompressionType.NONE),
                        binaryTable(1, 2, ArrowCompressionType.NONE),
                        binaryTable(1, 1, ArrowCompressionType.LZ4_FRAME))) {
            try (EncodedRecords output = transcode(changed, new byte[] {2})) {
                assertRecords(output.records(), changed, new byte[] {2});
            }
        }
        assertThat(transcoder.writerPool().freeWriters()).hasSize(4);
        long epoch = originalWriter.getEpoch();
        try (EncodedRecords output = transcode(original, new byte[] {3})) {
            assertRecords(output.records(), original, new byte[] {3});
        }
        assertThat(originalWriter.getEpoch()).isEqualTo(epoch + 1);
    }

    @Test
    void testConcurrentBatchesContinueAfterTranscoderCloses() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch finish = new CountDownLatch(1);
        TableInfo table = binaryTable(1, 1, ArrowCompressionType.NONE);
        List<Future<EncodedRecords>> encodes = new ArrayList<>();
        try {
            for (byte value = 1; value <= 2; value++) {
                byte[] payload = {value};
                encodes.add(
                        executor.submit(() -> transcodeBlocked(table, payload, entered, finish)));
            }
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(allocator.getAllocatedMemory()).isPositive();
            executor.submit(transcoder::close).get(10, TimeUnit.SECONDS);
            allocator.assertOpen();
            assertThatThrownBy(() -> transcode(table, new byte[] {3}))
                    .isInstanceOf(FlussRuntimeException.class)
                    .cause()
                    .isInstanceOf(FlussRuntimeException.class)
                    .hasMessageContaining("pool closed");
            finish.countDown();
            for (int i = 0; i < encodes.size(); i++) {
                try (EncodedRecords output = encodes.get(i).get(10, TimeUnit.SECONDS)) {
                    assertRecords(
                            output.records(), table, new byte[] {(byte) (i + 1)}, new byte[0]);
                }
            }
            assertThat(transcoder.writerPool().freeWriters()).isEmpty();
            assertThat(allocator.getAllocatedMemory()).isZero();
            assertThat(memorySegmentPool.usedPages()).isZero();
        } finally {
            finish.countDown();
            try {
                for (Future<EncodedRecords> encode : encodes) {
                    try {
                        encode.get(10, TimeUnit.SECONDS).deallocate();
                    } catch (ExecutionException ignored) {
                        // Failed conversions release their resources before completing the future.
                    }
                }
            } finally {
                executor.shutdownNow();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1024, 1024 * 1024})
    void testPooledOutputRemainsValidUntilDeallocated(int pageSize) throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.SERVER_BUFFER_PAGE_SIZE, new MemorySize(pageSize));
        conf.set(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE, new MemorySize(4 * MIB));
        conf.set(ConfigOptions.SERVER_BUFFER_PER_REQUEST_MEMORY_SIZE, new MemorySize(pageSize));
        try (LazyMemorySegmentPool pool = LazyMemorySegmentPool.createServerBufferPool(conf)) {
            TableInfo table = binaryTable(1, 1, ArrowCompressionType.NONE);
            byte[] value = new byte[32 * 1024];
            new Random(42).nextBytes(value);
            try (ArrowKafkaRecordTranscoder encoder =
                            new ArrowKafkaRecordTranscoder(allocator, pool);
                    EncodedRecords output = encoder.transcode(0, records(value), table)) {
                MemoryLogRecords records = output.records();
                int pagesInUse = pool.usedPages();
                assertThat(pagesInUse).isPositive();
                ArrowWriter writer = idleWriter(encoder);
                long epoch = writer.getEpoch();
                try (EncodedRecords next = encoder.transcode(0, records(new byte[] {7}), table)) {
                    assertRecords(next.records(), table, new byte[] {7});
                }
                assertThat(pool.usedPages()).isEqualTo(pagesInUse);
                assertThat(idleWriter(encoder)).isSameAs(writer);
                assertThat(writer.getEpoch()).isEqualTo(epoch + 1);
                assertThat(allocator.getAllocatedMemory()).isZero();
                for (MemorySegment segment : pool.getAllCachePages()) {
                    Arrays.fill(segment.getArray(), (byte) -1);
                }
                encoder.close();
                assertRecords(records, table, value);
                output.deallocate();
                output.deallocate();
                assertThat(pool.availableMemory()).isEqualTo(pool.totalSize());
                assertThatThrownBy(output::records).isInstanceOf(IllegalStateException.class);
                if (records.sizeInBytes() <= pageSize) {
                    // Single-page results point directly into the pool, without a copy.
                    assertThat(pool.getAllCachePages())
                            .anySatisfy(
                                    segment ->
                                            assertThat(segment.getArray())
                                                    .isSameAs(
                                                            records.getMemorySegment().getArray()));
                }
            }
            allocator.assertOpen();
            MemorySegment segment = pool.nextSegment();
            pool.returnPage(segment);
        }
    }

    @Test
    void testMemoryPoolFailureReturnsWriter() {
        memorySegmentPool.close();
        assertThatThrownBy(
                        () ->
                                transcode(
                                        binaryTable(1, 1, ArrowCompressionType.NONE),
                                        new byte[] {1}))
                .isInstanceOf(FlussRuntimeException.class)
                .cause()
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Memory segment pool closed");
        assertThat(idleWriter(transcoder)).isNotNull();
        assertThat(allocator.getAllocatedMemory()).isZero();
    }

    private void assertTranscoderRemainsUsable(TableInfo table) throws Exception {
        assertThat(allocator.getAllocatedMemory()).isZero();
        assertThat(memorySegmentPool.usedPages()).isZero();
        ArrowWriter writer = idleWriter(transcoder);
        try (EncodedRecords output = transcode(table, bytes("next"))) {
            assertRecords(output.records(), table, bytes("next"));
        }
        assertThat(idleWriter(transcoder)).isSameAs(writer);
    }

    private EncodedRecords transcode(TableInfo table, byte[]... values) throws Exception {
        return transcoder.transcode(0, records(values), table);
    }

    private static List<Record> records(byte[]... values) {
        List<Record> records = new ArrayList<>();
        for (byte[] value : values) {
            records.add(record(Record.NO_TIMESTAMP, null, value, Collections.emptyList()));
        }
        return records;
    }

    private EncodedRecords transcodeBlocked(
            TableInfo table, byte[] value, CountDownLatch entered, CountDownLatch finish)
            throws Exception {
        List<Record> records =
                new AbstractList<Record>() {
                    @Override
                    public Record get(int index) {
                        if (index == 0) {
                            return record(1L, null, value, Collections.emptyList());
                        }
                        entered.countDown();
                        try {
                            assertThat(finish.await(10, TimeUnit.SECONDS)).isTrue();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new FlussRuntimeException(e);
                        }
                        return record(1L, null, new byte[0], Collections.emptyList());
                    }

                    @Override
                    public int size() {
                        return 2;
                    }
                };
        return transcoder.transcode(0, records, table);
    }

    private static ArrowWriter idleWriter(ArrowKafkaRecordTranscoder transcoder) {
        assertThat(transcoder.writerPool().freeWriters()).hasSize(1);
        Deque<ArrowWriter> writers =
                transcoder.writerPool().freeWriters().values().iterator().next();
        assertThat(writers).hasSize(1);
        return writers.getFirst();
    }

    private static TableInfo binaryTable(
            long tableId, int schemaId, ArrowCompressionType compression) {
        return table(
                tableId,
                schemaId,
                descriptor(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.BYTES())
                                        .build(),
                                false)
                        .property(ConfigOptions.TABLE_LOG_ARROW_COMPRESSION_TYPE, compression)
                        .build());
    }

    private static TableInfo valueTable() {
        return envelope(true, false);
    }

    private static TableInfo envelope(boolean string, boolean systemFields) {
        Schema.Builder schema = Schema.newBuilder();
        if (systemFields) {
            schema.column("_kafka_headers", DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()));
            schema.column("_kafka_rescue", DataTypes.STRING());
        }
        schema.column("_kafka_value", valueType(string));
        if (systemFields) {
            schema.column("_kafka_key", DataTypes.BYTES());
            schema.column("_kafka_metadata", metadataType());
        }
        return table(descriptor(schema.build(), string).build());
    }

    private static DataType metadataType() {
        return DataTypes.ROW(
                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                DataTypes.FIELD("partition", DataTypes.INT()));
    }

    private static DataType valueType(boolean string) {
        return string ? DataTypes.STRING() : DataTypes.BYTES();
    }

    private static TableDescriptor.Builder descriptor(Schema schema, boolean string) {
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(8)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(
                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                        string ? KafkaValueConversionType.STRING : KafkaValueConversionType.BINARY);
    }

    private static TableInfo table(TableDescriptor descriptor) {
        return table(42L, 3, descriptor);
    }

    private static TableInfo table(long tableId, int schemaId, TableDescriptor descriptor) {
        return TableInfo.of(
                TablePath.of("fluss", "topic"), tableId, schemaId, descriptor, null, 1L, 1L);
    }

    private static Map<String, byte[]> headers(InternalMap map) {
        Map<String, byte[]> result = new LinkedHashMap<>();
        for (int i = 0; i < map.size(); i++) {
            assertThat(map.keyArray().isNullAt(i)).isFalse();
            String key = map.keyArray().getString(i).toString();
            assertThat(result).doesNotContainKey(key);
            result.put(key, map.valueArray().isNullAt(i) ? null : map.valueArray().getBytes(i));
        }
        return result;
    }

    private static void assertValue(InternalRow row, int position, boolean string, byte[] value) {
        if (string) {
            assertThat(row.getString(position).toBytes()).isEqualTo(value);
        } else {
            assertThat(row.getBytes(position)).isEqualTo(value);
        }
    }

    private static void assertMetadata(
            InternalRow row, int position, long timestamp, int timestampType, int partition) {
        assertThat(row.isNullAt(position)).isFalse();
        InternalRow metadata = row.getRow(position, 3);
        assertThat(metadata.getFieldCount()).isEqualTo(3);
        if (timestamp == Record.NO_TIMESTAMP) {
            assertThat(metadata.isNullAt(0)).isTrue();
        } else {
            assertThat(metadata.isNullAt(0)).isFalse();
            assertThat(metadata.getTimestampLtz(0, 3).getEpochMillisecond()).isEqualTo(timestamp);
        }
        assertThat(metadata.isNullAt(1)).isFalse();
        assertThat(metadata.getInt(1)).isEqualTo(timestampType);
        assertThat(metadata.isNullAt(2)).isFalse();
        assertThat(metadata.getInt(2)).isEqualTo(partition);
    }

    private static void read(
            EncodedRecords encoded, TableInfo table, int count, Consumer<InternalRow> verify)
            throws Exception {
        try (EncodedRecords result = encoded) {
            readRecords(result.records(), table, count, verify);
        }
    }

    private static void assertRecords(MemoryLogRecords output, TableInfo table, byte[]... expected)
            throws Exception {
        AtomicInteger next = new AtomicInteger();
        readRecords(
                output,
                table,
                expected.length,
                row -> assertThat(row.getBytes(0)).isEqualTo(expected[next.getAndIncrement()]));
    }

    private static void readRecords(
            MemoryLogRecords output, TableInfo table, int count, Consumer<InternalRow> verify)
            throws Exception {
        assertThat(output.batches()).hasSize(1);
        LogRecordBatch batch = output.batches().iterator().next();
        batch.ensureValid();
        assertThat(batch.schemaId()).isEqualTo((short) table.getSchemaId());
        assertThat(batch.getRecordCount()).isEqualTo(count);
        byte attributes =
                output.getMemorySegment()
                        .get(output.getPosition() + attributeOffset(batch.magic()));
        assertThat(attributes & APPEND_ONLY_FLAG_MASK).isEqualTo(APPEND_ONLY_FLAG_MASK);
        try (LogRecordReadContext context =
                        LogRecordReadContext.createArrowReadContext(
                                table.getRowType(),
                                table.getSchemaId(),
                                new TestingSchemaGetter(
                                        new SchemaInfo(table.getSchema(), table.getSchemaId())));
                CloseableIterator<LogRecord> records = batch.records(context)) {
            for (int i = 0; i < count; i++) {
                assertThat(records.hasNext()).isTrue();
                verify.accept(records.next().getRow());
            }
            assertThat(records.hasNext()).isFalse();
        }
    }

    private static Record record(
            long timestamp,
            @Nullable byte[] key,
            @Nullable byte[] value,
            List<RecordHeader> headers) {
        return record(timestamp, TimestampType.UNKNOWN, key, value, headers);
    }

    private static Record record(
            long timestamp,
            TimestampType timestampType,
            @Nullable byte[] key,
            @Nullable byte[] value,
            List<RecordHeader> headers) {
        return new Record(timestamp, timestampType, buffer(key), buffer(value), headers);
    }

    private static @Nullable ByteBuffer buffer(@Nullable byte[] value) {
        return value == null ? null : ByteBuffer.wrap(value);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
