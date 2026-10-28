/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.record;

import org.apache.fluss.compression.ArrowCompressionFactory;
import org.apache.fluss.compression.ArrowCompressionType;
import org.apache.fluss.compression.ZstdArrowCompressionCodec;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.arrow.ArrowReader;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.RecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.OutOfMemoryException;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarCharVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorUnloader;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.MetadataVersion;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.apache.fluss.compression.ArrowCompressionFactory.toArrowCompressionCodecType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link ArrowReadPlan}. */
class ArrowReadPlanTest {
    private static final RowType ROW_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT()),
                    DataTypes.FIELD("discarded", DataTypes.STRING()),
                    DataTypes.FIELD("value", DataTypes.STRING()));

    @TempDir private Path tempDir;

    @ParameterizedTest
    @MethodSource("fileProjectionReadModes")
    void testFileReadsOnlyMetadataAndSelectedBuffers(
            ArrowCompressionType compression, boolean legacyIpc) throws Exception {
        byte[] bytes = serializedWideBatch(compression, legacyIpc);
        int arrowOffset = 17;
        byte[] fileBytes = new byte[arrowOffset + bytes.length + 23];
        System.arraycopy(bytes, 0, fileBytes, arrowOffset, bytes.length);
        Path file = Files.write(tempDir.resolve("projected.arrow"), fileBytes);
        List<long[]> ranges = new ArrayList<>();
        try (ReadChannel metadataChannel =
                new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes)))) {
            MessageMetadataResult metadata = MessageSerializer.readMessage(metadataChannel);
            long bodyOffset = arrowOffset + metadataChannel.bytesRead();
            ranges.add(new long[] {arrowOffset, bodyOffset});
            RecordBatch batch = (RecordBatch) metadata.getMessage().header(new RecordBatch());
            // id has two buffers; each string has three. None of the discarded string's
            // validity, offsets or data may be touched, even with short FileChannel reads.
            for (int index : new int[] {0, 1, 5, 6, 7}) {
                ranges.add(
                        new long[] {
                            bodyOffset + batch.buffers(index).offset(),
                            bodyOffset
                                    + batch.buffers(index).offset()
                                    + batch.buffers(index).length()
                        });
            }
        }
        AtomicLong bytesRead = new AtomicLong();
        try (FileChannel source = FileChannel.open(file);
                BufferAllocator allocator = new RootAllocator(16 * 1024);
                ArrowReadPlan plan =
                        new ArrowReadPlan(ROW_TYPE, null, new int[] {2, 0}, allocator)) {
            source.position(11);
            FileChannel channel = mock(FileChannel.class);
            when(channel.size()).thenReturn(source.size());
            doAnswer(
                            invocation -> {
                                ByteBuffer target = invocation.getArgument(0);
                                long position = invocation.getArgument(1);
                                long end = position + target.remaining();
                                assertThat(
                                                ranges.stream()
                                                        .anyMatch(
                                                                range ->
                                                                        position >= range[0]
                                                                                && end <= range[1]))
                                        .as(
                                                "read [%s, %s) must be metadata or a selected buffer",
                                                position, end)
                                        .isTrue();
                                if (position >= ranges.get(0)[1]) {
                                    assertThat(target.isDirect()).isTrue();
                                }
                                int limit = target.limit();
                                target.limit(Math.min(limit, target.position() + 31));
                                int read = source.read(target, position);
                                target.limit(limit);
                                bytesRead.addAndGet(read);
                                return read;
                            })
                    .when(channel)
                    .read(any(ByteBuffer.class), anyLong());
            ArrowReader reader = plan.load(channel, arrowOffset, bytes.length);
            long expectedBytes = ranges.stream().mapToLong(range -> range[1] - range[0]).sum();
            assertThat(bytesRead.get()).isEqualTo(expectedBytes).isLessThan(bytes.length / 10);
            assertThat(source.position()).isEqualTo(11);
            assertThat(source.isOpen()).isTrue();
            assertThat(reader.getRowCount()).isEqualTo(64);
            for (int i = 0; i < 64; i++) {
                assertThat(reader.read(i).getInt(0)).isEqualTo(i);
                assertThat(reader.read(i).getString(1).toString()).isEqualTo("value-" + i);
            }
            plan.clear();
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testFileReadFailureReleasesBuffersAndCanRetry() throws Exception {
        byte[] bytes = serializedWideBatch(ArrowCompressionType.NONE, false);
        Path file = Files.write(tempDir.resolve("retry.arrow"), bytes);
        try (FileChannel source = FileChannel.open(file);
                BufferAllocator allocator = new RootAllocator();
                ArrowReadPlan plan =
                        new ArrowReadPlan(ROW_TYPE, null, new int[] {0, 2}, allocator)) {
            FileChannel channel = mock(FileChannel.class);
            when(channel.size()).thenReturn(source.size());
            doAnswer(
                            invocation -> {
                                ByteBuffer target = invocation.getArgument(0);
                                if (target.isDirect()) {
                                    throw new IOException("buffer read failed");
                                }
                                return source.read(target, invocation.getArgument(1));
                            })
                    .when(channel)
                    .read(any(ByteBuffer.class), anyLong());
            assertThatThrownBy(() -> plan.load(channel, 0, bytes.length))
                    .isInstanceOf(FlussRuntimeException.class)
                    .hasRootCauseMessage("buffer read failed");
            assertThat(allocator.getAllocatedMemory()).isZero();
            assertThat(plan.load(source, 0, bytes.length).read(63).getInt(0)).isEqualTo(63);
            plan.clear();
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testFileMetadataAndBodyAreBoundedByBatchLength() throws Exception {
        byte[] bytes = serializedWideBatch(ArrowCompressionType.NONE, false);
        Path file = Files.write(tempDir.resolve("truncated.arrow"), bytes);
        try (BufferAllocator allocator = new RootAllocator(0);
                ArrowReadPlan plan = new ArrowReadPlan(ROW_TYPE, null, new int[0], allocator)) {
            try (FileChannel source = FileChannel.open(file)) {
                assertThatThrownBy(() -> plan.load(source, 0, bytes.length - 1))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("Invalid Arrow body length");
                assertThat(plan.load(source, 0, bytes.length).getRowCount()).isEqualTo(64);
            }
            // Even when the file contains enough bytes, metadata must not borrow from the next
            // batch.
            ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putInt(4, bytes.length);
            Files.write(file, bytes);
            try (FileChannel source = FileChannel.open(file)) {
                assertThatThrownBy(() -> plan.load(source, 0, bytes.length))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("Invalid Arrow metadata length");
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @ParameterizedTest
    @MethodSource("projectionReadModes")
    void testOnlySelectedBuffersAreCopied(
            ArrowCompressionType compression, boolean offHeap, boolean legacyIpc) throws Exception {
        byte[] bytes = serializedWideBatch(compression, legacyIpc);
        // The discarded column alone exceeds the allocator limit, even when compressed. Reading
        // the full body before pruning would fail, but the two selected columns easily fit.
        assertThat(bytes.length).isGreaterThan(64 * 1024);
        int arrowOffset = 17;
        MemorySegment segment =
                offHeap
                        ? MemorySegment.allocateOffHeapMemory(bytes.length + 40)
                        : MemorySegment.allocateHeapMemory(bytes.length + 40);
        try (BufferAllocator allocator = new RootAllocator(16 * 1024);
                ArrowReadPlan plan =
                        new ArrowReadPlan(ROW_TYPE, null, new int[] {2, 0}, allocator)) {
            segment.put(arrowOffset, bytes);
            ArrowReader reader = plan.load(segment, arrowOffset, bytes.length);
            // The loaded vectors must own their data independently of the input segment.
            segment.put(arrowOffset, new byte[bytes.length]);
            assertThat(reader.getRowCount()).isEqualTo(64);
            for (int i = 0; i < reader.getRowCount(); i++) {
                assertThat(reader.read(i).getInt(0)).isEqualTo(i);
                assertThat(reader.read(i).getString(1).toString()).isEqualTo("value-" + i);
            }
            plan.clear();
            assertThat(allocator.getAllocatedMemory()).isZero();
        } finally {
            segment.free();
        }
    }

    @ParameterizedTest
    @EnumSource(ArrowCompressionType.class)
    void testEmptyProjectionDoesNotAllocateBody(ArrowCompressionType compression) throws Exception {
        byte[] bytes = serializedWideBatch(compression, false);
        try (BufferAllocator allocator = new RootAllocator(0);
                ArrowReadPlan plan = new ArrowReadPlan(ROW_TYPE, null, new int[0], allocator)) {
            ArrowReader reader = plan.load(MemorySegment.wrap(bytes), 0, bytes.length);
            assertThat(reader.getRowCount()).isEqualTo(64);
            assertThat(plan.getLoadedRowType().getFieldCount()).isZero();
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testTruncatedBodyIsRejectedBeforeCopying() throws Exception {
        byte[] bytes = serializedWideBatch(ArrowCompressionType.NONE, false);
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReadPlan plan = new ArrowReadPlan(ROW_TYPE, null, new int[] {0}, allocator)) {
            // Keep the complete body in the backing segment. The supplied length must bound reads.
            assertThatThrownBy(() -> plan.load(MemorySegment.wrap(bytes), 0, bytes.length - 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid Arrow body length");
            assertThat(allocator.getAllocatedMemory()).isZero();
            ArrowReader reader = plan.load(MemorySegment.wrap(bytes), 0, bytes.length);
            assertThat(reader.read(63).getInt(0)).isEqualTo(63);
        }
    }

    @Test
    void testFailedCopyCanBeRetried() throws Exception {
        byte[] bytes = serializedWideBatch(ArrowCompressionType.NONE, false);
        try (BufferAllocator allocator = new RootAllocator(0);
                ArrowReadPlan plan = new ArrowReadPlan(ROW_TYPE, null, new int[] {0}, allocator)) {
            assertThatThrownBy(() -> plan.load(MemorySegment.wrap(bytes), 0, bytes.length))
                    .isInstanceOf(OutOfMemoryException.class);
            assertThat(allocator.getAllocatedMemory()).isZero();
            allocator.setLimit(16 * 1024);
            ArrowReader reader = plan.load(MemorySegment.wrap(bytes), 0, bytes.length);
            assertThat(reader.read(63).getInt(0)).isEqualTo(63);
            plan.clear();
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testSelectedBufferCannotReadPastDeclaredBody() throws Exception {
        byte[] bytes = serializedWideBatch(ArrowCompressionType.NONE, false);
        byte[] invalidBytes;
        try (BufferAllocator allocator = new RootAllocator();
                ArrowRecordBatch batch =
                        ArrowUtils.deserializeArrowRecordBatch(
                                MemorySegment.wrap(bytes), 0, bytes.length, allocator);
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            List<ArrowBuffer> layout = new ArrayList<>(batch.getBuffersLayout());
            layout.set(1, new ArrowBuffer(batch.computeBodyLength(), 8));
            WriteChannel channel = new WriteChannel(Channels.newChannel(output));
            ArrowUtils.serializeArrowRecordBatchMetadata(
                    channel,
                    batch.getLength(),
                    batch.getNodes(),
                    layout,
                    batch.getBodyCompression(),
                    batch.computeBodyLength());
            MessageSerializer.writeBatchBuffers(channel, batch);
            // The segment has enough trailing bytes, but the selected buffer must fit in the body
            // declared by this message rather than borrowing bytes from the next message.
            invalidBytes = Arrays.copyOf(output.toByteArray(), output.size() + 8);
        }
        try (BufferAllocator allocator = new RootAllocator(0);
                ArrowReadPlan plan = new ArrowReadPlan(ROW_TYPE, null, new int[] {0}, allocator)) {
            assertThatThrownBy(
                            () ->
                                    plan.load(
                                            MemorySegment.wrap(invalidBytes),
                                            0,
                                            invalidBytes.length))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Arrow buffer 1 range")
                    .hasMessageContaining("exceeds body length");
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testUnselectedBuffersAreNotDecompressed() throws Exception {
        // The discarded column's data buffer contains an invalid ZSTD frame. Reading the selected
        // columns must not invoke the decompressor on that buffer.
        byte[] bytes = serializedBatch(4);
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReadPlan plan =
                        new ArrowReadPlan(ROW_TYPE, null, new int[] {2, 0}, allocator)) {
            assertThat(plan.getLoadedRowType().getFieldNames()).containsExactly("id", "value");
            ArrowReader reader = plan.load(MemorySegment.wrap(bytes), 0, bytes.length);
            assertThat(reader.getRowCount()).isEqualTo(64);
            for (int i = 0; i < reader.getRowCount(); i++) {
                assertThat(reader.read(i).getInt(0)).isEqualTo(i);
                assertThat(reader.read(i).getString(1).toString()).isEqualTo(value());
            }
            ProjectedRow output = plan.createOutputRow();
            assertThat(output).isNotNull();
            output.replaceRow(reader.read(1));
            assertThat(output.getInt(0)).isEqualTo(1);
            assertThat(output.isNullAt(1)).isTrue();
            assertThat(output.getString(2).toString()).isEqualTo(value());
            plan.clear();
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testAllSelectedFieldsLoadWithoutOutputRow() throws Exception {
        byte[] bytes = serializedBatch(-1);
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReadPlan plan =
                        new ArrowReadPlan(ROW_TYPE, null, new int[] {2, 1, 0}, allocator)) {
            assertThat(plan.getLoadedRowType()).isEqualTo(ROW_TYPE);
            assertThat(plan.createOutputRow()).isNull();
            ArrowReader reader = plan.load(MemorySegment.wrap(bytes), 0, bytes.length);
            assertThat(reader.read(63).getInt(0)).isEqualTo(63);
            assertThat(reader.read(63).getString(1).toString()).isEqualTo(value());
            plan.clear();
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testFailedLoadReleasesSelectedBuffers() throws Exception {
        // The id field fits, but the value field requires a 64 KiB decompression buffer.
        byte[] bytes = serializedBatch(-1);
        try (BufferAllocator allocator = new RootAllocator(32 * 1024);
                ArrowReadPlan plan =
                        new ArrowReadPlan(ROW_TYPE, null, new int[] {0, 2}, allocator)) {
            assertThatThrownBy(() -> plan.load(MemorySegment.wrap(bytes), 0, bytes.length))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasRootCauseInstanceOf(OutOfMemoryException.class);
            assertThat(allocator.getAllocatedMemory()).isZero();

            allocator.setLimit(Long.MAX_VALUE);
            ArrowReader reader = plan.load(MemorySegment.wrap(bytes), 0, bytes.length);
            assertThat(reader.read(63).getInt(0)).isEqualTo(63);
            plan.clear();
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    private static Stream<Arguments> fileProjectionReadModes() {
        return Arrays.stream(ArrowCompressionType.values())
                .flatMap(
                        compression ->
                                Stream.of(
                                        Arguments.of(compression, false),
                                        Arguments.of(compression, true)));
    }

    private static Stream<Arguments> projectionReadModes() {
        List<Arguments> modes = new ArrayList<>();
        for (ArrowCompressionType compression : ArrowCompressionType.values()) {
            for (boolean offHeap : new boolean[] {false, true}) {
                for (boolean legacyIpc : new boolean[] {false, true}) {
                    modes.add(Arguments.of(compression, offHeap, legacyIpc));
                }
            }
        }
        return modes.stream();
    }

    private static byte[] serializedWideBatch(ArrowCompressionType compression, boolean legacyIpc)
            throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(ROW_TYPE), allocator)) {
            Random random = new Random(42);
            byte[] discarded = new byte[4096];
            for (int i = 0; i < 64; i++) {
                random.nextBytes(discarded);
                for (int j = 0; j < discarded.length; j++) {
                    discarded[j] = (byte) (' ' + (discarded[j] & 0x3f));
                }
                ((IntVector) root.getVector(0)).setSafe(i, i);
                ((VarCharVector) root.getVector(1)).setSafe(i, discarded);
                ((VarCharVector) root.getVector(2))
                        .setSafe(i, ("value-" + i).getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(64);
            try (ArrowRecordBatch batch =
                            new VectorUnloader(
                                            root,
                                            true,
                                            ArrowCompressionFactory.INSTANCE.createCodec(
                                                    toArrowCompressionCodecType(compression)),
                                            true)
                                    .getRecordBatch();
                    ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                MessageSerializer.serialize(
                        new WriteChannel(Channels.newChannel(output)),
                        batch,
                        new IpcOption(legacyIpc, MetadataVersion.V5));
                return output.toByteArray();
            }
        }
    }

    private static byte[] serializedBatch(int corruptBuffer) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(ROW_TYPE), allocator)) {
            byte[] value = value().getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < 64; i++) {
                ((IntVector) root.getVector(0)).setSafe(i, i);
                ((VarCharVector) root.getVector(1)).setSafe(i, value);
                ((VarCharVector) root.getVector(2)).setSafe(i, value);
            }
            root.setRowCount(64);
            try (ArrowRecordBatch batch =
                            new VectorUnloader(root, true, new ZstdArrowCompressionCodec(), true)
                                    .getRecordBatch();
                    ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                if (corruptBuffer >= 0) {
                    ArrowBuf buffer = batch.getBuffers().get(corruptBuffer);
                    assertThat(buffer.getLong(0)).isPositive();
                    buffer.setByte(8, buffer.getByte(8) ^ 0xff);
                }
                MessageSerializer.serialize(new WriteChannel(Channels.newChannel(output)), batch);
                return output.toByteArray();
            }
        }
    }

    private static String value() {
        char[] chars = new char[1024];
        Arrays.fill(chars, 'a');
        return new String(chars);
    }
}
