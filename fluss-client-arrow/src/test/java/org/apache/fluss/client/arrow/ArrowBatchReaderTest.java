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

package org.apache.fluss.client.arrow;

import org.apache.fluss.compression.ArrowCompressionFactory;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.ArrowIpcBatch;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorUnloader;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.fluss.types.DataTypes;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests decoding Fluss IPC payloads with unshaded Arrow Java. */
class ArrowBatchReaderTest {
    @ParameterizedTest
    @EnumSource(CodecType.class)
    void testCodecsAndChangelogSlices(CodecType codec) throws Exception {
        ArrowIpcBatch ipc = createBatch(codec).slice(1, 6).slice(2, 2);
        assertThat(ipc.getRowOffset()).isEqualTo(3);
        assertThat(ipc.getBaseLogOffset()).isEqualTo(103);
        assertThat(ipc.getChangeTypes().get().remaining()).isEqualTo(2);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (ArrowBatch batch = ArrowBatchReader.read(ipc, allocator)) {
                IntVector values = (IntVector) batch.getVectorSchemaRoot().getVector(0);
                assertThat(values.getValueCount()).isEqualTo(2);
                assertThat(values.get(0)).isEqualTo(3);
                assertThat(values.get(1)).isEqualTo(4);
                assertThat(batch.getChangeType(0)).isEqualTo(ChangeType.UPDATE_AFTER);
                assertThat(batch.getChangeType(1)).isEqualTo(ChangeType.UPDATE_BEFORE);
                assertThat(batch.getChangeTypes().get().get(0))
                        .isEqualTo(ChangeType.UPDATE_AFTER.toByteValue());
                assertThat(batch.getTimestamp()).isEqualTo(12345L);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
            try (ArrowBatch empty = ArrowBatchReader.read(ipc.slice(2, 0), allocator)) {
                assertThat(empty.getVectorSchemaRoot().getRowCount()).isZero();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(CodecType.class)
    void testStandardArrowStreamInteroperability(CodecType codec) throws Exception {
        ArrowIpcBatch ipc = createBatch(codec);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader reader =
                        new ArrowStreamReader(
                                new SequenceInputStream(
                                        new ByteArrayInputStream(bytes(ipc.getSchema())),
                                        new ByteArrayInputStream(bytes(ipc.getRecordBatch()))),
                                allocator,
                                CommonsCompressionFactory.INSTANCE)) {
            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(reader.getVectorSchemaRoot().getRowCount()).isEqualTo(1000);
            IntVector values = (IntVector) reader.getVectorSchemaRoot().getVector(0);
            assertThat(values.get(0)).isZero();
            assertThat(values.get(999)).isEqualTo(999);
            assertThat(reader.loadNextBatch()).isFalse();
        }
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    @Test
    void testAllocationFailureLeavesAllocatorUsable() throws Exception {
        ArrowIpcBatch ipc = createBatch(CodecType.ZSTD);
        try (BufferAllocator allocator = new RootAllocator(8)) {
            assertThatThrownBy(() -> ArrowBatchReader.read(ipc, allocator))
                    .isInstanceOf(org.apache.arrow.memory.OutOfMemoryException.class);
            assertThat(allocator.getAllocatedMemory()).isZero();
            try (org.apache.arrow.memory.ArrowBuf buffer = allocator.buffer(1)) {
                buffer.setByte(0, 7);
                assertThat(buffer.getByte(0)).isEqualTo((byte) 7);
            }
        }
    }

    @Test
    void testInputBuffersAndSlicesAreIndependent() throws Exception {
        ArrowIpcBatch ipc = createBatch(CodecType.NO_COMPRESSION);
        ipc.getRecordBatch().get();
        ipc.getSchema().get();
        assertThat(ipc.getRecordBatch().position()).isZero();
        assertThat(ipc.getSchema().position()).isZero();
        assertThatThrownBy(() -> ipc.getRecordBatch().put((byte) 0))
                .isInstanceOf(java.nio.ReadOnlyBufferException.class);
        assertThatThrownBy(() -> ipc.slice(Integer.MAX_VALUE, 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(ipc.slice(2, 1).getChangeType(0)).isEqualTo(ChangeType.UPDATE_BEFORE);
        assertThat(ipc.getRecordCount()).isEqualTo(1000);
    }

    private static ArrowIpcBatch createBatch(CodecType codec) throws Exception {
        Schema schema = Schema.newBuilder().column("value", DataTypes.INT()).build();
        try (LogRecordReadContext context =
                LogRecordReadContext.createArrowReadContext(
                        schema.getRowType(), 1, new TestingSchemaGetter(1, schema))) {
            org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot root =
                    context.getVectorSchemaRoot(1);
            root.allocateNew();
            org.apache.fluss.shaded.arrow.org.apache.arrow.vector.IntVector values =
                    (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.IntVector)
                            root.getVector(0);
            byte[] changes = new byte[1000];
            for (int i = 0; i < 1000; i++) {
                values.setSafe(i, i);
                changes[i] =
                        (i % 2 == 0 ? ChangeType.UPDATE_BEFORE : ChangeType.UPDATE_AFTER)
                                .toByteValue();
            }
            root.setRowCount(1000);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowRecordBatch batch =
                    new VectorUnloader(
                                    root,
                                    true,
                                    ArrowCompressionFactory.INSTANCE.createCodec(codec),
                                    true)
                            .getRecordBatch()) {
                MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch);
            }
            return context.createArrowIpcBatch(out.toByteArray(), 100, 12345L, 1, 1000, changes);
        }
    }
}
