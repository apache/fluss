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

package com.alibaba.fluss.compression;

import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.util.AutoCloseables;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarBinaryVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarCharVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorUnloader;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for arrow compression codec, such as {@link ZstdArrowCompressionCodec} and {@link
 * Lz4ArrowCompressionCodec}.
 */
class ArrowCompressionCodecTest {
    private BufferAllocator allocator;

    @BeforeEach
    void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @ParameterizedTest
    @MethodSource("codecs")
    void testCompressFixedWidthBuffers(int vectorLength, CompressionCodec codec) throws Exception {
        // prepare vector to compress
        IntVector origVec = new IntVector("vec", allocator);
        origVec.allocateNew(vectorLength);
        for (int i = 0; i < vectorLength; i++) {
            if (i % 10 == 0) {
                origVec.setNull(i);
            } else {
                origVec.set(i, i);
            }
        }
        origVec.setValueCount(vectorLength);
        int nullCount = origVec.getNullCount();

        // compress & decompress.
        List<ArrowBuf> origBuffers = origVec.getFieldBuffers();
        List<ArrowBuf> compressedBuffers = compressBuffers(codec, origBuffers);
        List<ArrowBuf> decompressedBuffers = deCompressBuffers(codec, compressedBuffers);

        assertThat(decompressedBuffers.size()).isEqualTo(2);
        assertWriterIndex(decompressedBuffers);

        // orchestrate new vector.
        IntVector newVec = new IntVector("new vec", allocator);
        newVec.loadFieldBuffers(new ArrowFieldNode(vectorLength, nullCount), decompressedBuffers);

        // verify new vector.
        assertThat(newVec.getValueCount()).isEqualTo(vectorLength);
        for (int i = 0; i < vectorLength; i++) {
            if (i % 10 == 0) {
                assertThat(newVec.isNull(i)).isTrue();
            } else {
                assertThat(newVec.get(i)).isEqualTo(i);
            }
        }

        newVec.close();
        AutoCloseables.close(decompressedBuffers);
    }

    @ParameterizedTest
    @MethodSource("codecs")
    void testCompressVariableWidthBuffers(int vectorLength, CompressionCodec codec)
            throws Exception {
        // prepare vector to compress
        VarCharVector origVec = new VarCharVector("vec", allocator);
        origVec.allocateNew();
        for (int i = 0; i < vectorLength; i++) {
            if (i % 10 == 0) {
                origVec.setNull(i);
            } else {
                origVec.setSafe(i, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            }
        }
        origVec.setValueCount(vectorLength);
        int nullCount = origVec.getNullCount();

        // compress & decompress
        List<ArrowBuf> origBuffers = origVec.getFieldBuffers();
        List<ArrowBuf> compressedBuffers = compressBuffers(codec, origBuffers);
        List<ArrowBuf> decompressedBuffers = deCompressBuffers(codec, compressedBuffers);

        assertThat(decompressedBuffers.size()).isEqualTo(3);
        assertWriterIndex(decompressedBuffers);

        // orchestrate new vector
        VarCharVector newVec = new VarCharVector("new vec", allocator);
        newVec.loadFieldBuffers(new ArrowFieldNode(vectorLength, nullCount), decompressedBuffers);

        // verify new vector
        assertThat(newVec.getValueCount()).isEqualTo(vectorLength);
        for (int i = 0; i < vectorLength; i++) {
            if (i % 10 == 0) {
                assertThat(newVec.isNull(i)).isTrue();
            } else {
                assertThat(newVec.get(i))
                        .isEqualTo(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            }
        }

        newVec.close();
        AutoCloseables.close(decompressedBuffers);
    }

    @ParameterizedTest
    @MethodSource("codecs")
    void testEmptyBuffer(int vectorLength, CompressionCodec codec) throws Exception {
        final VarBinaryVector origVec = new VarBinaryVector("vec", allocator);

        origVec.allocateNew(vectorLength);

        // Do not set any values (all missing)
        origVec.setValueCount(vectorLength);

        final List<ArrowBuf> origBuffers = origVec.getFieldBuffers();
        final List<ArrowBuf> compressedBuffers = compressBuffers(codec, origBuffers);
        final List<ArrowBuf> decompressedBuffers = deCompressBuffers(codec, compressedBuffers);

        // orchestrate new vector
        VarBinaryVector newVec = new VarBinaryVector("new vec", allocator);
        newVec.loadFieldBuffers(
                new ArrowFieldNode(vectorLength, vectorLength), decompressedBuffers);

        // verify new vector.
        assertThat(newVec.getValueCount()).isEqualTo(vectorLength);
        for (int i = 0; i < vectorLength; i++) {
            assertThat(newVec.isNull(i)).isTrue();
        }

        newVec.close();
        AutoCloseables.close(decompressedBuffers);
    }

    @ParameterizedTest
    @MethodSource("codecTypes")
    void testUnloadCompressed(CompressionUtil.CodecType codec) {
        final Schema schema =
                new Schema(
                        Arrays.asList(
                                Field.nullable("ints", new ArrowType.Int(32, true)),
                                Field.nullable("strings", ArrowType.Utf8.INSTANCE)));
        CompressionCodec.Factory factory =
                codec == CompressionUtil.CodecType.NO_COMPRESSION
                        ? NoCompressionCodec.Factory.INSTANCE
                        : ArrowCompressionFactory.INSTANCE;
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            final IntVector ints = (IntVector) root.getVector(0);
            final VarCharVector strings = (VarCharVector) root.getVector(1);
            // Doesn't get compressed
            ints.setSafe(0, 0x4a3e);
            ints.setSafe(1, 0x8aba);
            ints.setSafe(2, 0x4362);
            ints.setSafe(3, 0x383f);
            // Gets compressed
            String compressibleString = "                "; // 16 bytes
            compressibleString = compressibleString + compressibleString;
            compressibleString = compressibleString + compressibleString;
            compressibleString = compressibleString + compressibleString;
            compressibleString = compressibleString + compressibleString;
            compressibleString = compressibleString + compressibleString; // 512 bytes
            byte[] compressibleData = compressibleString.getBytes(StandardCharsets.UTF_8);
            strings.setSafe(0, compressibleData);
            strings.setSafe(1, compressibleData);
            strings.setSafe(2, compressibleData);
            strings.setSafe(3, compressibleData);
            root.setRowCount(4);

            List<FieldVector> fieldVectors = root.getFieldVectors();
            for (FieldVector fieldVector : fieldVectors) {
                for (ArrowBuf buffer : fieldVector.getBuffers(false)) {
                    assertThat(buffer.getReferenceManager().getRefCount()).isNotEqualTo(0);
                }
            }

            final VectorUnloader vectorUnloader =
                    new VectorUnloader(root, true, factory.createCodec(codec), true);
            vectorUnloader.getRecordBatch().close();

            for (FieldVector fieldVector : fieldVectors) {
                for (ArrowBuf buffer : fieldVector.getBuffers(false)) {
                    assertThat(buffer.getReferenceManager().getRefCount()).isNotEqualTo(0);
                }
            }
        }
    }

    private static Collection<Arguments> codecs() {
        List<Arguments> params = new ArrayList<>();

        int[] lengths = new int[] {10, 100, 1000};
        for (int len : lengths) {
            CompressionCodec dumbCodec = NoCompressionCodec.INSTANCE;
            params.add(Arguments.arguments(len, dumbCodec));

            CompressionCodec lz4Codec = new Lz4ArrowCompressionCodec();
            params.add(Arguments.arguments(len, lz4Codec));

            CompressionCodec zstdCodec = new ZstdArrowCompressionCodec();
            params.add(Arguments.arguments(len, zstdCodec));

            CompressionCodec zstdCodecAndCompressionLevel = new ZstdArrowCompressionCodec(7);
            params.add(Arguments.arguments(len, zstdCodecAndCompressionLevel));
        }

        return params;
    }

    private static Stream<CompressionUtil.CodecType> codecTypes() {
        return Arrays.stream(CompressionUtil.CodecType.values());
    }

    private List<ArrowBuf> compressBuffers(CompressionCodec codec, List<ArrowBuf> inputBuffers) {
        List<ArrowBuf> outputBuffers = new ArrayList<>(inputBuffers.size());
        for (ArrowBuf buf : inputBuffers) {
            outputBuffers.add(codec.compress(allocator, buf));
        }
        return outputBuffers;
    }

    private List<ArrowBuf> deCompressBuffers(CompressionCodec codec, List<ArrowBuf> inputBuffers) {
        List<ArrowBuf> outputBuffers = new ArrayList<>(inputBuffers.size());
        for (ArrowBuf buf : inputBuffers) {
            outputBuffers.add(codec.decompress(allocator, buf));
        }
        return outputBuffers;
    }

    private void assertWriterIndex(List<ArrowBuf> decompressedBuffers) {
        for (ArrowBuf decompressedBuf : decompressedBuffers) {
            assertThat(decompressedBuf.writerIndex()).isGreaterThan(0);
        }
    }
}
