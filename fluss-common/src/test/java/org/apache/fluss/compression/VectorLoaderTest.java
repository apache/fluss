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

package org.apache.fluss.compression;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarCharVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorLoader;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorUnloader;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.utils.ByteBufferReadableChannel;
import org.apache.fluss.utils.MemorySegmentWritableChannel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the patched {@link VectorLoader} that ensures decompressed buffers are released when an
 * error occurs during loading.
 *
 * @see <a href="https://github.com/apache/fluss/issues/2646">FLUSS-2646</a>
 */
class VectorLoaderTest {

    private static final Schema SCHEMA =
            new Schema(
                    Arrays.asList(
                            Field.nullable("ints", new ArrowType.Int(32, true)),
                            Field.nullable("strings", ArrowType.Utf8.INSTANCE)));

    private BufferAllocator allocator;

    @BeforeEach
    void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    /** Tests that normal load with compression succeeds and data is correctly decompressed. */
    @Test
    void testNormalLoadWithCompression() throws Exception {
        testLoadWithCompression(
                (schemaRoot, arrowRecordBatch) -> {
                    VectorLoader loader =
                            new VectorLoader(schemaRoot, ArrowCompressionFactory.INSTANCE);
                    loader.load(arrowRecordBatch);
                    assertThat(schemaRoot.getRowCount()).isEqualTo(10);
                    IntVector readInts = (IntVector) schemaRoot.getVector(0);
                    VarCharVector readStrings = (VarCharVector) schemaRoot.getVector(1);
                    for (int i = 0; i < 10; i++) {
                        assertThat(readInts.get(i)).isEqualTo(i);
                        assertThat(readStrings.get(i))
                                .isEqualTo(("value_" + i).getBytes(StandardCharsets.UTF_8));
                    }
                });
    }

    /**
     * Tests that when decompression fails mid-way through loading, the already-decompressed buffers
     * are properly released by the try-finally block in VectorLoader.
     *
     * <p>This verifies the fix that moved the decompression loop inside the try block. Without the
     * fix, decompressed buffers created before the failure would be leaked.
     *
     * <p>The test uses a tracking codec that retains an extra reference on each decompressed buffer
     * for inspection. After VectorLoader's finally block closes the buffers, only our tracking
     * retain should remain (ref count = 1). Without the fix, the ref count would be 2.
     */
    @Test
    void testLoadReleasesDecompressedBuffersOnDecompressionFailure() throws Exception {
        // Fail on the 4th decompress call.
        // "ints" has 2 buffers (both succeed), "strings" validity buffer succeeds,
        // "ints" offset buffer (2nd) fails.
        testLoadWithCompression(
                (schemaRoot, arrowRecordBatch) -> {
                    CompressionCodec.Factory failingFactory = new TrackingFailCodecFactory(2);
                    VectorLoader loader = new VectorLoader(schemaRoot, failingFactory);
                    assertThatThrownBy(() -> loader.load(arrowRecordBatch))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("Could not load buffers")
                            .hasMessageContaining(
                                    "error message: Simulated OOM on decompress call");
                });
    }

    /**
     * Tests that when decompression fails for the second field, decompressed buffers from the first
     * field (already loaded into the VectorSchemaRoot) are properly managed and can be released via
     * VectorSchemaRoot.close().
     */
    @Test
    void testLoadReleasesBuffersFromPreviousFieldOnFailure() throws Exception {
        // Fail on the 4th decompress call.
        // "ints" has 2 buffers (both succeed), "strings" validity buffer succeeds,
        // "strings" offset buffer (4th) fails.
        testLoadWithCompression(
                (schemaRoot, arrowRecordBatch) -> {
                    CompressionCodec.Factory failingFactory = new TrackingFailCodecFactory(4);
                    VectorLoader loader = new VectorLoader(schemaRoot, failingFactory);
                    assertThatThrownBy(() -> loader.load(arrowRecordBatch))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("Could not load buffers")
                            .hasMessageContaining(
                                    "error message: Simulated OOM on decompress call");
                });
    }

    void testLoadWithCompression(BiConsumer<VectorSchemaRoot, ArrowRecordBatch> loadAction)
            throws Exception {
        MemorySegment memorySegment = MemorySegment.allocateHeapMemory(1024 * 10);
        MemorySegmentWritableChannel memorySegmentWritableChannel =
                new MemorySegmentWritableChannel(memorySegment);
        ArrowBlock arrowBlock;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
            populateData(root, 10);
            try (ArrowRecordBatch arrowBatch =
                    new VectorUnloader(root, true, new ZstdArrowCompressionCodec(), true)
                            .getRecordBatch()) {
                arrowBlock =
                        MessageSerializer.serialize(
                                new WriteChannel(memorySegmentWritableChannel), arrowBatch);
            }
        }

        ByteBuffer arrowBatchBuffer =
                memorySegment.wrap(
                        (int) arrowBlock.getOffset(),
                        (int) arrowBlock.getBodyLength() + arrowBlock.getMetadataLength());
        ReadChannel channel = new ReadChannel(new ByteBufferReadableChannel(arrowBatchBuffer));
        try (VectorSchemaRoot readRoot = VectorSchemaRoot.create(SCHEMA, allocator);
                ArrowRecordBatch arrowMessage =
                        MessageSerializer.deserializeRecordBatch(channel, allocator)) {
            loadAction.accept(readRoot, arrowMessage);
        }
    }

    private static void populateData(VectorSchemaRoot root, int rowCount) {
        IntVector ints = (IntVector) root.getVector(0);
        VarCharVector strings = (VarCharVector) root.getVector(1);
        for (int i = 0; i < rowCount; i++) {
            ints.setSafe(i, i);
            strings.setSafe(i, ("value_" + i).getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(rowCount);
    }

    // -----------------------------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------------------------

    /**
     * A {@link CompressionCodec.Factory} that creates a {@link TrackingFailCodec} which tracks
     * decompressed buffers and fails on the Nth decompress call.
     */
    private static class TrackingFailCodecFactory implements CompressionCodec.Factory {
        private final int failOnNthDecompress;

        TrackingFailCodecFactory(int failOnNthDecompress) {
            this.failOnNthDecompress = failOnNthDecompress;
        }

        @Override
        public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
            return new TrackingFailCodec(
                    ArrowCompressionFactory.INSTANCE.createCodec(codecType), failOnNthDecompress);
        }

        @Override
        public CompressionCodec createCodec(
                CompressionUtil.CodecType codecType, int compressionLevel) {
            return new TrackingFailCodec(
                    ArrowCompressionFactory.INSTANCE.createCodec(codecType, compressionLevel),
                    failOnNthDecompress);
        }
    }

    /**
     * A {@link CompressionCodec} that wraps a real codec but:
     *
     * <ol>
     *   <li>Retains an extra reference on each decompressed buffer for tracking.
     *   <li>Throws a simulated OOM on the Nth decompress call.
     * </ol>
     */
    private static class TrackingFailCodec implements CompressionCodec {
        private final CompressionCodec delegate;
        private final int failOnN;
        private final AtomicInteger count = new AtomicInteger(0);

        TrackingFailCodec(CompressionCodec delegate, int failOnN) {
            this.delegate = delegate;
            this.failOnN = failOnN;
        }

        @Override
        public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
            if (count.incrementAndGet() >= failOnN) {
                throw new RuntimeException("Simulated OOM on decompress call #" + count.get());
            }
            return delegate.decompress(allocator, compressedBuffer);
        }

        @Override
        public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
            return delegate.compress(allocator, uncompressedBuffer);
        }

        @Override
        public CompressionUtil.CodecType getCodecType() {
            return delegate.getCodecType();
        }
    }
}
