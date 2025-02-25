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

import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.compression.AbstractCompressionCodec;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;

import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;

/* This file is based on source code of Apache Arrow-java Project (https://github.com/apache/arrow-java), licensed by
 * the Apache Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership. */

/** Arrow Compression codec for the Zstd algorithm. */
public class ZstdArrowCompressionCodec extends AbstractCompressionCodec {
    private static final int DEFAULT_COMPRESSION_LEVEL = 3;
    private final int compressionLevel;

    public ZstdArrowCompressionCodec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    public ZstdArrowCompressionCodec(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

    @Override
    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
        long maxSize = Zstd.compressBound(uncompressedBuffer.writerIndex());
        ByteBuffer uncompressedDirectBuffer = uncompressedBuffer.nioBuffer();

        long compressedSize = CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + maxSize;
        ArrowBuf compressedBuffer = allocator.buffer(compressedSize);
        ByteBuffer compressedDirectBuffer =
                compressedBuffer.nioBuffer(
                        CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, (int) maxSize);

        // The reason why we use Zstd.compressDirectByteBuffer instead of Zstd.compressUnsafe used
        // in arrow-java here is that compressUnsafe() may encounter occasional data corruption
        // issues when dealing with large volumes of data, and the cause has not yet been
        // determined.
        long bytesWritten =
                Zstd.compressDirectByteBuffer(
                        compressedDirectBuffer,
                        0,
                        (int) maxSize,
                        uncompressedDirectBuffer,
                        0,
                        (int) uncompressedBuffer.writerIndex(),
                        compressionLevel);

        if (Zstd.isError(bytesWritten)) {
            compressedBuffer.close();
            throw new RuntimeException("Error compressing: " + Zstd.getErrorName(bytesWritten));
        }

        compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + bytesWritten);
        return compressedBuffer;
    }

    @Override
    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
        long decompressedLength = readUncompressedLength(compressedBuffer);

        ByteBuffer compressedDirectBuffer = compressedBuffer.nioBuffer();
        ArrowBuf uncompressedBuffer = allocator.buffer(decompressedLength);
        ByteBuffer uncompressedDirectBuffer =
                uncompressedBuffer.nioBuffer(0, (int) decompressedLength);

        long decompressedSize =
                Zstd.decompressDirectByteBuffer(
                        uncompressedDirectBuffer,
                        0,
                        (int) decompressedLength,
                        compressedDirectBuffer,
                        (int) CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH,
                        (int)
                                (compressedBuffer.writerIndex()
                                        - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH));
        if (Zstd.isError(decompressedSize)) {
            uncompressedBuffer.close();
            throw new RuntimeException(
                    "Error decompressing: " + Zstd.getErrorName(decompressedSize));
        }

        if (decompressedLength != decompressedSize) {
            uncompressedBuffer.close();
            throw new RuntimeException(
                    "Expected != actual decompressed length: "
                            + decompressedLength
                            + " != "
                            + decompressedSize);
        }
        uncompressedBuffer.writerIndex(decompressedLength);
        return uncompressedBuffer;
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
        return CompressionUtil.CodecType.ZSTD;
    }
}
