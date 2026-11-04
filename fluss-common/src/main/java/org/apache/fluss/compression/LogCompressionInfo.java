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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.MemorySegment;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Compression codec and level of batch-level compression of row-based log record batches (e.g.
 * COMPACTED), with the methods to compress and decompress a batch's records section.
 */
@Internal
public class LogCompressionInfo {

    /** Batches are not compressed. */
    public static final LogCompressionInfo NO_COMPRESSION =
            new LogCompressionInfo(LogCompressionType.NONE, -1);

    private final LogCompressionType compressionType;
    private final int compressionLevel;

    /**
     * Creates a compression info.
     *
     * @param compressionType the compression codec
     * @param compressionLevel the compression level, or -1 if the codec has no level
     */
    public LogCompressionInfo(LogCompressionType compressionType, int compressionLevel) {
        this.compressionType = compressionType;
        this.compressionLevel = compressionLevel;
    }

    /** Returns the compression codec. */
    public LogCompressionType getCompressionType() {
        return compressionType;
    }

    /** Returns the compression level, or -1 if the compression type has no level. */
    public int getCompressionLevel() {
        return compressionLevel;
    }

    /** Creates the compression info of COMPACTED log batches from the table configuration. */
    public static LogCompressionInfo fromConf(Configuration conf) {
        LogCompressionType compressionType =
                conf.get(ConfigOptions.TABLE_LOG_COMPACTED_COMPRESSION_TYPE);
        if (compressionType == LogCompressionType.ZSTD) {
            int compressionLevel =
                    conf.get(ConfigOptions.TABLE_LOG_COMPACTED_COMPRESSION_ZSTD_LEVEL);
            if (compressionLevel < 1 || compressionLevel > 22) {
                throw new IllegalArgumentException(
                        "Invalid ZSTD compression level: "
                                + compressionLevel
                                + ". Expected a value between 1 and 22.");
            }
            return new LogCompressionInfo(compressionType, compressionLevel);
        } else {
            return new LogCompressionInfo(compressionType, -1);
        }
    }

    /**
     * Compresses {@code length} bytes of {@code src} starting at {@code srcOffset} into {@code dst}
     * starting at {@code dstOffset}, writing at most {@code maxLength} bytes.
     *
     * @return the number of compressed bytes, or -1 if they don't fit into {@code maxLength} bytes
     */
    public int compress(
            byte[] src, int srcOffset, int length, byte[] dst, int dstOffset, int maxLength) {
        switch (compressionType) {
            case ZSTD:
                try {
                    return (int)
                            Zstd.compressByteArray(
                                    dst,
                                    dstOffset,
                                    maxLength,
                                    src,
                                    srcOffset,
                                    length,
                                    compressionLevel);
                } catch (ZstdException e) {
                    if (e.getErrorCode() == Zstd.errDstSizeTooSmall()) {
                        return -1;
                    }
                    throw e;
                }
            case LZ4_FRAME:
                BoundedOutputStream out = new BoundedOutputStream(dst, dstOffset, maxLength);
                try {
                    FlussLZ4BlockOutputStream lz4 = new FlussLZ4BlockOutputStream(out);
                    lz4.write(src, srcOffset, length);
                    lz4.close();
                    return out.written;
                } catch (OutputFullException e) {
                    return -1;
                } catch (IOException e) {
                    throw new UncheckedIOException("LZ4 compression failed", e);
                }
            default:
                throw new IllegalArgumentException(
                        "Unsupported compression type: " + compressionType);
        }
    }

    /**
     * Decompresses {@code length} bytes of {@code segment} starting at {@code offset} into a new
     * array of {@code uncompressedSize} bytes.
     */
    public static byte[] decompress(
            LogCompressionType type,
            MemorySegment segment,
            int offset,
            int length,
            int uncompressedSize) {
        ByteBuffer src = segment.wrap(offset, length);
        byte[] dst = new byte[uncompressedSize];
        switch (type) {
            case ZSTD:
                long read;
                if (src.hasArray()) {
                    read =
                            Zstd.decompressByteArray(
                                    dst,
                                    0,
                                    dst.length,
                                    src.array(),
                                    src.arrayOffset() + src.position(),
                                    length);
                } else {
                    // zstd-jni only decompresses a direct buffer into a direct buffer
                    byte[] heapSrc = new byte[length];
                    src.get(heapSrc);
                    read = Zstd.decompressByteArray(dst, 0, dst.length, heapSrc, 0, length);
                }
                if (Zstd.isError(read) || read != uncompressedSize) {
                    throw new IllegalStateException(
                            "ZSTD decompression failed: "
                                    + (Zstd.isError(read) ? Zstd.getErrorName(read) : read));
                }
                return dst;
            case LZ4_FRAME:
                try (FlussLZ4BlockInputStream lz4 = new FlussLZ4BlockInputStream(src)) {
                    int pos = 0;
                    while (pos < uncompressedSize) {
                        int n = lz4.read(dst, pos, uncompressedSize - pos);
                        if (n < 0) {
                            throw new IllegalStateException(
                                    "LZ4 stream ended after " + pos + " bytes");
                        }
                        pos += n;
                    }
                    return dst;
                } catch (IOException e) {
                    throw new UncheckedIOException("LZ4 decompression failed", e);
                }
            default:
                throw new IllegalArgumentException("Unsupported compression type: " + type);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogCompressionInfo that = (LogCompressionInfo) o;
        return compressionLevel == that.compressionLevel && compressionType == that.compressionType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(compressionType, compressionLevel);
    }

    @Override
    public String toString() {
        return compressionLevel == -1
                ? compressionType.toString()
                : compressionType + "-" + compressionLevel;
    }

    /** An {@link OutputStream} writing into a fixed region of an array. */
    private static final class BoundedOutputStream extends OutputStream {
        private final byte[] buffer;
        private final int offset;
        private final int capacity;
        private int written;

        private BoundedOutputStream(byte[] buffer, int offset, int capacity) {
            this.buffer = buffer;
            this.offset = offset;
            this.capacity = capacity;
        }

        @Override
        public void write(int b) throws IOException {
            if (written == capacity) {
                throw new OutputFullException();
            }
            buffer[offset + written++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (len > capacity - written) {
                throw new OutputFullException();
            }
            System.arraycopy(b, off, buffer, offset + written, len);
            written += len;
        }
    }

    /** Thrown when the compressed bytes don't fit into the output region. */
    private static final class OutputFullException extends IOException {
        private static final long serialVersionUID = 1L;

        private OutputFullException() {
            super("Compressed output exceeds the available space");
        }
    }
}
