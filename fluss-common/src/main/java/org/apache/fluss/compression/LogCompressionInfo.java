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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.MemorySegment;

import com.github.luben.zstd.Zstd;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Compression information for batch-level compression of row-based log record batches. */
public class LogCompressionInfo {

    public static final LogCompressionInfo NO_COMPRESSION =
            new LogCompressionInfo(LogCompressionType.NONE, -1);

    private final LogCompressionType compressionType;
    private final int compressionLevel;

    public LogCompressionInfo(LogCompressionType compressionType, int compressionLevel) {
        this.compressionType = compressionType;
        this.compressionLevel = compressionLevel;
    }

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

    /** Compresses {@code length} bytes of {@code src} starting at {@code offset}. */
    public byte[] compress(byte[] src, int offset, int length) {
        switch (compressionType) {
            case ZSTD:
                byte[] dst = new byte[(int) Zstd.compressBound(length)];
                long written =
                        Zstd.compressByteArray(
                                dst, 0, dst.length, src, offset, length, compressionLevel);
                if (Zstd.isError(written)) {
                    throw new IllegalStateException(
                            "ZSTD compression failed: " + Zstd.getErrorName(written));
                }
                return Arrays.copyOf(dst, (int) written);
            case LZ4_FRAME:
                try {
                    ByteArrayOutputStream out = new ByteArrayOutputStream(length / 2 + 64);
                    try (FlussLZ4BlockOutputStream lz4 = new FlussLZ4BlockOutputStream(out)) {
                        lz4.write(src, offset, length);
                    }
                    return out.toByteArray();
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
        byte[] src = new byte[length];
        segment.get(offset, src, 0, length);
        byte[] dst = new byte[uncompressedSize];
        switch (type) {
            case ZSTD:
                long read = Zstd.decompressByteArray(dst, 0, dst.length, src, 0, length);
                if (Zstd.isError(read) || read != uncompressedSize) {
                    throw new IllegalStateException(
                            "ZSTD decompression failed: "
                                    + (Zstd.isError(read) ? Zstd.getErrorName(read) : read));
                }
                return dst;
            case LZ4_FRAME:
                try (FlussLZ4BlockInputStream lz4 =
                        new FlussLZ4BlockInputStream(ByteBuffer.wrap(src))) {
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
}
