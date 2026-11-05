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
import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.memory.MemorySegment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LogCompressionInfo}. */
class LogCompressionInfoTest {

    @Test
    void testFromConf() {
        Configuration conf = new Configuration();
        assertThat(LogCompressionInfo.fromConf(conf)).isEqualTo(LogCompressionInfo.NO_COMPRESSION);

        conf.set(ConfigOptions.TABLE_LOG_COMPACTED_COMPRESSION_TYPE, LogCompressionType.ZSTD);
        assertThat(LogCompressionInfo.fromConf(conf))
                .isEqualTo(new LogCompressionInfo(LogCompressionType.ZSTD, 3));

        conf.set(ConfigOptions.TABLE_LOG_COMPACTED_COMPRESSION_ZSTD_LEVEL, 9);
        assertThat(LogCompressionInfo.fromConf(conf))
                .isEqualTo(new LogCompressionInfo(LogCompressionType.ZSTD, 9));

        conf.set(ConfigOptions.TABLE_LOG_COMPACTED_COMPRESSION_ZSTD_LEVEL, 23);
        assertThatThrownBy(() -> LogCompressionInfo.fromConf(conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid ZSTD compression level: 23");

        // the level is only used by ZSTD
        conf.set(ConfigOptions.TABLE_LOG_COMPACTED_COMPRESSION_TYPE, LogCompressionType.LZ4_FRAME);
        assertThat(LogCompressionInfo.fromConf(conf))
                .isEqualTo(new LogCompressionInfo(LogCompressionType.LZ4_FRAME, -1));
    }

    @ParameterizedTest
    @EnumSource(
            value = LogCompressionType.class,
            names = {"LZ4_FRAME", "ZSTD"})
    void testCompressAndDecompress(LogCompressionType type) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("row-").append(i % 7).append(';');
        }
        byte[] raw = sb.toString().getBytes(StandardCharsets.UTF_8);
        LogCompressionInfo info =
                new LogCompressionInfo(type, type == LogCompressionType.ZSTD ? 3 : -1);

        // compress a slice of a larger array into the middle of another array
        byte[] padded = new byte[raw.length + 10];
        System.arraycopy(raw, 0, padded, 5, raw.length);
        byte[] out = new byte[raw.length + 8];
        int compressedLength = info.compress(padded, 5, raw.length, out, 8, raw.length);
        assertThat(compressedLength).isPositive().isLessThan(raw.length);

        // decompress from an offset in a heap segment
        byte[] decompressed =
                LogCompressionInfo.decompress(
                        type, MemorySegment.wrap(out), 8, compressedLength, raw.length);
        assertThat(decompressed).isEqualTo(raw);

        // decompress from an offset in an off-heap segment
        ByteBuffer direct = ByteBuffer.allocateDirect(out.length);
        direct.put(out).flip();
        decompressed =
                LogCompressionInfo.decompress(
                        type,
                        MemorySegment.wrapOffHeapMemory(direct),
                        8,
                        compressedLength,
                        raw.length);
        assertThat(decompressed).isEqualTo(raw);
    }

    @ParameterizedTest
    @EnumSource(
            value = LogCompressionType.class,
            names = {"LZ4_FRAME", "ZSTD"})
    void testCompressIntoTooSmallOutput(LogCompressionType type) {
        byte[] raw = new byte[4096];
        new Random(42).nextBytes(raw);
        LogCompressionInfo info =
                new LogCompressionInfo(type, type == LogCompressionType.ZSTD ? 3 : -1);
        byte[] out = new byte[raw.length];
        // random bytes don't compress, so the output can't fit into fewer bytes than the input
        assertThat(info.compress(raw, 0, raw.length, out, 0, raw.length - 1)).isEqualTo(-1);
    }

    @ParameterizedTest
    @EnumSource(
            value = LogCompressionType.class,
            names = {"LZ4_FRAME", "ZSTD"})
    void testDecompressCorruptInput(LogCompressionType type) {
        byte[] raw = new byte[4096];
        LogCompressionInfo info =
                new LogCompressionInfo(type, type == LogCompressionType.ZSTD ? 3 : -1);
        byte[] out = new byte[raw.length];
        int compressedLength = info.compress(raw, 0, raw.length, out, 0, raw.length);
        assertThat(compressedLength).isPositive();

        // valid compressed bytes, but the batch claims a different uncompressed size
        assertThatThrownBy(
                        () ->
                                LogCompressionInfo.decompress(
                                        type,
                                        MemorySegment.wrap(out),
                                        0,
                                        compressedLength,
                                        raw.length + 1))
                .isInstanceOf(CorruptMessageException.class);

        // bytes that are not a valid compressed stream
        byte[] garbage = new byte[64];
        new Random(7).nextBytes(garbage);
        assertThatThrownBy(
                        () ->
                                LogCompressionInfo.decompress(
                                        type,
                                        MemorySegment.wrap(garbage),
                                        0,
                                        garbage.length,
                                        raw.length))
                .isInstanceOf(CorruptMessageException.class);
    }

    @Test
    void testIdRoundTrip() {
        for (LogCompressionType type : LogCompressionType.values()) {
            assertThat(LogCompressionType.fromId(type.id())).isEqualTo(type);
        }
        assertThatThrownBy(() -> LogCompressionType.fromId(3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown log compression type id: 3");
    }
}
