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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;

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

        // compress a slice of a larger array
        byte[] padded = new byte[raw.length + 10];
        System.arraycopy(raw, 0, padded, 5, raw.length);
        byte[] compressed = info.compress(padded, 5, raw.length);
        assertThat(compressed.length).isLessThan(raw.length);

        // decompress from an offset in a segment
        byte[] segmentBytes = new byte[compressed.length + 8];
        System.arraycopy(compressed, 0, segmentBytes, 8, compressed.length);
        byte[] decompressed =
                LogCompressionInfo.decompress(
                        type, MemorySegment.wrap(segmentBytes), 8, compressed.length, raw.length);
        assertThat(decompressed).isEqualTo(raw);
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
