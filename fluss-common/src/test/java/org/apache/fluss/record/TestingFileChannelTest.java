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

import org.apache.fluss.utils.FileUtils;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the read-range assertions in {@link TestingFileChannel}. */
class TestingFileChannelTest {
    @TempDir private Path tempDir;

    @ParameterizedTest
    @ValueSource(ints = {5, Integer.MAX_VALUE})
    void testReadsWithinAdjacentAndOverlappingRanges(int maxBytesPerRead) throws Exception {
        Path file = Files.write(tempDir.resolve("allowed.log"), new byte[80]);
        List<long[]> ranges =
                Arrays.asList(
                        new long[] {16, 32},
                        new long[] {0, 8},
                        new long[] {4, 12},
                        new long[] {12, 16},
                        new long[] {40, 48});
        try (TestingFileChannel channel = new TestingFileChannel(FileChannel.open(file))) {
            channel.setMaxBytesPerRead(maxBytesPerRead);
            FileUtils.readFullyOrFail(channel, ByteBuffer.allocate(32), 17, "adjacent ranges");
            FileUtils.readFullyOrFail(channel, ByteBuffer.allocate(8), 57, "separate range");
            channel.assertReadsWithin(17, ranges);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {5, Integer.MAX_VALUE})
    void testReadsAcrossUnselectedGapAreRejected(int maxBytesPerRead) throws Exception {
        Path file = Files.write(tempDir.resolve("gap.log"), new byte[80]);
        List<long[]> ranges = Arrays.asList(new long[] {0, 8}, new long[] {9, 32});
        try (TestingFileChannel channel = new TestingFileChannel(FileChannel.open(file))) {
            channel.setMaxBytesPerRead(maxBytesPerRead);
            FileUtils.readFullyOrFail(channel, ByteBuffer.allocate(32), 17, "range with a gap");
            assertThatThrownBy(() -> channel.assertReadsWithin(17, ranges))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("must be in the allowed ranges");
        }
    }
}
