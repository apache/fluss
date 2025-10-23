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

package org.apache.fluss.utils;

import org.apache.fluss.memory.MemorySegment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.apache.fluss.utils.UnsafeUtils.BYTE_ARRAY_BASE_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MurmurHashUtils}. */
class MurmurHashUtilsTest {

    @Test
    void testHashUnsafeBytesByWords() {
        // Test with aligned length (4 bytes)
        byte[] data = {1, 2, 3, 4};
        int hash1 = MurmurHashUtils.hashUnsafeBytesByWords(data, BYTE_ARRAY_BASE_OFFSET, 4);

        // Test that it produces a non-zero hash
        assertThat(hash1).isNotZero();

        // Test with 8-byte aligned data
        byte[] data8 = {1, 2, 3, 4, 5, 6, 7, 8};
        int hash8 = MurmurHashUtils.hashUnsafeBytesByWords(data8, BYTE_ARRAY_BASE_OFFSET, 8);
        assertThat(hash8).isNotZero();

        // Test with 12-byte aligned data
        byte[] data12 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        int hash12 = MurmurHashUtils.hashUnsafeBytesByWords(data12, BYTE_ARRAY_BASE_OFFSET, 12);
        assertThat(hash12).isNotZero();
    }

    @Test
    void testHashBytesByWords() {
        // Test with aligned length (4 bytes)
        byte[] data = {1, 2, 3, 4};
        MemorySegment segment = MemorySegment.wrap(data);
        int hash1 = MurmurHashUtils.hashBytesByWords(segment, 0, 4);

        // Test that it produces a non-zero hash
        assertThat(hash1).isNotZero();

        // Test with 8-byte aligned data
        byte[] data8 = {1, 2, 3, 4, 5, 6, 7, 8};
        MemorySegment segment8 = MemorySegment.wrap(data8);
        int hash8 = MurmurHashUtils.hashBytesByWords(segment8, 0, 8);
        assertThat(hash8).isNotZero();

        // Test with 12-byte aligned data
        byte[] data12 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        MemorySegment segment12 = MemorySegment.wrap(data12);
        int hash12 = MurmurHashUtils.hashBytesByWords(segment12, 0, 12);
        assertThat(hash12).isNotZero();
    }

    @Test
    void testConsistencyBetweenUnsafeAndMemorySegmentMethods() {
        // Test that hashUnsafeBytesByWords and hashBytesByWords produce the same results
        // for the same data
        byte[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        MemorySegment segment = MemorySegment.wrap(data);

        int unsafeHash = MurmurHashUtils.hashUnsafeBytesByWords(data, BYTE_ARRAY_BASE_OFFSET, 12);
        int segmentHash = MurmurHashUtils.hashBytesByWords(segment, 0, 12);

        assertThat(unsafeHash).isEqualTo(segmentHash);
    }

    @Test
    void testConsistencyWithExistingMethods() {
        // Test consistency with existing hashUnsafeBytes method for word-aligned data
        byte[] data = {1, 2, 3, 4, 5, 6, 7, 8};

        // For word-aligned data, the "ByWords" methods should produce the same result
        // as the regular methods when the data length is already aligned
        int wordHash = MurmurHashUtils.hashUnsafeBytesByWords(data, BYTE_ARRAY_BASE_OFFSET, 8);
        int regularHash = MurmurHashUtils.hashUnsafeBytes(data, BYTE_ARRAY_BASE_OFFSET, 8);

        // They should produce the same result for aligned data
        assertThat(wordHash).isEqualTo(regularHash);

        // Test with MemorySegment
        MemorySegment segment = MemorySegment.wrap(data);
        int segmentWordHash = MurmurHashUtils.hashBytesByWords(segment, 0, 8);
        int segmentRegularHash = MurmurHashUtils.hashBytes(segment, 0, 8);

        assertThat(segmentWordHash).isEqualTo(segmentRegularHash);
    }

    @ParameterizedTest
    @ValueSource(ints = {4, 8, 12, 16, 20, 24, 32, 64, 128})
    void testHashUnsafeBytesByWordsWithDifferentAlignedSizes(int size) {
        byte[] data = new byte[size];
        // Fill with predictable data
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }

        int hash = MurmurHashUtils.hashUnsafeBytesByWords(data, BYTE_ARRAY_BASE_OFFSET, size);
        assertThat(hash).isNotZero();

        // Test consistency - same data should produce same hash
        int hash2 = MurmurHashUtils.hashUnsafeBytesByWords(data, BYTE_ARRAY_BASE_OFFSET, size);
        assertThat(hash).isEqualTo(hash2);
    }

    @ParameterizedTest
    @ValueSource(ints = {4, 8, 12, 16, 20, 24, 32, 64, 128})
    void testHashBytesByWordsWithDifferentAlignedSizes(int size) {
        byte[] data = new byte[size];
        // Fill with predictable data
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }

        MemorySegment segment = MemorySegment.wrap(data);
        int hash = MurmurHashUtils.hashBytesByWords(segment, 0, size);
        assertThat(hash).isNotZero();

        // Test consistency - same data should produce same hash
        int hash2 = MurmurHashUtils.hashBytesByWords(segment, 0, size);
        assertThat(hash).isEqualTo(hash2);
    }

    @Test
    void testHashDistribution() {
        // Test that different data produces different hashes (good distribution)
        int hashCount = 1000;
        int[] hashes = new int[hashCount];

        for (int i = 0; i < hashCount; i++) {
            byte[] data = new byte[8];
            // Create different data for each iteration
            data[0] = (byte) (i & 0xFF);
            data[1] = (byte) ((i >> 8) & 0xFF);
            data[2] = (byte) ((i >> 16) & 0xFF);
            data[3] = (byte) ((i >> 24) & 0xFF);
            data[4] = (byte) i;
            data[5] = (byte) (i + 1);
            data[6] = (byte) (i + 2);
            data[7] = (byte) (i + 3);

            hashes[i] = MurmurHashUtils.hashUnsafeBytesByWords(data, BYTE_ARRAY_BASE_OFFSET, 8);
        }

        // Check that we have good distribution - most hashes should be unique
        long uniqueHashes = Arrays.stream(hashes).distinct().count();
        // Expect at least 95% unique hashes for good distribution
        assertThat(uniqueHashes).isGreaterThan((long) (hashCount * 0.95));
    }

    @Test
    void testWithOffset() {
        // Test hashUnsafeBytesByWords with different offset
        byte[] data = {0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8};

        int hash1 = MurmurHashUtils.hashUnsafeBytesByWords(data, BYTE_ARRAY_BASE_OFFSET + 4, 8);

        byte[] expectedData = {1, 2, 3, 4, 5, 6, 7, 8};
        int hash2 = MurmurHashUtils.hashUnsafeBytesByWords(expectedData, BYTE_ARRAY_BASE_OFFSET, 8);

        assertThat(hash1).isEqualTo(hash2);

        // Test with MemorySegment
        MemorySegment segment = MemorySegment.wrap(data);
        int segmentHash = MurmurHashUtils.hashBytesByWords(segment, 4, 8);
        assertThat(segmentHash).isEqualTo(hash1);
    }

    @Test
    void testEmptyData() {
        // Test with zero-length data
        byte[] emptyData = new byte[0];
        int hash = MurmurHashUtils.hashUnsafeBytesByWords(emptyData, BYTE_ARRAY_BASE_OFFSET, 0);

        // Should produce a consistent hash for empty data
        int hash2 = MurmurHashUtils.hashUnsafeBytesByWords(emptyData, BYTE_ARRAY_BASE_OFFSET, 0);
        assertThat(hash).isEqualTo(hash2);

        // Test with MemorySegment
        MemorySegment segment = MemorySegment.wrap(emptyData);
        int segmentHash = MurmurHashUtils.hashBytesByWords(segment, 0, 0);
        assertThat(segmentHash).isEqualTo(hash);
    }

    @Test
    void testRandomData() {
        Random random = new Random(42); // Fixed seed for reproducible tests

        for (int i = 0; i < 100; i++) {
            // Generate random aligned size
            int size = (random.nextInt(16) + 1) * 4; // 4, 8, 12, ..., 64 bytes
            byte[] data = new byte[size];
            random.nextBytes(data);

            int unsafeHash =
                    MurmurHashUtils.hashUnsafeBytesByWords(data, BYTE_ARRAY_BASE_OFFSET, size);

            MemorySegment segment = MemorySegment.wrap(data);
            int segmentHash = MurmurHashUtils.hashBytesByWords(segment, 0, size);

            // Both methods should produce the same result
            assertThat(unsafeHash).isEqualTo(segmentHash);
        }
    }

    @Test
    void testStringData() {
        // Test with string data converted to bytes
        String[] testStrings = {
            "hello", "world", "test", "data", "murmur", "hash", "function", "testing"
        };

        for (String str : testStrings) {
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);

            // Pad to word alignment if needed
            int alignedSize = ((bytes.length + 3) / 4) * 4;
            byte[] alignedBytes = new byte[alignedSize];
            System.arraycopy(bytes, 0, alignedBytes, 0, bytes.length);

            int unsafeHash =
                    MurmurHashUtils.hashUnsafeBytesByWords(
                            alignedBytes, BYTE_ARRAY_BASE_OFFSET, alignedSize);

            MemorySegment segment = MemorySegment.wrap(alignedBytes);
            int segmentHash = MurmurHashUtils.hashBytesByWords(segment, 0, alignedSize);

            assertThat(unsafeHash).isEqualTo(segmentHash);
            assertThat(unsafeHash).isNotZero();
        }
    }

    @Test
    void testHashStability() {
        // Test that hash values are stable across multiple calls
        byte[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

        int expectedHash = MurmurHashUtils.hashUnsafeBytesByWords(data, BYTE_ARRAY_BASE_OFFSET, 16);

        // Call multiple times and verify same result
        for (int i = 0; i < 10; i++) {
            int hash = MurmurHashUtils.hashUnsafeBytesByWords(data, BYTE_ARRAY_BASE_OFFSET, 16);
            assertThat(hash).isEqualTo(expectedHash);

            MemorySegment segment = MemorySegment.wrap(data);
            int segmentHash = MurmurHashUtils.hashBytesByWords(segment, 0, 16);
            assertThat(segmentHash).isEqualTo(expectedHash);
        }
    }

    @Test
    void testDifferentDataProducesDifferentHashes() {
        // Test that changing even one byte produces a different hash
        byte[] data1 = {1, 2, 3, 4, 5, 6, 7, 8};
        byte[] data2 = {1, 2, 3, 4, 5, 6, 7, 9}; // Last byte different

        int hash1 = MurmurHashUtils.hashUnsafeBytesByWords(data1, BYTE_ARRAY_BASE_OFFSET, 8);
        int hash2 = MurmurHashUtils.hashUnsafeBytesByWords(data2, BYTE_ARRAY_BASE_OFFSET, 8);

        assertThat(hash1).isNotEqualTo(hash2);

        // Test with MemorySegment
        MemorySegment segment1 = MemorySegment.wrap(data1);
        MemorySegment segment2 = MemorySegment.wrap(data2);

        int segmentHash1 = MurmurHashUtils.hashBytesByWords(segment1, 0, 8);
        int segmentHash2 = MurmurHashUtils.hashBytesByWords(segment2, 0, 8);

        assertThat(segmentHash1).isNotEqualTo(segmentHash2);
        assertThat(segmentHash1).isEqualTo(hash1);
        assertThat(segmentHash2).isEqualTo(hash2);
    }
}
