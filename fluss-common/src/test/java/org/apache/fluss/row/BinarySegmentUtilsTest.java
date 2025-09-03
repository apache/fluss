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

package org.apache.fluss.row;

import org.apache.fluss.memory.MemorySegment;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.row.BinarySegmentUtils}. */
public class BinarySegmentUtilsTest {

    @Test
    public void testCopy() {
        // test copy the content of the latter Seg
        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = MemorySegment.wrap(new byte[] {0, 2, 5});
        segments[1] = MemorySegment.wrap(new byte[] {6, 12, 15});

        byte[] bytes = BinarySegmentUtils.copyToBytes(segments, 4, 2);
        assertThat(bytes).isEqualTo(new byte[] {12, 15});
    }

    @Test
    public void testEquals() {
        // test copy the content of the latter Seg
        MemorySegment[] segments1 = new MemorySegment[3];
        segments1[0] = MemorySegment.wrap(new byte[] {0, 2, 5});
        segments1[1] = MemorySegment.wrap(new byte[] {6, 12, 15});
        segments1[2] = MemorySegment.wrap(new byte[] {1, 1, 1});

        MemorySegment[] segments2 = new MemorySegment[2];
        segments2[0] = MemorySegment.wrap(new byte[] {6, 0, 2, 5});
        segments2[1] = MemorySegment.wrap(new byte[] {6, 12, 15, 18});

        assertThat(BinarySegmentUtils.equalsMultiSegments(segments1, 0, segments2, 0, 0)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 3)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 6)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 7)).isFalse();
    }

    @Test
    public void testBoundaryEquals() {
        // test var segs
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegment.wrap(new byte[32]);
        segments1[1] = MemorySegment.wrap(new byte[32]);
        MemorySegment[] segments2 = new MemorySegment[3];
        segments2[0] = MemorySegment.wrap(new byte[16]);
        segments2[1] = MemorySegment.wrap(new byte[16]);
        segments2[2] = MemorySegment.wrap(new byte[16]);

        segments1[0].put(9, (byte) 1);
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 14, 14)).isFalse();
        segments2[1].put(7, (byte) 1);
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 14, 14)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 2, segments2, 16, 14)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 2, segments2, 16, 16)).isTrue();

        segments2[2].put(7, (byte) 1);
        assertThat(BinarySegmentUtils.equals(segments1, 2, segments2, 32, 14)).isTrue();
    }

    @Test
    public void testBoundaryCopy() {
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegment.wrap(new byte[32]);
        segments1[1] = MemorySegment.wrap(new byte[32]);
        segments1[0].put(15, (byte) 5);
        segments1[1].put(15, (byte) 6);

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegment.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 0, bytes, 0, 64);
            assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 0, 64)).isTrue();
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegment.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 32, bytes, 0, 14);
            assertThat(BinarySegmentUtils.equals(segments1, 32, segments2, 0, 14)).isTrue();
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegment.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 34, bytes, 0, 14);
            assertThat(BinarySegmentUtils.equals(segments1, 34, segments2, 0, 14)).isTrue();
        }
    }

    @Test
    public void testFind() {
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegment.wrap(new byte[32]);
        segments1[1] = MemorySegment.wrap(new byte[32]);
        MemorySegment[] segments2 = new MemorySegment[3];
        segments2[0] = MemorySegment.wrap(new byte[16]);
        segments2[1] = MemorySegment.wrap(new byte[16]);
        segments2[2] = MemorySegment.wrap(new byte[16]);

        assertThat(BinarySegmentUtils.find(segments1, 34, 0, segments2, 0, 0)).isEqualTo(34);
        assertThat(BinarySegmentUtils.find(segments1, 34, 0, segments2, 0, 15)).isEqualTo(-1);
    }

    @Test
    public void testBitUnSet() {
        // Test bitUnSet method with various bit patterns
        MemorySegment segment = MemorySegment.wrap(new byte[10]);

        // Set all bits to 1 first (0xFF = 11111111)
        for (int i = 0; i < 10; i++) {
            segment.put(i, (byte) 0xFF);
        }

        // Test unsetting specific bits
        BinarySegmentUtils.bitUnSet(segment, 0, 0); // First bit of first byte
        assertThat(segment.get(0) & 0xFF).isEqualTo(0xFE); // Should be 11111110

        BinarySegmentUtils.bitUnSet(segment, 0, 7); // Last bit of first byte
        assertThat(segment.get(0) & 0xFF).isEqualTo(0x7E); // Should be 01111110

        BinarySegmentUtils.bitUnSet(segment, 0, 8); // First bit of second byte
        assertThat(segment.get(1) & 0xFF).isEqualTo(0xFE); // Should be 11111110

        BinarySegmentUtils.bitUnSet(segment, 0, 15); // Last bit of second byte
        assertThat(segment.get(1) & 0xFF).isEqualTo(0x7E); // Should be 01111110

        // Test with different base offset
        BinarySegmentUtils.bitUnSet(segment, 2, 0); // First bit of third byte (offset=2)
        assertThat(segment.get(2) & 0xFF).isEqualTo(0xFE); // Should be 11111110

        // Test boundary cases - bit index at byte boundaries
        BinarySegmentUtils.bitUnSet(segment, 0, 63); // Should affect byte 7
        assertThat(segment.get(7) & 0xFF).isEqualTo(0x7F); // Should be 01111111
    }

    @Test
    public void testGetLongSingleSegment() {
        // Test getLong with single segment (fast path)
        MemorySegment segment = MemorySegment.wrap(new byte[16]);
        MemorySegment[] segments = {segment};

        // Test basic long value
        long testValue = 0x123456789ABCDEFL;
        segment.putLong(0, testValue);
        assertThat(BinarySegmentUtils.getLong(segments, 0)).isEqualTo(testValue);

        // Test with offset
        segment.putLong(8, testValue);
        assertThat(BinarySegmentUtils.getLong(segments, 8)).isEqualTo(testValue);

        // Test negative value
        long negativeValue = -123456789L;
        segment.putLong(0, negativeValue);
        assertThat(BinarySegmentUtils.getLong(segments, 0)).isEqualTo(negativeValue);

        // Test zero
        segment.putLong(0, 0L);
        assertThat(BinarySegmentUtils.getLong(segments, 0)).isEqualTo(0L);

        // Test max and min values
        segment.putLong(0, Long.MAX_VALUE);
        assertThat(BinarySegmentUtils.getLong(segments, 0)).isEqualTo(Long.MAX_VALUE);

        segment.putLong(0, Long.MIN_VALUE);
        assertThat(BinarySegmentUtils.getLong(segments, 0)).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    public void testGetLongMultiSegments() {
        // Test getLong with multiple segments (slow path)
        MemorySegment[] segments = new MemorySegment[3];
        segments[0] = MemorySegment.wrap(new byte[8]);
        segments[1] = MemorySegment.wrap(new byte[8]);
        segments[2] = MemorySegment.wrap(new byte[8]);

        // Test value spanning across segments at boundary
        long testValue = 0x123456789ABCDEFL;

        // Use setLong to properly set the value, then getLong to read it back
        // This tests the consistency between setLong and getLong across segments
        BinarySegmentUtils.setLong(segments, 6, testValue);
        assertThat(BinarySegmentUtils.getLong(segments, 6)).isEqualTo(testValue);

        // Test completely in second segment
        segments[1].putLong(0, testValue);
        assertThat(BinarySegmentUtils.getLong(segments, 8)).isEqualTo(testValue);

        // Test another value across different boundary
        long testValue2 = Long.MAX_VALUE;
        BinarySegmentUtils.setLong(segments, 12, testValue2);
        assertThat(BinarySegmentUtils.getLong(segments, 12)).isEqualTo(testValue2);

        // Test negative value across segments
        long negativeValue = Long.MIN_VALUE;
        BinarySegmentUtils.setLong(segments, 6, negativeValue);
        assertThat(BinarySegmentUtils.getLong(segments, 6)).isEqualTo(negativeValue);
    }

    @Test
    public void testSetLongSingleSegment() {
        // Test setLong with single segment
        MemorySegment segment = MemorySegment.wrap(new byte[16]);
        MemorySegment[] segments = {segment};

        long testValue = 0x123456789ABCDEFL;
        BinarySegmentUtils.setLong(segments, 0, testValue);
        assertThat(segment.getLong(0)).isEqualTo(testValue);

        // Test with offset
        BinarySegmentUtils.setLong(segments, 8, testValue);
        assertThat(segment.getLong(8)).isEqualTo(testValue);

        // Test negative value
        long negativeValue = -987654321L;
        BinarySegmentUtils.setLong(segments, 0, negativeValue);
        assertThat(segment.getLong(0)).isEqualTo(negativeValue);
    }

    @Test
    public void testSetLongMultiSegments() {
        // Test setLong with multiple segments
        MemorySegment[] segments = new MemorySegment[3];
        segments[0] = MemorySegment.wrap(new byte[8]);
        segments[1] = MemorySegment.wrap(new byte[8]);
        segments[2] = MemorySegment.wrap(new byte[8]);

        long testValue = 0x123456789ABCDEFL;

        // Test setting across segment boundary
        BinarySegmentUtils.setLong(segments, 6, testValue);

        // Verify by reading back
        assertThat(BinarySegmentUtils.getLong(segments, 6)).isEqualTo(testValue);

        // Test setting completely in second segment
        BinarySegmentUtils.setLong(segments, 8, testValue);
        assertThat(segments[1].getLong(0)).isEqualTo(testValue);
    }

    @Test
    public void testCopyFromBytesSingleSegment() {
        // Test copyFromBytes with single segment
        byte[] sourceBytes = {0x01, 0x02, 0x03, 0x04, 0x05};
        MemorySegment segment = MemorySegment.wrap(new byte[10]);
        MemorySegment[] segments = {segment};

        BinarySegmentUtils.copyFromBytes(segments, 0, sourceBytes, 0, sourceBytes.length);

        // Verify copied data
        for (int i = 0; i < sourceBytes.length; i++) {
            assertThat(segment.get(i)).isEqualTo(sourceBytes[i]);
        }

        // Test with offset
        BinarySegmentUtils.copyFromBytes(segments, 5, sourceBytes, 1, 3);
        assertThat(segment.get(5)).isEqualTo(sourceBytes[1]);
        assertThat(segment.get(6)).isEqualTo(sourceBytes[2]);
        assertThat(segment.get(7)).isEqualTo(sourceBytes[3]);
    }

    @Test
    public void testCopyFromBytesMultiSegments() {
        // Test copyFromBytes with multiple segments
        MemorySegment[] segments = new MemorySegment[3];
        segments[0] = MemorySegment.wrap(new byte[4]);
        segments[1] = MemorySegment.wrap(new byte[4]);
        segments[2] = MemorySegment.wrap(new byte[4]);

        byte[] sourceBytes = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};

        // Copy across segments
        BinarySegmentUtils.copyFromBytes(segments, 2, sourceBytes, 0, sourceBytes.length);

        // Verify by reading back
        byte[] result = BinarySegmentUtils.copyToBytes(segments, 2, sourceBytes.length);
        assertThat(result).isEqualTo(sourceBytes);

        // Test copying to start of second segment
        BinarySegmentUtils.copyFromBytes(segments, 4, sourceBytes, 0, 4);
        assertThat(segments[1].get(0)).isEqualTo(sourceBytes[0]);
        assertThat(segments[1].get(1)).isEqualTo(sourceBytes[1]);
        assertThat(segments[1].get(2)).isEqualTo(sourceBytes[2]);
        assertThat(segments[1].get(3)).isEqualTo(sourceBytes[3]);
    }

    @Test
    public void testReadDecimalData() {
        // Test readDecimalData method
        BigDecimal testDecimal = new BigDecimal("123.456");
        byte[] decimalBytes = testDecimal.unscaledValue().toByteArray();

        // Create segments with decimal data
        MemorySegment segment = MemorySegment.wrap(new byte[20]);
        MemorySegment[] segments = {segment};

        // Copy decimal bytes to segment
        segment.put(4, decimalBytes, 0, decimalBytes.length);

        // Create offsetAndSize (high 32 bits = offset, low 32 bits = size)
        long offsetAndSize = ((long) 0 << 32) | decimalBytes.length;

        Decimal result = BinarySegmentUtils.readDecimalData(segments, 4, offsetAndSize, 6, 3);
        assertThat(result.toBigDecimal()).isEqualTo(testDecimal);

        // Test with negative decimal
        BigDecimal negativeDecimal = new BigDecimal("-987.123");
        byte[] negativeBytes = negativeDecimal.unscaledValue().toByteArray();
        segment.put(10, negativeBytes, 0, negativeBytes.length);

        long negativeOffsetAndSize = ((long) 0 << 32) | negativeBytes.length;
        Decimal negativeResult =
                BinarySegmentUtils.readDecimalData(segments, 10, negativeOffsetAndSize, 6, 3);
        assertThat(negativeResult.toBigDecimal()).isEqualTo(negativeDecimal);
    }

    @Test
    public void testReadBinaryLargeData() {
        // Test readBinary for data >= 8 bytes (stored in variable part)
        byte[] testData = "Hello World Test Data!".getBytes(StandardCharsets.UTF_8);
        MemorySegment segment = MemorySegment.wrap(new byte[50]);
        MemorySegment[] segments = {segment};

        // Copy test data to segment
        segment.put(10, testData, 0, testData.length);

        // Create variablePartOffsetAndLen for large data (mark bit = 0)
        // High 32 bits = offset, low 32 bits = length
        long variablePartOffsetAndLen = ((long) 0 << 32) | testData.length;

        byte[] result = BinarySegmentUtils.readBinary(segments, 10, 0, variablePartOffsetAndLen);
        assertThat(result).isEqualTo(testData);
    }

    @Test
    public void testReadBinarySmallData() {
        // Test readBinary for data < 8 bytes (stored in fixed part)
        byte[] smallData = {0x01, 0x02, 0x03, 0x04, 0x05};

        // Create variablePartOffsetAndLen for small data (mark bit = 1)
        // Set highest bit = 1, next 7 bits = length, remaining bytes = data
        long variablePartOffsetAndLen = BinarySection.HIGHEST_FIRST_BIT;
        variablePartOffsetAndLen |= ((long) smallData.length << 56);

        // Pack data into the long value (considering endianness)
        for (int i = 0; i < smallData.length; i++) {
            if (BinarySegmentUtils.LITTLE_ENDIAN) {
                variablePartOffsetAndLen |= ((long) (smallData[i] & 0xFF)) << (i * 8);
            } else {
                variablePartOffsetAndLen |= ((long) (smallData[i] & 0xFF)) << ((6 - i) * 8);
            }
        }

        MemorySegment segment = MemorySegment.wrap(new byte[16]);
        MemorySegment[] segments = {segment};

        byte[] result = BinarySegmentUtils.readBinary(segments, 0, 0, variablePartOffsetAndLen);
        assertThat(result).isEqualTo(smallData);
    }

    @Test
    public void testReadBinaryStringLargeData() {
        // Test readBinaryString for data >= 8 bytes
        String testString = "Hello Binary String Test!";
        byte[] testBytes = testString.getBytes(StandardCharsets.UTF_8);

        MemorySegment segment = MemorySegment.wrap(new byte[50]);
        MemorySegment[] segments = {segment};

        // Copy string bytes to segment
        segment.put(5, testBytes, 0, testBytes.length);

        // Create variablePartOffsetAndLen for large data (mark bit = 0)
        long variablePartOffsetAndLen = ((long) 0 << 32) | testBytes.length;

        BinaryString result =
                BinarySegmentUtils.readBinaryString(segments, 5, 0, variablePartOffsetAndLen);
        assertThat(result.toString()).isEqualTo(testString);
    }

    @Test
    public void testReadBinaryStringSmallData() {
        // Test readBinaryString for small data < 8 bytes
        String smallString = "Hello";
        byte[] smallBytes = smallString.getBytes(StandardCharsets.UTF_8);

        MemorySegment segment = MemorySegment.wrap(new byte[16]);
        MemorySegment[] segments = {segment};

        // For small string data, we need to setup the field to contain the data directly
        // Set highest bit = 1, next 7 bits = length
        long variablePartOffsetAndLen = BinarySection.HIGHEST_FIRST_BIT;
        variablePartOffsetAndLen |= ((long) smallBytes.length << 56);

        // Put the long value at field offset
        segment.putLong(0, variablePartOffsetAndLen);

        // Put the actual string bytes right after the long (for little endian) or at offset+1 (for
        // big endian)
        int dataOffset = BinarySegmentUtils.LITTLE_ENDIAN ? 0 : 1;
        segment.put(dataOffset, smallBytes, 0, smallBytes.length);

        BinaryString result =
                BinarySegmentUtils.readBinaryString(segments, 0, 0, variablePartOffsetAndLen);
        assertThat(result.toString()).isEqualTo(smallString);
    }

    @Test
    public void testReadTimestampLtzData() {
        // Test readTimestampLtzData method
        long epochMillis = 1609459200000L; // 2021-01-01 00:00:00 UTC
        int nanoOfMillisecond = 123456;

        MemorySegment segment = MemorySegment.wrap(new byte[16]);
        MemorySegment[] segments = {segment};

        // Store millisecond at offset 0
        segment.putLong(0, epochMillis);

        // Create offsetAndNanos (high 32 bits = offset, low 32 bits = nanos)
        long offsetAndNanos = ((long) 0 << 32) | nanoOfMillisecond;

        TimestampLtz result = BinarySegmentUtils.readTimestampLtzData(segments, 0, offsetAndNanos);
        assertThat(result.getEpochMillisecond()).isEqualTo(epochMillis);
        assertThat(result.getNanoOfMillisecond()).isEqualTo(nanoOfMillisecond);

        // Test with different offset
        segment.putLong(8, epochMillis);
        long offsetAndNanos2 = ((long) 8 << 32) | nanoOfMillisecond;

        TimestampLtz result2 =
                BinarySegmentUtils.readTimestampLtzData(segments, 0, offsetAndNanos2);
        assertThat(result2.getEpochMillisecond()).isEqualTo(epochMillis);
        assertThat(result2.getNanoOfMillisecond()).isEqualTo(nanoOfMillisecond);
    }

    @Test
    public void testReadTimestampNtzData() {
        // Test readTimestampNtzData method
        long epochMillis = 1609459200000L; // 2021-01-01 00:00:00
        int nanoOfMillisecond = 789012;

        MemorySegment segment = MemorySegment.wrap(new byte[16]);
        MemorySegment[] segments = {segment};

        // Store millisecond at offset 0
        segment.putLong(0, epochMillis);

        // Create offsetAndNanos
        long offsetAndNanos = ((long) 0 << 32) | nanoOfMillisecond;

        TimestampNtz result = BinarySegmentUtils.readTimestampNtzData(segments, 0, offsetAndNanos);
        assertThat(result.getMillisecond()).isEqualTo(epochMillis);
        assertThat(result.getNanoOfMillisecond()).isEqualTo(nanoOfMillisecond);

        // Test with negative timestamp
        long negativeMillis = -86400000L; // 1969-12-31
        segment.putLong(8, negativeMillis);

        long negativeOffsetAndNanos = ((long) 8 << 32) | nanoOfMillisecond;
        TimestampNtz negativeResult =
                BinarySegmentUtils.readTimestampNtzData(segments, 0, negativeOffsetAndNanos);
        assertThat(negativeResult.getMillisecond()).isEqualTo(negativeMillis);
        assertThat(negativeResult.getNanoOfMillisecond()).isEqualTo(nanoOfMillisecond);
    }

    @Test
    public void testHashByWordsSingleSegment() {
        // Test hashByWords with single segment
        MemorySegment segment = MemorySegment.wrap(new byte[16]);
        MemorySegment[] segments = {segment};

        // Fill with test data (must be 4-byte aligned)
        segment.putInt(0, 0x12345678);
        segment.putInt(4, 0x9ABCDEF0);
        segment.putInt(8, 0x11223344);
        segment.putInt(12, 0x55667788);

        int hash1 = BinarySegmentUtils.hashByWords(segments, 0, 16);
        int hash2 = BinarySegmentUtils.hashByWords(segments, 0, 16);
        assertThat(hash1).isEqualTo(hash2); // Consistent hashing

        // Test with different offset
        int hash3 = BinarySegmentUtils.hashByWords(segments, 4, 8);
        assertThat(hash3).isNotEqualTo(hash1); // Different data should produce different hash

        // Test with same data should produce same hash
        MemorySegment segment2 = MemorySegment.wrap(new byte[16]);
        MemorySegment[] segments2 = {segment2};
        segment2.putInt(0, 0x12345678);
        segment2.putInt(4, 0x9ABCDEF0);
        segment2.putInt(8, 0x11223344);
        segment2.putInt(12, 0x55667788);

        int hash4 = BinarySegmentUtils.hashByWords(segments2, 0, 16);
        assertThat(hash4).isEqualTo(hash1);
    }

    @Test
    public void testHashByWordsMultiSegments() {
        // Test hashByWords with multiple segments
        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = MemorySegment.wrap(new byte[8]);
        segments[1] = MemorySegment.wrap(new byte[8]);

        // Fill with test data (must be 4-byte aligned)
        segments[0].putInt(0, 0x12345678);
        segments[0].putInt(4, 0x9ABCDEF0);
        segments[1].putInt(0, 0x11223344);
        segments[1].putInt(4, 0x55667788);

        int hash1 = BinarySegmentUtils.hashByWords(segments, 0, 16);

        // Test across segment boundary
        int hash2 = BinarySegmentUtils.hashByWords(segments, 4, 8);
        assertThat(hash2).isNotEqualTo(hash1);

        // Verify consistency
        int hash3 = BinarySegmentUtils.hashByWords(segments, 0, 16);
        assertThat(hash3).isEqualTo(hash1);
    }

    @Test
    public void testBitOperationsEdgeCases() {
        // Test bit operations with edge cases
        MemorySegment segment = MemorySegment.wrap(new byte[2]);

        // Test bitGet after bitUnSet
        BinarySegmentUtils.bitSet(segment, 0, 0);
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 0)).isTrue();

        BinarySegmentUtils.bitUnSet(segment, 0, 0);
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 0)).isFalse();

        // Test multiple bit operations
        BinarySegmentUtils.bitSet(segment, 0, 1);
        BinarySegmentUtils.bitSet(segment, 0, 3);
        BinarySegmentUtils.bitSet(segment, 0, 5);
        BinarySegmentUtils.bitUnSet(segment, 0, 3);

        assertThat(BinarySegmentUtils.bitGet(segment, 0, 1)).isTrue();
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 3)).isFalse();
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 5)).isTrue();
    }

    @Test
    public void testReadDecimalDataMultiSegments() {
        // Test readDecimalData with data spanning multiple segments
        BigDecimal testDecimal = new BigDecimal("999999999.123456789");
        byte[] decimalBytes = testDecimal.unscaledValue().toByteArray();

        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = MemorySegment.wrap(new byte[8]);
        segments[1] = MemorySegment.wrap(new byte[16]);

        // Copy decimal bytes starting from end of first segment
        BinarySegmentUtils.copyFromBytes(segments, 6, decimalBytes, 0, decimalBytes.length);

        // Create offsetAndSize
        long offsetAndSize = ((long) 6 << 32) | decimalBytes.length;

        Decimal result = BinarySegmentUtils.readDecimalData(segments, 0, offsetAndSize, 18, 9);
        assertThat(result.toBigDecimal()).isEqualTo(testDecimal);
    }

    @Test
    public void testReadBinaryDataEdgeCases() {
        // Test edge cases for readBinary
        MemorySegment segment = MemorySegment.wrap(new byte[20]);
        MemorySegment[] segments = {segment};

        // Test empty data
        byte[] emptyData = new byte[0];
        long emptyOffsetAndLen = ((long) 0 << 32) | 0;
        byte[] emptyResult = BinarySegmentUtils.readBinary(segments, 0, 0, emptyOffsetAndLen);
        assertThat(emptyResult).isEmpty();

        // Test single byte data in fixed part
        byte[] singleByte = {(byte) 0xAB};
        long fixedPartData = BinarySection.HIGHEST_FIRST_BIT;
        fixedPartData |= ((long) 1 << 56); // length = 1
        if (BinarySegmentUtils.LITTLE_ENDIAN) {
            fixedPartData |= (singleByte[0] & 0xFF);
        } else {
            fixedPartData |= ((long) (singleByte[0] & 0xFF)) << 48;
        }

        byte[] singleResult = BinarySegmentUtils.readBinary(segments, 0, 0, fixedPartData);
        assertThat(singleResult).isEqualTo(singleByte);

        // Test 7-byte data (maximum for fixed part)
        byte[] maxFixedData = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
        long maxFixedPartData = BinarySection.HIGHEST_FIRST_BIT;
        maxFixedPartData |= ((long) 7 << 56); // length = 7

        for (int i = 0; i < 7; i++) {
            if (BinarySegmentUtils.LITTLE_ENDIAN) {
                maxFixedPartData |= ((long) (maxFixedData[i] & 0xFF)) << (i * 8);
            } else {
                maxFixedPartData |= ((long) (maxFixedData[i] & 0xFF)) << ((6 - i) * 8);
            }
        }

        byte[] maxFixedResult = BinarySegmentUtils.readBinary(segments, 0, 0, maxFixedPartData);
        assertThat(maxFixedResult).isEqualTo(maxFixedData);
    }

    @Test
    public void testCopyFromBytesEdgeCases() {
        // Test edge cases for copyFromBytes
        MemorySegment segment = MemorySegment.wrap(new byte[10]);
        MemorySegment[] segments = {segment};

        // Test copying empty array
        byte[] emptyBytes = new byte[0];
        BinarySegmentUtils.copyFromBytes(segments, 0, emptyBytes, 0, 0);
        // Should not throw exception

        // Test copying single byte
        byte[] singleByte = {(byte) 0xFF};
        BinarySegmentUtils.copyFromBytes(segments, 5, singleByte, 0, 1);
        assertThat(segment.get(5)).isEqualTo((byte) 0xFF);

        // Test copying with source offset
        byte[] sourceData = {0x01, 0x02, 0x03, 0x04, 0x05};
        BinarySegmentUtils.copyFromBytes(segments, 0, sourceData, 2, 3);
        assertThat(segment.get(0)).isEqualTo((byte) 0x03);
        assertThat(segment.get(1)).isEqualTo((byte) 0x04);
        assertThat(segment.get(2)).isEqualTo((byte) 0x05);
    }

    @Test
    public void testHashByWordsEdgeCases() {
        // Test edge cases for hashByWords
        MemorySegment segment = MemorySegment.wrap(new byte[16]);
        MemorySegment[] segments = {segment};

        // Test with zero data
        int zeroHash = BinarySegmentUtils.hashByWords(segments, 0, 4);
        assertThat(zeroHash).isNotNull(); // Should not throw exception

        // Test with minimum aligned data (4 bytes)
        segment.putInt(0, 0x12345678);
        int minHash = BinarySegmentUtils.hashByWords(segments, 0, 4);
        assertThat(minHash).isNotNull();

        // Test hash consistency - same data should produce same hash
        MemorySegment segment2 = MemorySegment.wrap(new byte[16]);
        MemorySegment[] segments2 = {segment2};
        segment2.putInt(0, 0x12345678);
        int minHash2 = BinarySegmentUtils.hashByWords(segments2, 0, 4);
        assertThat(minHash2).isEqualTo(minHash);
    }

    @Test
    public void testTimestampDataEdgeCases() {
        // Test edge cases for timestamp methods
        MemorySegment segment = MemorySegment.wrap(new byte[16]);
        MemorySegment[] segments = {segment};

        // Test zero timestamp
        segment.putLong(0, 0L);
        long zeroOffsetAndNanos = ((long) 0 << 32) | 0;

        TimestampLtz zeroLtz =
                BinarySegmentUtils.readTimestampLtzData(segments, 0, zeroOffsetAndNanos);
        assertThat(zeroLtz.getEpochMillisecond()).isEqualTo(0L);
        assertThat(zeroLtz.getNanoOfMillisecond()).isEqualTo(0);

        TimestampNtz zeroNtz =
                BinarySegmentUtils.readTimestampNtzData(segments, 0, zeroOffsetAndNanos);
        assertThat(zeroNtz.getMillisecond()).isEqualTo(0L);
        assertThat(zeroNtz.getNanoOfMillisecond()).isEqualTo(0);

        // Test maximum nano value (999999)
        long maxNanos = 999999;
        long maxNanosOffset = ((long) 0 << 32) | maxNanos;

        TimestampLtz maxNanoLtz =
                BinarySegmentUtils.readTimestampLtzData(segments, 0, maxNanosOffset);
        assertThat(maxNanoLtz.getNanoOfMillisecond()).isEqualTo(maxNanos);

        TimestampNtz maxNanoNtz =
                BinarySegmentUtils.readTimestampNtzData(segments, 0, maxNanosOffset);
        assertThat(maxNanoNtz.getNanoOfMillisecond()).isEqualTo(maxNanos);
    }

    @Test
    public void testLongOperationsWithVariousValues() {
        // Test long operations with various special values
        MemorySegment[] segments = new MemorySegment[3];
        segments[0] = MemorySegment.wrap(new byte[4]);
        segments[1] = MemorySegment.wrap(new byte[4]);
        segments[2] = MemorySegment.wrap(new byte[4]);

        // Test powers of 2
        long[] testValues = {
            1L,
            2L,
            4L,
            8L,
            16L,
            32L,
            64L,
            128L,
            256L,
            512L,
            1024L,
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            0L,
            -1L,
            -123456789L
        };

        for (long testValue : testValues) {
            // Test across segment boundary (offset 2, spans first and second segment)
            BinarySegmentUtils.setLong(segments, 2, testValue);
            assertThat(BinarySegmentUtils.getLong(segments, 2)).isEqualTo(testValue);
        }
    }
}
