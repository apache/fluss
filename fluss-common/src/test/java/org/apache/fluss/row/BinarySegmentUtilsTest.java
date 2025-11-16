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
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void testHash() {
        // test hash with single segment
        MemorySegment[] segments1 = new MemorySegment[1];
        segments1[0] = MemorySegment.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        MemorySegment[] segments2 = new MemorySegment[2];
        segments2[0] = MemorySegment.wrap(new byte[] {1, 2, 3});
        segments2[1] = MemorySegment.wrap(new byte[] {4, 5, 6, 7, 8});

        // Hash values should be equal for same data
        int hash1 = BinarySegmentUtils.hash(segments1, 0, 8);
        int hash2 = BinarySegmentUtils.hash(segments2, 0, 8);
        assertThat(hash1).isEqualTo(hash2);

        // Different data should produce different hash
        MemorySegment[] segments3 = new MemorySegment[1];
        segments3[0] = MemorySegment.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7, 9});
        int hash3 = BinarySegmentUtils.hash(segments3, 0, 8);
        assertThat(hash1).isNotEqualTo(hash3);

        // Test hash with offset
        int hashOffset = BinarySegmentUtils.hash(segments1, 1, 7);
        assertThat(hashOffset).isNotEqualTo(hash1);
    }

    @Test
    public void testHashByWords() {
        // test hashByWords with data aligned to 4 bytes
        MemorySegment[] segments1 = new MemorySegment[1];
        segments1[0] = MemorySegment.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        MemorySegment[] segments2 = new MemorySegment[2];
        segments2[0] = MemorySegment.wrap(new byte[] {1, 2, 3, 4});
        segments2[1] = MemorySegment.wrap(new byte[] {5, 6, 7, 8});

        // Hash values should be equal for same data
        int hash1 = BinarySegmentUtils.hashByWords(segments1, 0, 8);
        int hash2 = BinarySegmentUtils.hashByWords(segments2, 0, 8);
        assertThat(hash1).isEqualTo(hash2);

        // Test with 4-byte boundary
        int hash4Bytes = BinarySegmentUtils.hashByWords(segments1, 0, 4);
        assertThat(hash4Bytes).isNotEqualTo(hash1);
    }

    @Test
    public void testCopyToView() throws IOException {
        // test copyToView with single segment
        MemorySegment[] segments = new MemorySegment[1];
        segments[0] = MemorySegment.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        MemorySegmentOutputView outputView = new MemorySegmentOutputView(32);
        BinarySegmentUtils.copyToView(segments, 0, 8, outputView);

        byte[] result = outputView.getCopyOfBuffer();
        assertThat(result).hasSize(8);
        assertThat(result).isEqualTo(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        // test copyToView with multiple segments
        MemorySegment[] multiSegments = new MemorySegment[2];
        multiSegments[0] = MemorySegment.wrap(new byte[] {10, 20, 30});
        multiSegments[1] = MemorySegment.wrap(new byte[] {40, 50, 60, 70, 80});

        MemorySegmentOutputView multiOutputView = new MemorySegmentOutputView(32);
        BinarySegmentUtils.copyToView(multiSegments, 0, 8, multiOutputView);

        byte[] multiResult = multiOutputView.getCopyOfBuffer();
        assertThat(multiResult).hasSize(8);
        assertThat(multiResult).isEqualTo(new byte[] {10, 20, 30, 40, 50, 60, 70, 80});

        // test copyToView with offset
        MemorySegmentOutputView offsetOutputView = new MemorySegmentOutputView(32);
        BinarySegmentUtils.copyToView(multiSegments, 2, 4, offsetOutputView);

        byte[] offsetResult = offsetOutputView.getCopyOfBuffer();
        assertThat(offsetResult).hasSize(4);
        assertThat(offsetResult).isEqualTo(new byte[] {30, 40, 50, 60});
    }

    @Test
    public void testBitOperations() {
        MemorySegment segment = MemorySegment.wrap(new byte[8]);

        // Test bitSet and bitGet
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 0)).isFalse();
        BinarySegmentUtils.bitSet(segment, 0, 0);
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 0)).isTrue();

        // Test different bit positions
        BinarySegmentUtils.bitSet(segment, 0, 7); // bit 7 in first byte
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 7)).isTrue();

        BinarySegmentUtils.bitSet(segment, 0, 8); // bit 0 in second byte
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 8)).isTrue();

        BinarySegmentUtils.bitSet(segment, 0, 15); // bit 7 in second byte
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 15)).isTrue();

        // Test bitUnSet
        BinarySegmentUtils.bitUnSet(segment, 0, 0);
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 0)).isFalse();

        BinarySegmentUtils.bitUnSet(segment, 0, 7);
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 7)).isFalse();

        // Test with base offset
        BinarySegmentUtils.bitSet(segment, 2, 0); // bit 0 in byte at offset 2
        assertThat(BinarySegmentUtils.bitGet(segment, 2, 0)).isTrue();
        assertThat(BinarySegmentUtils.bitGet(segment, 0, 16))
                .isTrue(); // same bit, different base offset

        BinarySegmentUtils.bitUnSet(segment, 2, 0);
        assertThat(BinarySegmentUtils.bitGet(segment, 2, 0)).isFalse();
    }

    @Test
    public void testLongOperations() {
        // Test getLong and setLong with single segment
        MemorySegment[] segments = new MemorySegment[1];
        segments[0] = MemorySegment.wrap(new byte[16]);

        long testValue = 0x123456789ABCDEF0L;
        BinarySegmentUtils.setLong(segments, 0, testValue);
        long retrievedValue = BinarySegmentUtils.getLong(segments, 0);
        assertThat(retrievedValue).isEqualTo(testValue);

        // Test with offset
        long testValue2 = 0xFEDCBA9876543210L;
        BinarySegmentUtils.setLong(segments, 8, testValue2);
        long retrievedValue2 = BinarySegmentUtils.getLong(segments, 8);
        assertThat(retrievedValue2).isEqualTo(testValue2);

        // Test with multiple segments - cross boundary
        MemorySegment[] multiSegments = new MemorySegment[2];
        multiSegments[0] = MemorySegment.wrap(new byte[6]); // 6 bytes, so long will cross boundary
        multiSegments[1] = MemorySegment.wrap(new byte[10]);

        long crossBoundaryValue = 0x1122334455667788L;
        BinarySegmentUtils.setLong(
                multiSegments,
                2,
                crossBoundaryValue); // starts at byte 2, crosses to second segment
        long retrievedCrossBoundaryValue = BinarySegmentUtils.getLong(multiSegments, 2);
        assertThat(retrievedCrossBoundaryValue).isEqualTo(crossBoundaryValue);

        // Test multiple long values in multiple segments
        MemorySegment[] largeSegments = new MemorySegment[3];
        largeSegments[0] = MemorySegment.wrap(new byte[8]);
        largeSegments[1] = MemorySegment.wrap(new byte[8]);
        largeSegments[2] = MemorySegment.wrap(new byte[8]);

        long[] testValues = {0x1111111111111111L, 0x2222222222222222L, 0x3333333333333333L};
        for (int i = 0; i < testValues.length; i++) {
            BinarySegmentUtils.setLong(largeSegments, i * 8, testValues[i]);
        }

        for (int i = 0; i < testValues.length; i++) {
            long retrieved = BinarySegmentUtils.getLong(largeSegments, i * 8);
            assertThat(retrieved).isEqualTo(testValues[i]);
        }
    }

    @Test
    public void testCopyFromBytes() {
        // Test copyFromBytes with single segment
        MemorySegment[] segments = new MemorySegment[1];
        segments[0] = MemorySegment.wrap(new byte[16]);

        byte[] sourceBytes = {1, 2, 3, 4, 5, 6, 7, 8};
        BinarySegmentUtils.copyFromBytes(segments, 0, sourceBytes, 0, 8);

        // Verify the data was copied correctly
        for (int i = 0; i < 8; i++) {
            assertThat(segments[0].get(i)).isEqualTo(sourceBytes[i]);
        }

        // Test copyFromBytes with offset in segments
        byte[] sourceBytes2 = {10, 20, 30, 40};
        BinarySegmentUtils.copyFromBytes(segments, 8, sourceBytes2, 0, 4);

        for (int i = 0; i < 4; i++) {
            assertThat(segments[0].get(8 + i)).isEqualTo(sourceBytes2[i]);
        }

        // Test copyFromBytes with multiple segments
        MemorySegment[] multiSegments = new MemorySegment[2];
        multiSegments[0] = MemorySegment.wrap(new byte[5]);
        multiSegments[1] = MemorySegment.wrap(new byte[10]);

        byte[] sourceBytes3 = {11, 12, 13, 14, 15, 16, 17, 18};
        BinarySegmentUtils.copyFromBytes(multiSegments, 0, sourceBytes3, 0, 8);

        // Verify first segment
        for (int i = 0; i < 5; i++) {
            assertThat(multiSegments[0].get(i)).isEqualTo(sourceBytes3[i]);
        }
        // Verify second segment
        for (int i = 0; i < 3; i++) {
            assertThat(multiSegments[1].get(i)).isEqualTo(sourceBytes3[5 + i]);
        }

        // Test with byte array offset
        byte[] sourceBytes4 = {100, 101, 102, 103, 104, 105};
        BinarySegmentUtils.copyFromBytes(
                multiSegments,
                6,
                sourceBytes4,
                2,
                4); // copy from sourceBytes4[2:6] to segments starting at offset 6

        for (int i = 0; i < 4; i++) {
            if (i < 4) { // remaining bytes in second segment
                assertThat(multiSegments[1].get(6 - 5 + i))
                        .isEqualTo(sourceBytes4[2 + i]); // offset 6 - first segment size (5) = 1 in
                // second segment
            }
        }
    }

    @Test
    public void testGetBytes() {
        // Test getBytes with single segment - heap memory
        byte[] originalBytes = {1, 2, 3, 4, 5, 6, 7, 8};
        MemorySegment[] segments = new MemorySegment[1];
        segments[0] = MemorySegment.wrap(originalBytes);

        byte[] result = BinarySegmentUtils.getBytes(segments, 0, 8);
        assertThat(result).isEqualTo(originalBytes);

        // Test getBytes with single segment - partial data
        byte[] partialResult = BinarySegmentUtils.getBytes(segments, 2, 4);
        assertThat(partialResult).isEqualTo(new byte[] {3, 4, 5, 6});

        // Test getBytes with multiple segments
        MemorySegment[] multiSegments = new MemorySegment[2];
        multiSegments[0] = MemorySegment.wrap(new byte[] {10, 20, 30});
        multiSegments[1] = MemorySegment.wrap(new byte[] {40, 50, 60, 70, 80});

        byte[] multiResult = BinarySegmentUtils.getBytes(multiSegments, 0, 8);
        assertThat(multiResult).isEqualTo(new byte[] {10, 20, 30, 40, 50, 60, 70, 80});

        // Test getBytes with offset in multiple segments
        byte[] offsetMultiResult = BinarySegmentUtils.getBytes(multiSegments, 2, 4);
        assertThat(offsetMultiResult).isEqualTo(new byte[] {30, 40, 50, 60});
    }

    @Test
    public void testAllocateReuseMethods() {
        // Test allocateReuseBytes with small size - should reuse thread local buffer
        byte[] bytes1 = BinarySegmentUtils.allocateReuseBytes(100);
        assertThat(bytes1).isNotNull();
        assertThat(bytes1.length).isGreaterThanOrEqualTo(100);

        byte[] bytes2 = BinarySegmentUtils.allocateReuseBytes(200);
        assertThat(bytes2).isNotNull();
        assertThat(bytes2.length).isGreaterThanOrEqualTo(200);
        // Should reuse the same buffer if within MAX_BYTES_LENGTH

        // Test allocateReuseBytes with large size - should create new array
        byte[] bytes3 =
                BinarySegmentUtils.allocateReuseBytes(70000); // larger than MAX_BYTES_LENGTH (64K)
        assertThat(bytes3).isNotNull();
        assertThat(bytes3.length).isEqualTo(70000);

        // Test allocateReuseChars with small size
        char[] chars1 = BinarySegmentUtils.allocateReuseChars(100);
        assertThat(chars1).isNotNull();
        assertThat(chars1.length).isGreaterThanOrEqualTo(100);

        char[] chars2 = BinarySegmentUtils.allocateReuseChars(200);
        assertThat(chars2).isNotNull();
        assertThat(chars2.length).isGreaterThanOrEqualTo(200);

        // Test allocateReuseChars with large size - should create new array
        char[] chars3 =
                BinarySegmentUtils.allocateReuseChars(40000); // larger than MAX_CHARS_LENGTH (32K)
        assertThat(chars3).isNotNull();
        assertThat(chars3.length).isEqualTo(40000);

        // Test that different thread local values work
        byte[] sameSize1 = BinarySegmentUtils.allocateReuseBytes(100);
        byte[] sameSize2 = BinarySegmentUtils.allocateReuseBytes(100);
        // Should return the same reference within thread
        assertThat(sameSize1).isSameAs(sameSize2);
    }

    @Test
    public void testReadDataTypes() {
        // Test readDecimalData
        Decimal originalDecimal = Decimal.fromBigDecimal(new BigDecimal("123.45"), 5, 2);
        byte[] decimalBytes = originalDecimal.toUnscaledBytes();

        MemorySegment[] decimalSegments = new MemorySegment[1];
        decimalSegments[0] =
                MemorySegment.wrap(
                        new byte[decimalBytes.length + 8]); // extra space for offset data

        // Store decimal bytes at offset 4
        decimalSegments[0].put(4, decimalBytes, 0, decimalBytes.length);

        // Create offsetAndSize - offset in high 32 bits, size in low 32 bits
        long offsetAndSize = ((long) 4 << 32) | decimalBytes.length;

        Decimal readDecimal =
                BinarySegmentUtils.readDecimalData(decimalSegments, 0, offsetAndSize, 5, 2);
        assertThat(readDecimal).isEqualTo(originalDecimal);

        // Test readTimestampLtzData
        long testMillis = 1698235273182L;
        int nanoOfMillisecond = 123456;
        TimestampLtz originalTimestampLtz =
                TimestampLtz.fromEpochMillis(testMillis, nanoOfMillisecond);

        MemorySegment[] timestampSegments = new MemorySegment[1];
        timestampSegments[0] = MemorySegment.wrap(new byte[16]);

        // Store millisecond at offset 8
        BinarySegmentUtils.setLong(timestampSegments, 8, testMillis);

        // Create offsetAndNanos - offset in high 32 bits, nanoseconds in low 32 bits
        long offsetAndNanos = ((long) 8 << 32) | nanoOfMillisecond;

        TimestampLtz readTimestampLtz =
                BinarySegmentUtils.readTimestampLtzData(timestampSegments, 0, offsetAndNanos);
        assertThat(readTimestampLtz).isEqualTo(originalTimestampLtz);

        // Test readTimestampNtzData
        TimestampNtz originalTimestampNtz = TimestampNtz.fromMillis(testMillis, nanoOfMillisecond);

        TimestampNtz readTimestampNtz =
                BinarySegmentUtils.readTimestampNtzData(timestampSegments, 0, offsetAndNanos);
        assertThat(readTimestampNtz).isEqualTo(originalTimestampNtz);
    }

    @Test
    public void testReadBinaryData() {
        // Test readBinary with complete write-read cycle
        // Test small binary data (inline storage, < 8 bytes)
        byte[] smallBinary = {1, 2, 3, 4};
        AlignedRow smallRow = new AlignedRow(1);
        AlignedRowWriter smallWriter = new AlignedRowWriter(smallRow);
        smallWriter.writeBinary(0, smallBinary);
        smallWriter.complete();

        // Calculate field offset based on AlignedRow structure
        int arity = 1;
        int headerSizeInBits = 8; // AlignedRow.HEADER_SIZE_IN_BITS
        int nullBitsSizeInBytes = ((arity + 63 + headerSizeInBits) / 64) * 8;
        int fieldOffset = smallRow.getOffset() + nullBitsSizeInBytes + 0 * 8; // pos = 0

        // Get the offset and length information stored at field offset
        long offsetAndLen = smallRow.getSegments()[0].getLong(fieldOffset);

        // Use BinarySegmentUtils.readBinary to read the data
        byte[] readSmallBinary =
                BinarySegmentUtils.readBinary(
                        smallRow.getSegments(), smallRow.getOffset(), fieldOffset, offsetAndLen);

        // Verify the read data matches original data
        assertThat(readSmallBinary).isEqualTo(smallBinary);
        // Also verify it matches AlignedRow's built-in method
        assertThat(readSmallBinary).isEqualTo(smallRow.getBytes(0));

        // Test large binary data (external storage, >= 8 bytes)
        byte[] largeBinary = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        AlignedRow largeRow = new AlignedRow(1);
        AlignedRowWriter largeWriter = new AlignedRowWriter(largeRow);
        largeWriter.writeBinary(0, largeBinary);
        largeWriter.complete();

        // Calculate field offset for the large row
        int largeFieldOffset = largeRow.getOffset() + nullBitsSizeInBytes + 0 * 8; // pos = 0

        // Get the offset and length information
        long largeOffsetAndLen = largeRow.getSegments()[0].getLong(largeFieldOffset);

        // Use BinarySegmentUtils.readBinary to read the large data
        byte[] readLargeBinary =
                BinarySegmentUtils.readBinary(
                        largeRow.getSegments(),
                        largeRow.getOffset(),
                        largeFieldOffset,
                        largeOffsetAndLen);

        // Verify the read data matches original data
        assertThat(readLargeBinary).isEqualTo(largeBinary);
        // Also verify it matches AlignedRow's built-in method
        assertThat(readLargeBinary).isEqualTo(largeRow.getBytes(0));
    }

    @Test
    public void testReadBinaryString() {
        // Test readBinaryString - small string stored inline
        String smallStr = "hi";
        byte[] smallStrBytes = smallStr.getBytes();

        MemorySegment[] segments = new MemorySegment[1];
        segments[0] = MemorySegment.wrap(new byte[16]);

        // For inline storage, the actual data is stored in the segment at fieldOffset
        int fieldOffset = 8;
        segments[0].put(fieldOffset, smallStrBytes, 0, smallStrBytes.length);

        // For inline storage: highest bit set + length in bits 56-62
        long inlineData = 0x8000000000000000L; // highest bit set
        inlineData |= ((long) smallStrBytes.length << 56); // length in bits 56-62

        BinaryString readSmallString =
                BinarySegmentUtils.readBinaryString(segments, 0, fieldOffset, inlineData);
        assertThat(readSmallString).isEqualTo(BinaryString.fromString(smallStr));

        // Test readBinaryString - large string stored externally
        String largeStr = "hello world test";
        byte[] largeStrBytes = largeStr.getBytes();
        MemorySegment[] largeSegments = new MemorySegment[1];
        largeSegments[0] = MemorySegment.wrap(new byte[30]);

        // Store large string at offset 4
        largeSegments[0].put(4, largeStrBytes, 0, largeStrBytes.length);

        // For external storage: highest bit is 0, offset in high 32 bits, size in low 32 bits
        long externalData = ((long) 4 << 32) | largeStrBytes.length;

        BinaryString readLargeString =
                BinarySegmentUtils.readBinaryString(largeSegments, 0, 0, externalData);
        assertThat(readLargeString).isEqualTo(BinaryString.fromString(largeStr));
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

    @Test
    public void testGetIntMultiSegmentsCrossBoundary() {
        MemorySegment[] segments = new MemorySegment[3];
        segments[0] = MemorySegment.wrap(new byte[3]);
        segments[1] = MemorySegment.wrap(new byte[3]);
        segments[2] = MemorySegment.wrap(new byte[3]);

        int testValue = 0x12345678;
        BinarySegmentUtils.setInt(segments, 1, testValue);
        assertThat(BinarySegmentUtils.getInt(segments, 1)).isEqualTo(testValue);

        int testValue2 = Integer.MIN_VALUE;
        BinarySegmentUtils.setInt(segments, 2, testValue2);
        assertThat(BinarySegmentUtils.getInt(segments, 2)).isEqualTo(testValue2);
    }

    @Test
    public void testGetShortMultiSegmentsCrossBoundary() {
        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = MemorySegment.wrap(new byte[3]);
        segments[1] = MemorySegment.wrap(new byte[3]);

        short testValue = (short) 0x1234;
        BinarySegmentUtils.setShort(segments, 0, testValue);
        assertThat(BinarySegmentUtils.getShort(segments, 0)).isEqualTo(testValue);

        short testValue2 = Short.MAX_VALUE;
        BinarySegmentUtils.setShort(segments, 2, testValue2);
        assertThat(BinarySegmentUtils.getShort(segments, 2)).isEqualTo(testValue2);
    }

    @Test
    public void testEqualsWithDifferentSegmentSizes() {
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegment.wrap(new byte[] {1, 2, 3});
        segments1[1] = MemorySegment.wrap(new byte[] {4, 5, 6});

        MemorySegment[] segments2 = new MemorySegment[3];
        segments2[0] = MemorySegment.wrap(new byte[] {1, 2});
        segments2[1] = MemorySegment.wrap(new byte[] {3, 4});
        segments2[2] = MemorySegment.wrap(new byte[] {5, 6});

        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 0, 6)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 1, segments2, 1, 5)).isTrue();
    }

    @Test
    public void testHashConsistencyMultiSegments() {
        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = MemorySegment.wrap(new byte[] {1, 2, 3, 4, 5});
        segments[1] = MemorySegment.wrap(new byte[] {6, 7, 8, 9, 10});

        int hash1 = BinarySegmentUtils.hash(segments, 0, 10);
        int hash2 = BinarySegmentUtils.hash(segments, 0, 10);
        assertThat(hash1).isEqualTo(hash2);

        MemorySegment[] segments2 = new MemorySegment[1];
        segments2[0] = MemorySegment.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        int hash3 = BinarySegmentUtils.hash(segments2, 0, 10);
        assertThat(hash1).isEqualTo(hash3);
    }

    @Test
    public void testLongCrossingThreeSegments() {
        MemorySegment[] segments = new MemorySegment[4];
        segments[0] = MemorySegment.wrap(new byte[3]);
        segments[1] = MemorySegment.wrap(new byte[3]);
        segments[2] = MemorySegment.wrap(new byte[3]);
        segments[3] = MemorySegment.wrap(new byte[3]);

        long testValue = 0x123456789ABCDEF0L;
        BinarySegmentUtils.setLong(segments, 1, testValue);
        long retrieved = BinarySegmentUtils.getLong(segments, 1);
        assertThat(retrieved).isEqualTo(testValue);
    }

    @Test
    public void testCopyToViewMultipleSegments() throws IOException {
        MemorySegment[] segments = new MemorySegment[3];
        segments[0] = MemorySegment.wrap(new byte[] {1, 2, 3, 4, 5});
        segments[1] = MemorySegment.wrap(new byte[] {6, 7, 8, 9, 10});
        segments[2] = MemorySegment.wrap(new byte[] {11, 12, 13, 14, 15});

        MemorySegmentOutputView outputView = new MemorySegmentOutputView(32);
        BinarySegmentUtils.copyToView(segments, 2, 11, outputView);

        byte[] result = outputView.getCopyOfBuffer();
        assertThat(result).isEqualTo(new byte[] {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
    }

    @Test
    public void testCopyToViewInsufficientData() {
        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = MemorySegment.wrap(new byte[3]);
        segments[1] = MemorySegment.wrap(new byte[2]);

        MemorySegmentOutputView outputView = new MemorySegmentOutputView(32);

        assertThatThrownBy(() -> BinarySegmentUtils.copyToView(segments, 0, 10, outputView))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("No copy finished, this should be a bug");
    }

    @Test
    public void testFindAcrossSegments() {
        MemorySegment[] segments1 = new MemorySegment[3];
        segments1[0] = MemorySegment.wrap(new byte[] {1, 2, 3});
        segments1[1] = MemorySegment.wrap(new byte[] {4, 5, 6});
        segments1[2] = MemorySegment.wrap(new byte[] {7, 8, 9});

        MemorySegment[] pattern = new MemorySegment[1];
        pattern[0] = MemorySegment.wrap(new byte[] {5, 6, 7});

        int foundIndex = BinarySegmentUtils.find(segments1, 0, 9, pattern, 0, 3);
        assertThat(foundIndex).isEqualTo(4);

        MemorySegment[] notFoundPattern = new MemorySegment[1];
        notFoundPattern[0] = MemorySegment.wrap(new byte[] {10, 11, 12});
        int notFoundIndex = BinarySegmentUtils.find(segments1, 0, 9, notFoundPattern, 0, 3);
        assertThat(notFoundIndex).isEqualTo(-1);
    }

    @Test
    public void testReadBinaryStringWithMultipleSegments() {
        String testString = "This is a test string that spans multiple segments";
        byte[] testBytes = testString.getBytes(StandardCharsets.UTF_8);

        MemorySegment[] segments = new MemorySegment[3];
        segments[0] = MemorySegment.wrap(new byte[20]);
        segments[1] = MemorySegment.wrap(new byte[20]);
        segments[2] = MemorySegment.wrap(new byte[20]);

        BinarySegmentUtils.copyFromBytes(segments, 5, testBytes, 0, testBytes.length);

        long variablePartOffsetAndLen = ((long) 5 << 32) | testBytes.length;
        BinaryString result =
                BinarySegmentUtils.readBinaryString(segments, 0, 0, variablePartOffsetAndLen);
        assertThat(result.toString()).isEqualTo(testString);
    }

    @Test
    public void testFloatDoubleCrossSegments() {
        MemorySegment[] segments = new MemorySegment[3];
        segments[0] = MemorySegment.wrap(new byte[3]);
        segments[1] = MemorySegment.wrap(new byte[3]);
        segments[2] = MemorySegment.wrap(new byte[3]);

        float testFloat = 3.14159f;
        BinarySegmentUtils.setFloat(segments, 1, testFloat);
        assertThat(BinarySegmentUtils.getFloat(segments, 1)).isEqualTo(testFloat);

        MemorySegment[] longSegments = new MemorySegment[3];
        longSegments[0] = MemorySegment.wrap(new byte[5]);
        longSegments[1] = MemorySegment.wrap(new byte[5]);
        longSegments[2] = MemorySegment.wrap(new byte[5]);

        double testDouble = 2.718281828;
        BinarySegmentUtils.setDouble(longSegments, 2, testDouble);
        assertThat(BinarySegmentUtils.getDouble(longSegments, 2)).isEqualTo(testDouble);
    }

    @Test
    public void testBitOperationsMultiSegments() {
        MemorySegment[] segments = new MemorySegment[3];
        segments[0] = MemorySegment.wrap(new byte[4]);
        segments[1] = MemorySegment.wrap(new byte[4]);
        segments[2] = MemorySegment.wrap(new byte[4]);

        BinarySegmentUtils.bitSet(segments, 0, 0);
        BinarySegmentUtils.bitSet(segments, 0, 31);
        BinarySegmentUtils.bitSet(segments, 0, 32);
        BinarySegmentUtils.bitSet(segments, 0, 63);

        assertThat(BinarySegmentUtils.bitGet(segments, 0, 0)).isTrue();
        assertThat(BinarySegmentUtils.bitGet(segments, 0, 31)).isTrue();
        assertThat(BinarySegmentUtils.bitGet(segments, 0, 32)).isTrue();
        assertThat(BinarySegmentUtils.bitGet(segments, 0, 63)).isTrue();

        BinarySegmentUtils.bitUnSet(segments, 0, 31);
        BinarySegmentUtils.bitUnSet(segments, 0, 63);

        assertThat(BinarySegmentUtils.bitGet(segments, 0, 31)).isFalse();
        assertThat(BinarySegmentUtils.bitGet(segments, 0, 63)).isFalse();
    }

    @Test
    public void testByteBooleanArrayOperations() {
        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = MemorySegment.wrap(new byte[5]);
        segments[1] = MemorySegment.wrap(new byte[5]);

        byte[] testBytes = {1, 0, 1, 0, 1, 0, 1, 0};
        for (int i = 0; i < testBytes.length; i++) {
            BinarySegmentUtils.setByte(segments, i, testBytes[i]);
        }

        for (int i = 0; i < testBytes.length; i++) {
            assertThat(BinarySegmentUtils.getByte(segments, i)).isEqualTo(testBytes[i]);
            assertThat(BinarySegmentUtils.getBoolean(segments, i)).isEqualTo(testBytes[i] != 0);
        }

        boolean[] testBooleans = {true, false, true, false};
        for (int i = 0; i < testBooleans.length; i++) {
            BinarySegmentUtils.setBoolean(segments, i, testBooleans[i]);
        }

        for (int i = 0; i < testBooleans.length; i++) {
            assertThat(BinarySegmentUtils.getBoolean(segments, i)).isEqualTo(testBooleans[i]);
        }
    }

    @Test
    public void testCopyToUnsafeMultiSegments() {
        MemorySegment[] segments = new MemorySegment[3];
        segments[0] = MemorySegment.wrap(new byte[8]);
        segments[1] = MemorySegment.wrap(new byte[8]);
        segments[2] = MemorySegment.wrap(new byte[8]);

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 8; j++) {
                segments[i].put(j, (byte) (i * 8 + j));
            }
        }

        boolean[] boolResult = new boolean[24];
        BinarySegmentUtils.copyToUnsafe(segments, 0, boolResult, 0, 24);
        for (int i = 0; i < 24; i++) {
            assertThat(boolResult[i]).isEqualTo(i != 0);
        }

        byte[] byteResult = new byte[24];
        BinarySegmentUtils.copyToUnsafe(segments, 0, byteResult, 0, 24);
        for (int i = 0; i < 24; i++) {
            assertThat(byteResult[i]).isEqualTo((byte) i);
        }
    }
}
