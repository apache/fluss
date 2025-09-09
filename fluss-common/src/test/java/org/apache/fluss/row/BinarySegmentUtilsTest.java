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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BinarySegmentUtils}. */
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
        // Test readBinary - small data stored inline (< 8 bytes)
        byte[] smallBinary = {1, 2, 3, 4};

        // For inline storage, we use the highest bit to mark inline data
        // and the next 7 bits for length
        long inlineData = 0x8000000000000000L; // highest bit set for inline
        inlineData |= ((long) smallBinary.length << 56); // length in bits 56-62

        // Store the actual data in the lower bytes
        if (BinarySegmentUtils.LITTLE_ENDIAN) {
            for (int i = 0; i < smallBinary.length; i++) {
                inlineData |= ((long) (smallBinary[i] & 0xFF)) << (i * 8);
            }
        } else {
            for (int i = 0; i < smallBinary.length; i++) {
                inlineData |= ((long) (smallBinary[i] & 0xFF)) << ((6 - i) * 8);
            }
        }

        MemorySegment[] segments = new MemorySegment[1];
        segments[0] = MemorySegment.wrap(new byte[16]);

        byte[] readSmallBinary = BinarySegmentUtils.readBinary(segments, 0, 0, inlineData);
        assertThat(readSmallBinary).isEqualTo(smallBinary);

        // Test readBinary - large data stored externally (>= 8 bytes)
        byte[] largeBinary = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        MemorySegment[] largeSegments = new MemorySegment[1];
        largeSegments[0] = MemorySegment.wrap(new byte[20]);

        // Store large binary at offset 4
        largeSegments[0].put(4, largeBinary, 0, largeBinary.length);

        // For external storage, highest bit is 0, offset in high 32 bits, size in low 32 bits
        long externalData = ((long) 4 << 32) | largeBinary.length;

        byte[] readLargeBinary = BinarySegmentUtils.readBinary(largeSegments, 0, 0, externalData);
        assertThat(readLargeBinary).isEqualTo(largeBinary);
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
}
