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

package com.alibaba.fluss.memory;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies interoperability between heap and off-heap modes of {@link
 * com.alibaba.fluss.memory.MemorySegment}.
 */
public class CrossMemorySegmentTypeTest {
    private static final long BYTE_ARRAY_BASE_OFFSET =
            MemoryUtils.UNSAFE.arrayBaseOffset(byte[].class);

    private final int pageSize = 32 * 1024;

    // ------------------------------------------------------------------------

    @Test
    public void testCompareBytesMixedSegments() {
        MemorySegment[] segs1 = createSegments(pageSize);
        MemorySegment[] segs2 = createSegments(pageSize);

        Random rnd = new Random();

        for (MemorySegment seg1 : segs1) {
            for (MemorySegment seg2 : segs2) {
                testCompare(seg1, seg2, rnd);
            }
        }
    }

    private void testCompare(MemorySegment seg1, MemorySegment seg2, Random random) {
        assertThat(seg1.size()).isEqualTo(pageSize);
        assertThat(seg2.size()).isEqualTo(pageSize);

        final byte[] bytes1 = new byte[pageSize];
        final byte[] bytes2 = new byte[pageSize];

        final int stride = pageSize / 255;
        final int shift = 16666;

        for (int i = 0; i < pageSize; i++) {
            byte val = (byte) ((i / stride) & 0xff);
            bytes1[i] = val;

            if (i + shift < bytes2.length) {
                bytes2[i + shift] = val;
            }
        }

        seg1.put(0, bytes1);
        seg2.put(0, bytes2);

        for (int i = 0; i < 1000; i++) {
            int pos1 = random.nextInt(bytes1.length);
            int pos2 = random.nextInt(bytes2.length);

            int len =
                    Math.min(
                            Math.min(bytes1.length - pos1, bytes2.length - pos2),
                            random.nextInt(pageSize / 50));

            int cmp = seg1.compare(seg2, pos1, pos2, len);

            if (pos1 < pos2 - shift) {
                assertThat(cmp <= 0).isTrue();
            } else {
                assertThat(cmp >= 0).isTrue();
            }
        }
    }

    @Test
    public void testSwapBytesMixedSegments() {
        final int halfPageSize = pageSize / 2;
        MemorySegment[] segs1 = createSegments(pageSize);
        MemorySegment[] segs2 = createSegments(halfPageSize);

        Random rnd = new Random();

        for (MemorySegment seg1 : segs1) {
            for (MemorySegment seg2 : segs2) {
                testSwap(seg1, seg2, rnd, halfPageSize);
            }
        }
    }

    private void testSwap(MemorySegment seg1, MemorySegment seg2, Random random, int smallerSize) {
        assertThat(seg1.size()).isEqualTo(pageSize);
        assertThat(seg2.size()).isEqualTo(smallerSize);

        final byte[] bytes1 = new byte[pageSize];
        final byte[] bytes2 = new byte[smallerSize];

        Arrays.fill(bytes2, (byte) 1);

        seg1.put(0, bytes1);
        seg2.put(0, bytes2);

        // wap the second half of the first segment with the second segment

        int pos = 0;
        while (pos < smallerSize) {
            int len = random.nextInt(pageSize / 40);
            len = Math.min(len, smallerSize - pos);
            seg1.swapBytes(new byte[len], seg2, pos + smallerSize, pos, len);
            pos += len;
        }

        // the second segment should now be all zeros, the first segment should have one in its
        // second half

        for (int i = 0; i < smallerSize; i++) {
            assertThat(seg1.get(i)).isEqualTo((byte) 0);
            assertThat(seg2.get(i)).isEqualTo((byte) 0);
            assertThat(seg1.get(i + smallerSize)).isEqualTo((byte) 1);
        }
    }

    @Test
    public void testCopyMixedSegments() {
        MemorySegment[] segs1 = createSegments(pageSize);
        MemorySegment[] segs2 = createSegments(pageSize);

        Random rnd = new Random();

        for (MemorySegment seg1 : segs1) {
            for (MemorySegment seg2 : segs2) {
                testCopy(seg1, seg2, rnd);
            }
        }
    }

    private static MemorySegment[] createSegments(int size) {
        return new MemorySegment[] {
            MemorySegment.allocateHeapMemory(size), MemorySegment.allocateOffHeapMemory(size)
        };
    }

    private void testCopy(MemorySegment seg1, MemorySegment seg2, Random random) {
        assertThat(seg1.size()).isEqualTo(pageSize);
        assertThat(seg2.size()).isEqualTo(pageSize);

        byte[] expected = new byte[pageSize];
        byte[] actual = new byte[pageSize];
        byte[] unsafeCopy = new byte[pageSize];
        MemorySegment unsafeCopySeg = MemorySegment.allocateHeapMemory(pageSize);

        // zero out the memory
        seg1.put(0, expected);
        seg2.put(0, expected);

        for (int i = 0; i < 40; i++) {
            int numBytes = random.nextInt(pageSize / 20);
            byte[] bytes = new byte[numBytes];
            random.nextBytes(bytes);

            int thisPos = random.nextInt(pageSize - numBytes);
            int otherPos = random.nextInt(pageSize - numBytes);

            // track what we expect
            System.arraycopy(bytes, 0, expected, otherPos, numBytes);

            seg1.put(thisPos, bytes);
            seg1.copyTo(thisPos, seg2, otherPos, numBytes);
            seg1.copyToUnsafe(
                    thisPos, unsafeCopy, (int) (otherPos + BYTE_ARRAY_BASE_OFFSET), numBytes);

            int otherPos2 = random.nextInt(pageSize - numBytes);
            unsafeCopySeg.copyFromUnsafe(
                    otherPos2, unsafeCopy, (int) (otherPos + BYTE_ARRAY_BASE_OFFSET), numBytes);
            assertThat(unsafeCopySeg.equalTo(seg2, otherPos2, otherPos, numBytes)).isTrue();
        }

        seg2.get(0, actual);
        assertThat(actual).isEqualTo(expected);

        // test out of bound conditions

        final int[] validOffsets = {0, 1, pageSize / 10 * 9};
        final int[] invalidOffsets = {
            -1, pageSize + 1, -pageSize, Integer.MAX_VALUE, Integer.MIN_VALUE
        };

        final int[] validLengths = {0, 1, pageSize / 10, pageSize};
        final int[] invalidLengths = {
            -1, -pageSize, pageSize + 1, Integer.MAX_VALUE, Integer.MIN_VALUE
        };

        for (int off1 : validOffsets) {
            for (int off2 : validOffsets) {
                for (int len : invalidLengths) {
                    assertThatThrownBy(() -> seg1.copyTo(off1, seg2, off2, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg1.copyTo(off2, seg2, off1, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg2.copyTo(off1, seg1, off2, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg2.copyTo(off2, seg1, off1, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                }
            }
        }

        for (int off1 : validOffsets) {
            for (int off2 : invalidOffsets) {
                for (int len : validLengths) {
                    assertThatThrownBy(() -> seg1.copyTo(off1, seg2, off2, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg1.copyTo(off2, seg2, off1, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg2.copyTo(off1, seg1, off2, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg2.copyTo(off2, seg1, off1, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                }
            }
        }

        for (int off1 : invalidOffsets) {
            for (int off2 : validOffsets) {
                for (int len : validLengths) {
                    assertThatThrownBy(() -> seg1.copyTo(off1, seg2, off2, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg1.copyTo(off2, seg2, off1, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg2.copyTo(off1, seg1, off2, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg2.copyTo(off2, seg1, off1, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                }
            }
        }

        for (int off1 : invalidOffsets) {
            for (int off2 : invalidOffsets) {
                for (int len : validLengths) {
                    assertThatThrownBy(() -> seg1.copyTo(off1, seg2, off2, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg1.copyTo(off2, seg2, off1, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg2.copyTo(off1, seg1, off2, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                    assertThatThrownBy(() -> seg2.copyTo(off2, seg1, off1, len))
                            .isInstanceOf(IndexOutOfBoundsException.class);
                }
            }
        }
    }
}
