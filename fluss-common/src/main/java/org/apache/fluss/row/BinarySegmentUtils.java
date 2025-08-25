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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.utils.MurmurHashUtils;

import java.io.IOException;

import static org.apache.fluss.memory.MemoryUtils.UNSAFE;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Utilities for binary data segments which heavily uses {@link MemorySegment}. */
@Internal
public final class BinarySegmentUtils {

    private static final int ADDRESS_BITS_PER_WORD = 3;

    private static final int BIT_BYTE_INDEX_MASK = 7;

    /**
     * SQL execution threads is limited, not too many, so it can bear the overhead of 64K per
     * thread.
     */
    private static final int MAX_BYTES_LENGTH = 1024 * 64;

    private static final int MAX_CHARS_LENGTH = 1024 * 32;

    private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private static final ThreadLocal<byte[]> BYTES_LOCAL = new ThreadLocal<>();
    private static final ThreadLocal<char[]> CHARS_LOCAL = new ThreadLocal<>();

    private BinarySegmentUtils() {
        // do not instantiate
    }

    /**
     * Equals two memory segments regions.
     *
     * @param segments1 Segments 1
     * @param offset1 Offset of segments1 to start equaling
     * @param segments2 Segments 2
     * @param offset2 Offset of segments2 to start equaling
     * @param len Length of the equaled memory region
     * @return true if equal, false otherwise
     */
    public static boolean equals(
            MemorySegment[] segments1,
            int offset1,
            MemorySegment[] segments2,
            int offset2,
            int len) {
        if (inFirstSegment(segments1, offset1, len) && inFirstSegment(segments2, offset2, len)) {
            return segments1[0].equalTo(segments2[0], offset1, offset2, len);
        } else {
            return equalsMultiSegments(segments1, offset1, segments2, offset2, len);
        }
    }

    /**
     * Copy segments to a new byte[].
     *
     * @param segments Source segments.
     * @param offset Source segments offset.
     * @param numBytes the number bytes to copy.
     */
    public static byte[] copyToBytes(MemorySegment[] segments, int offset, int numBytes) {
        return copyToBytes(segments, offset, new byte[numBytes], 0, numBytes);
    }

    /**
     * Copy segments to target byte[].
     *
     * @param segments Source segments.
     * @param offset Source segments offset.
     * @param bytes target byte[].
     * @param bytesOffset target byte[] offset.
     * @param numBytes the number bytes to copy.
     */
    public static byte[] copyToBytes(
            MemorySegment[] segments, int offset, byte[] bytes, int bytesOffset, int numBytes) {
        if (inFirstSegment(segments, offset, numBytes)) {
            segments[0].get(offset, bytes, bytesOffset, numBytes);
        } else {
            copyMultiSegmentsToBytes(segments, offset, bytes, bytesOffset, numBytes);
        }
        return bytes;
    }

    /**
     * Copy bytes of segments to output view.
     *
     * <p>Note: It just copies the data in, not include the length.
     *
     * @param segments source segments
     * @param offset offset for segments
     * @param sizeInBytes size in bytes
     * @param target target output view
     */
    public static void copyToView(
            MemorySegment[] segments, int offset, int sizeInBytes, OutputView target)
            throws IOException {
        for (MemorySegment sourceSegment : segments) {
            int curSegRemain = sourceSegment.size() - offset;
            if (curSegRemain > 0) {
                int copySize = Math.min(curSegRemain, sizeInBytes);

                byte[] bytes = allocateReuseBytes(copySize);
                sourceSegment.get(offset, bytes, 0, copySize);
                target.write(bytes, 0, copySize);

                sizeInBytes -= copySize;
                offset = 0;
            } else {
                offset -= sourceSegment.size();
            }

            if (sizeInBytes == 0) {
                return;
            }
        }

        if (sizeInBytes != 0) {
            throw new RuntimeException(
                    "No copy finished, this should be a bug, "
                            + "The remaining length is: "
                            + sizeInBytes);
        }
    }

    /**
     * Find equal segments2 in segments1.
     *
     * @param segments1 segs to find.
     * @param segments2 sub segs.
     * @return Return the found offset, return -1 if not find.
     */
    public static int find(
            MemorySegment[] segments1,
            int offset1,
            int numBytes1,
            MemorySegment[] segments2,
            int offset2,
            int numBytes2) {
        if (numBytes2 == 0) { // quick way 1.
            return offset1;
        }
        if (inFirstSegment(segments1, offset1, numBytes1)
                && inFirstSegment(segments2, offset2, numBytes2)) {
            byte first = segments2[0].get(offset2);
            int end = numBytes1 - numBytes2 + offset1;
            for (int i = offset1; i <= end; i++) {
                // quick way 2: equal first byte.
                if (segments1[0].get(i) == first
                        && segments1[0].equalTo(segments2[0], i, offset2, numBytes2)) {
                    return i;
                }
            }
            return -1;
        } else {
            return findInMultiSegments(
                    segments1, offset1, numBytes1, segments2, offset2, numBytes2);
        }
    }

    private static int findInMultiSegments(
            MemorySegment[] segments1,
            int offset1,
            int numBytes1,
            MemorySegment[] segments2,
            int offset2,
            int numBytes2) {
        int end = numBytes1 - numBytes2 + offset1;
        for (int i = offset1; i <= end; i++) {
            if (equalsMultiSegments(segments1, i, segments2, offset2, numBytes2)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * hash segments to int.
     *
     * @param segments Source segments.
     * @param offset Source segments offset.
     * @param numBytes the number bytes to hash.
     */
    public static int hash(MemorySegment[] segments, int offset, int numBytes) {
        if (inFirstSegment(segments, offset, numBytes)) {
            return MurmurHashUtils.hashBytes(segments[0], offset, numBytes);
        } else {
            return hashMultiSeg(segments, offset, numBytes);
        }
    }

    /**
     * Allocate bytes that is only for temporary usage, it should not be stored in somewhere else.
     * Use a {@link ThreadLocal} to reuse bytes to avoid overhead of byte[] new and gc.
     *
     * <p>If there are methods that can only accept a byte[], instead of a MemorySegment[]
     * parameter, we can allocate a reuse bytes and copy the MemorySegment data to byte[], then call
     * the method. Such as String deserialization.
     */
    public static byte[] allocateReuseBytes(int length) {
        byte[] bytes = BYTES_LOCAL.get();

        if (bytes == null) {
            if (length <= MAX_BYTES_LENGTH) {
                bytes = new byte[MAX_BYTES_LENGTH];
                BYTES_LOCAL.set(bytes);
            } else {
                bytes = new byte[length];
            }
        } else if (bytes.length < length) {
            bytes = new byte[length];
        }

        return bytes;
    }

    public static char[] allocateReuseChars(int length) {
        char[] chars = CHARS_LOCAL.get();

        if (chars == null) {
            if (length <= MAX_CHARS_LENGTH) {
                chars = new char[MAX_CHARS_LENGTH];
                CHARS_LOCAL.set(chars);
            } else {
                chars = new char[length];
            }
        } else if (chars.length < length) {
            chars = new char[length];
        }

        return chars;
    }

    public static void copyMultiSegmentsToBytes(
            MemorySegment[] segments, int offset, byte[] bytes, int bytesOffset, int numBytes) {
        int remainSize = numBytes;
        for (MemorySegment segment : segments) {
            int remain = segment.size() - offset;
            if (remain > 0) {
                int nCopy = Math.min(remain, remainSize);
                segment.get(offset, bytes, numBytes - remainSize + bytesOffset, nCopy);
                remainSize -= nCopy;
                // next new segment.
                offset = 0;
                if (remainSize == 0) {
                    return;
                }
            } else {
                // remain is negative, let's advance to next segment
                // now the offset = offset - segmentSize (-remain)
                offset = -remain;
            }
        }
    }

    /** Maybe not copied, if want copy, please use copyTo. */
    public static byte[] getBytes(MemorySegment[] segments, int baseOffset, int sizeInBytes) {
        // avoid copy if `base` is `byte[]`
        if (segments.length == 1) {
            byte[] heapMemory = segments[0].getHeapMemory();
            if (baseOffset == 0 && heapMemory != null && heapMemory.length == sizeInBytes) {
                return heapMemory;
            } else {
                byte[] bytes = new byte[sizeInBytes];
                segments[0].get(baseOffset, bytes, 0, sizeInBytes);
                return bytes;
            }
        } else {
            byte[] bytes = new byte[sizeInBytes];
            copyMultiSegmentsToBytes(segments, baseOffset, bytes, 0, sizeInBytes);
            return bytes;
        }
    }

    /** Is it just in first MemorySegment, we use quick way to do something. */
    private static boolean inFirstSegment(MemorySegment[] segments, int offset, int numBytes) {
        return numBytes + offset <= segments[0].size();
    }

    static boolean equalsMultiSegments(
            MemorySegment[] segments1,
            int offset1,
            MemorySegment[] segments2,
            int offset2,
            int len) {
        if (len == 0) {
            // quick way and avoid segSize is zero.
            return true;
        }

        int segSize1 = segments1[0].size();
        int segSize2 = segments2[0].size();

        // find first segIndex and segOffset of segments.
        int segIndex1 = offset1 / segSize1;
        int segIndex2 = offset2 / segSize2;
        int segOffset1 = offset1 - segSize1 * segIndex1; // equal to %
        int segOffset2 = offset2 - segSize2 * segIndex2; // equal to %

        while (len > 0) {
            int equalLen = Math.min(Math.min(len, segSize1 - segOffset1), segSize2 - segOffset2);
            if (!segments1[segIndex1].equalTo(
                    segments2[segIndex2], segOffset1, segOffset2, equalLen)) {
                return false;
            }
            len -= equalLen;
            segOffset1 += equalLen;
            if (segOffset1 == segSize1) {
                segOffset1 = 0;
                segIndex1++;
            }
            segOffset2 += equalLen;
            if (segOffset2 == segSize2) {
                segOffset2 = 0;
                segIndex2++;
            }
        }
        return true;
    }

    /**
     * set bit.
     *
     * @param segment target segment.
     * @param baseOffset bits base offset.
     * @param index bit index from base offset.
     */
    public static void bitSet(MemorySegment segment, int baseOffset, int index) {
        int offset = baseOffset + byteIndex(index);
        byte current = segment.get(offset);
        current |= (1 << (index & BIT_BYTE_INDEX_MASK));
        segment.put(offset, current);
    }

    /**
     * Given a bit index, return the byte index containing it.
     *
     * @param bitIndex the bit index.
     * @return the byte index.
     */
    private static int byteIndex(int bitIndex) {
        return bitIndex >>> ADDRESS_BITS_PER_WORD;
    }

    /**
     * read bit.
     *
     * @param segment target segment.
     * @param baseOffset bits base offset.
     * @param index bit index from base offset.
     */
    public static boolean bitGet(MemorySegment segment, int baseOffset, int index) {
        int offset = baseOffset + byteIndex(index);
        byte current = segment.get(offset);
        return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
    }

    private static int hashMultiSeg(MemorySegment[] segments, int offset, int numBytes) {
        byte[] bytes = allocateReuseBytes(numBytes);
        copyMultiSegmentsToBytes(segments, offset, bytes, 0, numBytes);
        return MurmurHashUtils.hashUnsafeBytes(bytes, BYTE_ARRAY_BASE_OFFSET, numBytes);
    }
}
