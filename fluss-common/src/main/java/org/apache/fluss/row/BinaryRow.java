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

import java.io.Serializable;

/**
 * A binary format {@link InternalRow} that is backed on {@link MemorySegment} and supports all
 * interfaces provided by {@link MemoryAwareGetters}.
 */
public interface BinaryRow extends InternalRow, MemoryAwareGetters, Serializable {

    /**
     * Copies the bytes of the row to the destination memory, beginning at the given offset.
     *
     * @param dst The memory into which the bytes will be copied.
     * @param dstOffset The copying offset in the destination memory.
     */
    void copyTo(byte[] dst, int dstOffset);

    /**
     * Copy the bytes of the row to the destination memory, beginning at the given offset.
     *
     * @return The copied row.
     */
    BinaryRow copy();

    /**
     * Point to the bytes of the row.
     *
     * @param segment The memory segment.
     * @param offset The offset in the memory segment.
     * @param sizeInBytes The size of the row.
     */
    void pointTo(MemorySegment segment, int offset, int sizeInBytes);

    /**
     * Point to the bytes of the row.
     *
     * @param segments The memory segments.
     * @param offset The offset in the memory segments.
     * @param sizeInBytes The size of the row.
     */
    void pointTo(MemorySegment[] segments, int offset, int sizeInBytes);

    /**
     * Calculate the width of the bit set.
     *
     * @param arity the number of fields.
     * @return the width of the bit set.
     */
    static int calculateBitSetWidthInBytes(int arity) {
        // need arity bits to store null bits, round up to the nearest byte size
        return (arity + 7) / 8;
    }
}
