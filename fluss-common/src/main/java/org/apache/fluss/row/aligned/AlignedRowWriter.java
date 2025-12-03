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

package org.apache.fluss.row.aligned;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.AbstractBinaryWriter;
import org.apache.fluss.row.BinarySegmentUtils;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Writer for {@link AlignedRow}.
 *
 * <p>Use the special format to write data to a {@link MemorySegment} (its capacity grows
 * automatically).
 *
 * <p>If write a format binary: 1. New a writer. 2. Write each field by writeXX or setNullAt.
 * (Variable length fields can not be written repeatedly.) 3. Invoke {@link #complete()}.
 *
 * <p>If want to reuse this writer, please invoke {@link #reset()} first.
 */
public final class AlignedRowWriter extends AbstractBinaryWriter {

    private final int nullBitsSizeInBytes;
    private final AlignedRow row;
    private final int fixedSize;

    public AlignedRowWriter(AlignedRow row) {
        this(row, 0);
    }

    public AlignedRowWriter(AlignedRow row, int initialSize) {
        this.nullBitsSizeInBytes = AlignedRow.calculateBitSetWidthInBytes(row.getFieldCount());
        this.fixedSize = row.getFixedLengthPartSize();
        this.cursor = fixedSize;

        this.segment = MemorySegment.wrap(new byte[fixedSize + initialSize]);
        this.row = row;
        this.row.pointTo(segment, 0, segment.size());
    }

    /** Reset writer to prepare next write. First, reset. */
    public void reset() {
        this.cursor = fixedSize;
        for (int i = 0; i < nullBitsSizeInBytes; i += 8) {
            segment.putLong(i, 0L);
        }
    }

    /** Set null to this field. Default not null. */
    public void setNullAt(int pos) {
        setNullBit(pos);
        segment.putLong(getFieldOffset(pos), 0L);
    }

    public void setNullBit(int pos) {
        BinarySegmentUtils.bitSet(segment, 0, pos + AlignedRow.HEADER_SIZE_IN_BITS);
    }

    public void writeBoolean(int pos, boolean value) {
        segment.putBoolean(getFieldOffset(pos), value);
    }

    public void writeByte(int pos, byte value) {
        segment.put(getFieldOffset(pos), value);
    }

    public void writeShort(int pos, short value) {
        segment.putShort(getFieldOffset(pos), value);
    }

    public void writeInt(int pos, int value) {
        segment.putInt(getFieldOffset(pos), value);
    }

    public void writeLong(int pos, long value) {
        segment.putLong(getFieldOffset(pos), value);
    }

    public void writeFloat(int pos, float value) {
        segment.putFloat(getFieldOffset(pos), value);
    }

    public void writeDouble(int pos, double value) {
        segment.putDouble(getFieldOffset(pos), value);
    }

    /** Finally, complete write to set real size to binary. */
    public void complete() {
        row.setTotalSize(cursor);
    }

    public int getFieldOffset(int pos) {
        return nullBitsSizeInBytes + 8 * pos;
    }

    /** Set offset and size to fix len part. */
    public void setOffsetAndSize(int pos, int offset, long size) {
        final long offsetAndSize = ((long) offset << 32) | size;
        segment.putLong(getFieldOffset(pos), offsetAndSize);
    }

    /** After grow, need point to new memory. */
    public void afterGrow() {
        row.pointTo(segment, 0, segment.size());
    }
}
