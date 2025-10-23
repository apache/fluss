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
import org.apache.fluss.utils.IOUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Describe a section of memory. */
@Internal
public abstract class BinarySection implements MemoryAwareGetters, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * It decides whether to put data in FixLenPart or VarLenPart. See more in {@link BinaryRow}.
     *
     * <p>If len is less than 8, its binary format is: 1-bit mark(1) = 1, 7-bits len, and 7-bytes
     * data. Data is stored in fix-length part.
     *
     * <p>If len is greater or equal to 8, its binary format is: 1-bit mark(1) = 0, 31-bits offset
     * to the data, and 4-bytes length of data. Data is stored in variable-length part.
     */
    public static final int MAX_FIX_PART_DATA_SIZE = 7;

    /**
     * To get the mark in highest bit of long. Form: 10000000 00000000 ... (8 bytes)
     *
     * <p>This is used to decide whether the data is stored in fixed-length part or variable-length
     * part. see {@link #MAX_FIX_PART_DATA_SIZE} for more information.
     */
    public static final long HIGHEST_FIRST_BIT = 0x80L << 56;

    /**
     * To get the 7 bits length in second bit to eighth bit out of a long. Form: 01111111 00000000
     * ... (8 bytes)
     *
     * <p>This is used to get the length of the data which is stored in this long. see {@link
     * #MAX_FIX_PART_DATA_SIZE} for more information.
     */
    public static final long HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7FL << 56;

    protected MemorySegment[] segments;
    protected int offset;
    protected int sizeInBytes;

    public BinarySection() {}

    public BinarySection(MemorySegment[] segments, int offset, int sizeInBytes) {
        checkArgument(segments != null);
        this.segments = segments;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    public final void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
        pointTo(new MemorySegment[] {segment}, offset, sizeInBytes);
    }

    public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
        checkArgument(segments != null);
        this.segments = segments;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    @Override
    public MemorySegment[] getSegments() {
        return segments;
    }

    @Override
    public int getOffset() {
        return offset;
    }

    @Override
    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public byte[] toBytes() {
        return BinarySegmentUtils.getBytes(segments, offset, sizeInBytes);
    }

    public ByteBuffer wrapByteBuffer() {
        if (segments.length == 1) {
            MemorySegment segment = segments[0];
            return segment.wrap(offset, sizeInBytes);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Support Java Serialization by customize writeObject and readObject methods, because {@link
     * MemorySegment} doesn't support Java Serialization.
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        byte[] bytes = toBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /**
     * Support Java Serialization by customize writeObject and readObject methods, because {@link
     * MemorySegment} doesn't support Java Serialization.
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        byte[] bytes = new byte[in.readInt()];
        IOUtils.readFully(in, bytes);
        pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BinarySection that = (BinarySection) o;
        return sizeInBytes == that.sizeInBytes
                && BinarySegmentUtils.equals(
                        segments, offset, that.segments, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return BinarySegmentUtils.hash(segments, offset, sizeInBytes);
    }
}
