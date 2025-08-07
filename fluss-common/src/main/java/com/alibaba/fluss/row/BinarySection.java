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

package com.alibaba.fluss.row;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.utils.IOUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/** Describe a section of memory. */
@Internal
abstract class BinarySection implements MemoryAwareGetters, Serializable {

    private static final long serialVersionUID = 1L;

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
