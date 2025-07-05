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

package org.apache.fluss.memory;

import java.io.EOFException;
import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** A {@link MemorySegment} input implementation for the {@link InputView} interface. */
public class MemorySegmentInputView implements InputView {

    private final MemorySegment segment;

    private final int end;

    private int position;

    public MemorySegmentInputView(MemorySegment segment) {
        this(segment, 0);
    }

    public MemorySegmentInputView(MemorySegment segment, int position) {
        checkArgument(position >= 0 && position < segment.size(), "Position is out of bounds.");
        this.segment = segment;
        this.end = segment.size();
        this.position = position;
    }

    @Override
    public boolean readBoolean() throws IOException {
        if (position < end) {
            return segment.get(position++) != 0;
        } else {
            throw new EOFException();
        }
    }

    @Override
    public byte readByte() throws IOException {
        if (position < end) {
            return segment.get(position++);
        } else {
            throw new EOFException();
        }
    }

    @Override
    public short readShort() throws IOException {
        if (position >= 0 && position < end - 1) {
            short v = segment.getShort(position);
            position += 2;
            return v;
        } else {
            throw new EOFException();
        }
    }

    @Override
    public int readInt() throws IOException {
        if (position >= 0 && position < end - 3) {
            int v = segment.getInt(position);
            position += 4;
            return v;
        } else {
            throw new EOFException();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (position >= 0 && position < end - 7) {
            long v = segment.getLong(position);
            position += 8;
            return v;
        } else {
            throw new EOFException();
        }
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int offset, int len) throws IOException {
        if (len >= 0) {
            if (offset <= b.length - len) {
                if (this.position <= this.end - len) {
                    segment.get(this.position, b, offset, len);
                    position += len;
                } else {
                    throw new EOFException();
                }
            } else {
                throw new ArrayIndexOutOfBoundsException();
            }
        } else {
            throw new IllegalArgumentException("Length may not be negative.");
        }
    }
}
