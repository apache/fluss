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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A {@link MemorySegment} output implementation for the {@link OutputView} interface.
 *
 * <p>NOTE: currently it only works on-heap {@link MemorySegment}, see {@link #resize(int)}.
 */
public class MemorySegmentOutputView implements OutputView, MemorySegmentWritable {
    private MemorySegment memorySegment;
    private int position;

    // ------------------------------------------------------------------------

    public MemorySegmentOutputView(int initialCapacity) {
        this(MemorySegment.wrap(new byte[initialCapacity]));
    }

    public MemorySegmentOutputView(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
        this.position = 0;
    }

    public MemorySegment getMemorySegment() {
        return memorySegment;
    }

    public int getPosition() {
        return position;
    }

    public byte[] getSharedBuffer() {
        return memorySegment.getHeapMemory();
    }

    public byte[] getCopyOfBuffer() {
        return Arrays.copyOf(memorySegment.getHeapMemory(), position);
    }

    public void setPosition(int position) throws IOException {
        if (this.memorySegment.size() <= position) {
            resize(position - this.memorySegment.size());
        }

        this.position = position;
    }

    @Override
    public void write(int b) throws IOException {
        writeByte(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        if (this.position > this.memorySegment.size() - len) {
            resize(len);
        }
        memorySegment.put(position, b, off, len);
        position += len;
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        writeByte(v ? 1 : 0);
    }

    @Override
    public void writeByte(int b) throws IOException {
        if (this.position >= memorySegment.size()) {
            resize(1);
        }

        memorySegment.put(position++, (byte) (b & 0xff));
    }

    @Override
    public void writeShort(int v) throws IOException {
        if (this.position >= this.memorySegment.size() - 1) {
            resize(2);
        }

        memorySegment.putShort(position, (short) v);
        position += 2;
    }

    @Override
    public void writeChar(int v) throws IOException {
        if (this.position >= this.memorySegment.size() - 1) {
            resize(2);
        }

        memorySegment.putChar(position, (char) v);
        position += 2;
    }

    @Override
    public void writeInt(int v) throws IOException {
        if (this.position >= this.memorySegment.size() - 3) {
            resize(4);
        }

        memorySegment.putInt(position, v);
        position += 4;
    }

    public void writeUnsignedInt(long v) throws IOException {
        writeInt((int) (v & 0xffffffffL));
    }

    @Override
    public void writeLong(long v) throws IOException {
        if (this.position >= this.memorySegment.size() - 7) {
            resize(8);
        }

        memorySegment.putLong(position, v);
        position += 8;
    }

    @Override
    public void writeFloat(float v) throws IOException {
        if (this.position >= this.memorySegment.size() - 3) {
            resize(4);
        }

        memorySegment.putFloat(position, v);
        position += 4;
    }

    @Override
    public void writeDouble(double v) throws IOException {
        if (this.position >= this.memorySegment.size() - 7) {
            resize(8);
        }

        memorySegment.putDouble(position, v);
        position += 8;
    }

    @Override
    public void writeBytes(String s) throws IOException {
        if (s == null) {
            throw new NullPointerException();
        }
        for (int i = 0; i < s.length(); i++) {
            writeByte(s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        if (s == null) {
            throw new NullPointerException();
        }
        for (int i = 0; i < s.length(); i++) {
            writeChar(s.charAt(i));
        }
    }

    @Override
    public void writeUTF(String str) throws IOException {
        // 实现UTF-8字符串写入
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        writeShort(bytes.length);
        write(bytes);
    }

    private void resize(int minCapacityAdd) throws IOException {
        int newLen =
                Math.max(this.memorySegment.size() * 2, this.memorySegment.size() + minCapacityAdd);
        byte[] nb;
        try {
            nb = new byte[newLen];
        } catch (NegativeArraySizeException e) {
            throw new IOException(
                    "Serialization failed because the record length would exceed 2GB (max addressable array size in Java).");
        } catch (OutOfMemoryError e) {
            // this was too large to allocate, try the smaller size (if possible)
            if (newLen > this.memorySegment.size() + minCapacityAdd) {
                newLen = this.memorySegment.size() + minCapacityAdd;
                try {
                    nb = new byte[newLen];
                } catch (OutOfMemoryError ee) {
                    // still not possible. give an informative exception message that reports the
                    // size
                    throw new IOException(
                            "Serialization failed because the record length would exceed 2GB (max addressable array size in Java).");
                }
            } else {
                throw new IOException(
                        "Serialization failed because the record length would exceed 2GB (max addressable array size in Java).");
            }
        }

        System.arraycopy(this.memorySegment.getHeapMemory(), 0, nb, 0, this.position);
        this.memorySegment = MemorySegment.wrap(nb);
    }

    @Override
    public void write(MemorySegment segment, int off, int len) throws IOException {
        if (len < 0 || off < 0 || off > segment.size() - len) {
            throw new IndexOutOfBoundsException(
                    String.format("offset: %d, length: %d, size: %d", off, len, segment.size()));
        }
        if (this.position > this.memorySegment.size() - len) {
            resize(len);
        }
        segment.copyTo(off, memorySegment, position, len);
        this.position += len;
    }
}
