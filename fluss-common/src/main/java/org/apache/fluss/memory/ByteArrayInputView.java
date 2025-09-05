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
import java.util.Arrays;

/** A byte array input implementation for the {@link InputView} interface. */
public class ByteArrayInputView implements InputView {

    private final byte[] data;
    private int position;

    public ByteArrayInputView(byte[] data) {
        this.data = data != null ? Arrays.copyOf(data, data.length) : new byte[0];
        this.position = 0;
    }

    @Override
    public boolean readBoolean() throws IOException {
        if (position < data.length) {
            return data[position++] != 0;
        } else {
            throw new EOFException();
        }
    }

    @Override
    public byte readByte() throws IOException {
        if (position < data.length) {
            return data[position++];
        } else {
            throw new EOFException();
        }
    }

    @Override
    public short readShort() throws IOException {
        if (position >= 0 && position < data.length - 1) {
            // Read in little endian
            short value = (short) (data[position] & 0xFF);
            value |= (short) ((data[position + 1] & 0xFF) << 8);
            position += 2;
            return value;
        } else {
            throw new EOFException();
        }
    }

    @Override
    public int readInt() throws IOException {
        if (position >= 0 && position < data.length - 3) {
            // Read in little endian
            int value = data[position] & 0xFF;
            value |= (data[position + 1] & 0xFF) << 8;
            value |= (data[position + 2] & 0xFF) << 16;
            value |= (data[position + 3] & 0xFF) << 24;
            position += 4;
            return value;
        } else {
            throw new EOFException();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (position >= 0 && position < data.length - 7) {
            // Read in little endian
            long value = (long) data[position] & 0xFF;
            value |= ((long) data[position + 1] & 0xFF) << 8;
            value |= ((long) data[position + 2] & 0xFF) << 16;
            value |= ((long) data[position + 3] & 0xFF) << 24;
            value |= ((long) data[position + 4] & 0xFF) << 32;
            value |= ((long) data[position + 5] & 0xFF) << 40;
            value |= ((long) data[position + 6] & 0xFF) << 48;
            value |= ((long) data[position + 7] & 0xFF) << 56;
            position += 8;
            return value;
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
                if (this.position <= this.data.length - len) {
                    System.arraycopy(this.data, this.position, b, offset, len);
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
