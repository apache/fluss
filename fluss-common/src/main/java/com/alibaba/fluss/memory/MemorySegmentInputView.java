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

import java.io.EOFException;
import java.io.IOException;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

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
    public int readUnsignedByte() throws IOException {
        if (position < end) {
            return segment.get(position++) & 0xff;
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
    public int readUnsignedShort() throws IOException {
        if (position >= 0 && position < end - 1) {
            int v = segment.getShort(position) & 0xffff;
            position += 2;
            return v;
        } else {
            throw new EOFException();
        }
    }

    @Override
    public char readChar() throws IOException {
        if (position >= 0 && position < end - 1) {
            char v = segment.getChar(position);
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
    public String readLine() throws IOException {
        if (position >= end) {
            return null;
        }

        StringBuilder bld = new StringBuilder();

        try {
            int b;
            while ((b = readUnsignedByte()) != '\n') {
                if (b != '\r') {
                    bld.append((char) b);
                }
            }
        } catch (EOFException ignored) {
            // End of file reached
        }

        if (bld.length() == 0 && position >= end) {
            return null;
        }

        // trim a trailing carriage return
        int len = bld.length();
        if (len > 0 && bld.charAt(len - 1) == '\r') {
            bld.setLength(len - 1);
        }

        return bld.toString();
    }

    @Override
    public String readUTF() throws IOException {
        int utflen = readUnsignedShort();

        if (utflen == 0) {
            return "";
        }

        byte[] bytearr = new byte[utflen];
        char[] chararr = new char[utflen];

        int c, char2, char3;
        int count = 0;
        int chararrCount = 0;

        readFully(bytearr, 0, utflen);

        // ASCII optimization - fast path for ASCII characters
        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            if (c > 127) {
                break;
            }
            count++;
            chararr[chararrCount++] = (char) c;
        }

        // Handle multi-byte UTF-8 sequences
        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx - 1-byte character */
                    count++;
                    chararr[chararrCount++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx 10xx xxxx - 2-byte character */
                    count += 2;
                    if (count > utflen) {
                        throw new IOException("malformed input: partial character at end");
                    }
                    char2 = (int) bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80) {
                        throw new IOException("malformed input around byte " + count);
                    }
                    chararr[chararrCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx 10xx xxxx 10xx xxxx - 3-byte character */
                    count += 3;
                    if (count > utflen) {
                        throw new IOException("malformed input: partial character at end");
                    }
                    char2 = (int) bytearr[count - 2];
                    char3 = (int) bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                        throw new IOException("malformed input around byte " + (count - 1));
                    }
                    chararr[chararrCount++] =
                            (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | (char3 & 0x3F));
                    break;
                default:
                    /* 10xx xxxx, 1111 xxxx */
                    throw new IOException("malformed input around byte " + count);
            }
        }

        return new String(chararr, 0, chararrCount);
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

    @Override
    public int skipBytes(int n) throws IOException {
        if (n <= 0) {
            return 0;
        }

        int remainingBytes = end - position;
        int bytesToSkip = Math.min(n, remainingBytes);
        position += bytesToSkip;
        return bytesToSkip;
    }
}
