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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.memory.InputView;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentInputView;
import com.alibaba.fluss.memory.OutputView;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/** Utils for encoding int/long to var length bytes. */
public class VarLengthUtils {

    /**
     * Read an integer stored in variable-length format using zig-zag decoding.
     *
     * @param buffer The buffer to read from
     * @param index The start index in buffer
     * @return The integer read
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     *     have been read
     */
    public static int readVarInt(byte[] buffer, int index) {
        int value = readUnsignedVarInt(buffer, index);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding.
     *
     * @param segment The memory segment to read from
     * @param position The start index in buffer
     * @return The integer read
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     *     have been read
     */
    public static int readVarInt(MemorySegment segment, int position) throws IOException {
        int value = readUnsignedVarInt(segment, position);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding.
     *
     * @param in The input to read from
     * @return The integer read
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     *     have been read
     * @throws IOException if {@link DataInput} throws {@link IOException}
     */
    public static int readVarInt(InputView in) throws IOException {
        int value = readUnsignedVarInt(in);
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding.
     *
     * @param in The input to read from
     * @return The integer read
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     *     have been read
     * @throws IOException if {@link InputStream} throws {@link IOException}
     * @throws EOFException if {@link InputStream} throws {@link EOFException}
     */
    public static int readUnsignedVarInt(InputView in) throws IOException {
        byte tmp = in.readByte();
        if (tmp >= 0) {
            return tmp;
        } else {
            int result = tmp & 127;
            if ((tmp = in.readByte()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 127) << 7;
                if ((tmp = in.readByte()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if ((tmp = in.readByte()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        result |= (tmp = in.readByte()) << 28;
                        if (tmp < 0) {
                            throw illegalVarIntException(result);
                        }
                    }
                }
            }
            return result;
        }
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     *     have been read
     */
    private static int readUnsignedVarInt(byte[] buffer, int index) {
        byte tmp = buffer[index++];
        if (tmp >= 0) {
            return tmp;
        } else {
            int result = tmp & 127;
            if ((tmp = buffer[index++]) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 127) << 7;
                if ((tmp = buffer[index++]) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if ((tmp = buffer[index++]) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        result |= (tmp = buffer[index]) << 28;
                        if (tmp < 0) {
                            throw illegalVarIntException(result);
                        }
                    }
                }
            }
            return result;
        }
    }

    /**
     * Read an integer stored in variable-length format using unsigned decoding.
     *
     * @param segment The memory segment to read from
     * @return The integer read
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes
     *     have been read
     */
    public static int readUnsignedVarInt(MemorySegment segment, int position) throws IOException {
        return readUnsignedVarInt(new MemorySegmentInputView(segment, position));
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding.
     *
     * @param in The input to read from
     * @return The long value read
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes
     *     have been read
     * @throws IOException if {@link InputView} throws {@link IOException}
     */
    public static long readVarLong(InputView in) throws IOException {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63) {
                throw illegalVarLongException(value);
            }
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding.
     *
     * @param buffer The buffer to read from
     * @param index the start index in buffer
     * @return The long value read
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes
     *     have been read
     */
    public static long readVarLong(byte[] buffer, int index) {
        long raw = readUnsignedVarLong(buffer, index);
        return (raw >>> 1) ^ -(raw & 1);
    }

    private static long readUnsignedVarLong(byte[] buffer, int index) {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer[index++]) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63) {
                throw illegalVarLongException(value);
            }
        }
        value |= b << i;
        return value;
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from <a
     * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">Google Protocol
     * Buffers</a> into the buffer.
     *
     * @param value The value to write
     * @param output The output to write to
     */
    public static void writeVarInt(OutputView output, int value) throws IOException {
        writeUnsignedVarInt((value << 1) ^ (value >> 31), output);
    }

    /**
     * Write the given integer following the variable-length unsigned encoding.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeUnsignedVarInt(int value, OutputView out) throws IOException {
        if ((value & (0xFFFFFFFF << 7)) == 0) {
            out.writeByte(value);
        } else {
            out.writeByte(value & 0x7F | 0x80);
            if ((value & (0xFFFFFFFF << 14)) == 0) {
                out.writeByte(value >>> 7);
            } else {
                out.writeByte((value >>> 7) & 0x7F | 0x80);
                if ((value & (0xFFFFFFFF << 21)) == 0) {
                    out.writeByte(value >>> 14);
                } else {
                    out.writeByte((byte) ((value >>> 14) & 0x7F | 0x80));
                    if ((value & (0xFFFFFFFF << 28)) == 0) {
                        out.writeByte(value >>> 21);
                    } else {
                        out.writeByte((value >>> 21) & 0x7F | 0x80);
                        out.writeByte(value >>> 28);
                    }
                }
            }
        }
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding.
     *
     * @param out The output to write to
     * @param value The value to write
     */
    public static void writeVarLong(OutputView out, long value) throws IOException {
        long v = (value << 1) ^ (value >> 63);
        while ((v & 0xffffffffffffff80L) != 0L) {
            out.writeByte(((int) v & 0x7f) | 0x80);
            v >>>= 7;
        }
        out.writeByte((byte) v);
    }

    /**
     * Number of bytes needed to encode an integer in variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfVarInt(int value) {
        return sizeOfUnsignedVarInt((value << 1) ^ (value >> 31));
    }

    /**
     * Number of bytes needed to encode a long in variable-length format.
     *
     * @param value The signed value
     * @see #sizeOfUnsignedVarInt(int)
     */
    public static int sizeOfVarLong(long value) {
        return sizeOfUnsignedVarLong((value << 1) ^ (value >> 63));
    }

    /**
     * Number of bytes needed to encode an integer in unsigned variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfUnsignedVarInt(int value) {
        int leadingZeros = Integer.numberOfLeadingZeros(value);
        int leadingZerosBelow38DividedBy7 = ((38 - leadingZeros) * 0b10010010010010011) >>> 19;
        return leadingZerosBelow38DividedBy7 + (leadingZeros >>> 5);
    }

    public static int sizeOfUnsignedVarLong(long v) {
        // For implementation notes @see #sizeOfUnsignedVarint(int)
        // Similar logic is applied to allow for 64bit input -> 1-9byte output.
        // return (70 - leadingZeros) / 7 + leadingZeros / 64;

        int leadingZeros = Long.numberOfLeadingZeros(v);
        int leadingZerosBelow70DividedBy7 = ((70 - leadingZeros) * 0b10010010010010011) >>> 19;
        return leadingZerosBelow70DividedBy7 + (leadingZeros >>> 6);
    }

    private static IllegalArgumentException illegalVarIntException(int value) {
        throw new IllegalArgumentException(
                "VarInt is too long, the most significant bit in the 5th byte is set, "
                        + "converted value: "
                        + Integer.toHexString(value));
    }

    private static IllegalArgumentException illegalVarLongException(long value) {
        throw new IllegalArgumentException(
                "VarLong is too long, most significant bit in the 10th byte is set, "
                        + "converted value: "
                        + Long.toHexString(value));
    }
}
