/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.memory;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.io.IOException;

/**
 * A very similar interface to {@link java.io.DataOutput} but writing multi-bytes primitive types in
 * little endian.
 *
 * @since 0.2
 */
@PublicEvolving
public interface OutputView {
    /**
     * Writes a <code>boolean</code> value to this output stream. If the argument <code>v</code> is
     * <code>true</code>, the value <code>(byte)1</code> is written; if <code>v</code> is <code>
     * false</code>, the value <code>(byte)0</code> is written. The byte written by this method may
     * be read by the {@link InputView#readBoolean()}, which will then return a <code>
     * boolean</code> equal to <code>v</code>.
     *
     * @param v the boolean to be written.
     * @throws IOException if an I/O error occurs.
     */
    void writeBoolean(boolean v) throws IOException;

    /**
     * Writes to the output stream the eight low-order bits of the argument <code>v</code>. The 24
     * high-order bits of <code>v</code> are ignored. The byte written by this method may be read by
     * the {@link InputView#readByte()}, which will then return a <code>byte</code> equal to <code>
     * (byte)v</code>.
     *
     * @param v the byte value to be written.
     * @throws IOException if an I/O error occurs.
     */
    void writeByte(int v) throws IOException;

    /**
     * Writes two bytes to the output stream to represent the value of the argument. The byte values
     * to be written, in the little-endian order. The bytes written by this method may be read by
     * the {@link InputView#readShort()}, which will then return a <code>short</code> equal to
     * <code>(short)v</code>.
     *
     * @param v the <code>short</code> value to be written.
     * @throws IOException if an I/O error occurs.
     */
    void writeShort(int v) throws IOException;

    /**
     * Writes an <code>int</code> value, which is consisted of four bytes, to the output stream. The
     * byte values to be written, in the little-endian order. The bytes written by this method may
     * be read by the {@link InputView#readInt()} , which will then return an <code>int
     * </code> equal to <code>
     * v</code>.
     *
     * @param v the <code>int</code> value to be written.
     * @throws IOException if an I/O error occurs.
     */
    void writeInt(int v) throws IOException;

    /**
     * Writes a <code>long</code> value, which is consisted of eight bytes, to the output stream.
     * The byte values to be written, in the little-endian order. The bytes written by this method
     * may be read by the {@link InputView#readLong()}, which will then return a <code>
     * long</code> equal to <code>
     * v</code>.
     *
     * @param v the <code>long</code> value to be written.
     * @throws IOException if an I/O error occurs.
     */
    void writeLong(long v) throws IOException;

    /**
     * Writes a <code>float</code> value, which is consisted of four bytes, to the output stream. It
     * does this as if it first converts this <code>float</code> value to an <code>int</code> in
     * exactly the manner of the <code>Float.floatToIntBits</code> method and then writes the <code>
     * int</code> value in exactly the manner of the <code>writeInt</code> method. The bytes written
     * by this method may be read by the {@link InputView#readFloat()}, which will then return a
     * <code>float</code> equal to <code>v</code> .
     *
     * @param v the <code>float</code> value to be written.
     * @throws IOException if an I/O error occurs.
     */
    void writeFloat(float v) throws IOException;

    /**
     * Writes a <code>double</code> value, which is consisted of eight bytes, to the output stream.
     * It does this as if it first converts this <code>double</code> value to a <code>long</code> in
     * exactly the manner of the <code>Double.doubleToLongBits</code> method and then writes the
     * <code>long</code> value in exactly the manner of the <code>writeLong</code> method. The bytes
     * written by this method may be read by the {@link InputView#readDouble()}, which will then
     * return a <code>double</code> equal to <code>v
     * </code>.
     *
     * @param v the <code>double</code> value to be written.
     * @throws IOException if an I/O error occurs.
     */
    void writeDouble(double v) throws IOException;

    /**
     * Writes to the output stream all the bytes in array <code>b</code>. If <code>b</code> is
     * <code>null</code>, a <code>NullPointerException</code> is thrown. If <code>b.length</code> is
     * zero, then no bytes are written. Otherwise, the byte <code>b[0]</code> is written first, then
     * <code>b[1]</code>, and so on; the last byte written is <code>b[b.length-1]</code>.
     *
     * @param b the data.
     * @throws IOException if an I/O error occurs.
     */
    void write(byte[] b) throws IOException;

    /**
     * Writes <code>len</code> bytes from array <code>b</code>, in order, to the output stream. If
     * <code>b</code> is <code>null</code>, a <code>NullPointerException</code> is thrown. If <code>
     * off</code> is negative, or <code>len</code> is negative, or <code>off+len</code> is greater
     * than the length of the array <code>b</code>, then an <code>IndexOutOfBoundsException</code>
     * is thrown. If <code>len</code> is zero, then no bytes are written. Otherwise, the byte <code>
     * b[off]</code> is written first, then <code>b[off+1]</code>, and so on; the last byte written
     * is <code>b[off+len-1]</code>.
     *
     * @param b the data.
     * @param offset the start offset in the data.
     * @param len the number of bytes to write.
     * @throws IOException if an I/O error occurs.
     */
    void write(byte[] b, int offset, int len) throws IOException;
}
