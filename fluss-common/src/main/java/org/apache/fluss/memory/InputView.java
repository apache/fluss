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

import org.apache.fluss.annotation.PublicEvolving;

import java.io.EOFException;
import java.io.IOException;

/**
 * A very similar interface to {@link java.io.DataInput} but reading multi-bytes primitive types in
 * little endian.
 *
 * @since 0.2
 */
@PublicEvolving
public interface InputView {

    /**
     * Reads one input byte and returns {@code true} if that byte is nonzero, {@code false} if that
     * byte is zero. This method is suitable for reading the byte written by the {@link
     * OutputView#writeBoolean(boolean)}.
     *
     * @return the {@code boolean} value read.
     * @exception EOFException if this stream reaches the end before reading all the bytes.
     * @exception IOException if an I/O error occurs.
     */
    boolean readBoolean() throws IOException;

    /**
     * Reads and returns one input byte. The byte is treated as a signed value in the range {@code
     * -128} through {@code 127}, inclusive. This method is suitable for reading the byte written by
     * the {@link OutputView#writeByte(int)}.
     *
     * @return the 8-bit value read.
     * @exception EOFException if this stream reaches the end before reading all the bytes.
     * @exception IOException if an I/O error occurs.
     */
    byte readByte() throws IOException;

    /**
     * Reads two input bytes and returns a {@code short} value. This method is suitable for reading
     * the bytes written by the {@link OutputView#writeShort(int)}.
     *
     * @return the 16-bit value read.
     * @exception EOFException if this stream reaches the end before reading all the bytes.
     * @exception IOException if an I/O error occurs.
     */
    short readShort() throws IOException;

    /**
     * Reads four input bytes and returns an {@code int} value. This method is suitable for reading
     * bytes written by the {@link OutputView#writeInt(int)}.
     *
     * @return the {@code int} value read.
     * @exception EOFException if this stream reaches the end before reading all the bytes.
     * @exception IOException if an I/O error occurs.
     */
    int readInt() throws IOException;

    /**
     * Reads eight input bytes and returns a {@code long} value. This method is suitable for reading
     * bytes written by the {@link OutputView#writeLong(long)}.
     *
     * @return the {@code long} value read.
     * @exception EOFException if this stream reaches the end before reading all the bytes.
     * @exception IOException if an I/O error occurs.
     */
    long readLong() throws IOException;

    /**
     * Reads four input bytes and returns a {@code float} value. It does this by first constructing
     * an {@code int} value in exactly the manner of the {@code readInt} method, then converting
     * this {@code int} value to a {@code float} in exactly the manner of the method {@code
     * Float.intBitsToFloat}. This method is suitable for reading bytes written by the {@link
     * OutputView#writeFloat(float)}.
     *
     * @return the {@code float} value read.
     * @exception EOFException if this stream reaches the end before reading all the bytes.
     * @exception IOException if an I/O error occurs.
     */
    float readFloat() throws IOException;

    /**
     * Reads eight input bytes and returns a {@code double} value. It does this by first
     * constructing a {@code long} value in exactly the manner of the {@code readLong} method, then
     * converting this {@code long} value to a {@code double} in exactly the manner of the method
     * {@code Double.longBitsToDouble}. This method is suitable for reading bytes written by the
     * {@link OutputView#writeDouble(double)}.
     *
     * @return the {@code double} value read.
     * @exception EOFException if this stream reaches the end before reading all the bytes.
     * @exception IOException if an I/O error occurs.
     */
    double readDouble() throws IOException;

    /**
     * Reads some bytes from an input stream and stores them into the buffer array {@code b}. The
     * number of bytes read is equal to the length of {@code b}.
     *
     * <p>This method blocks until one of the following conditions occurs:
     *
     * <ul>
     *   <li>{@code b.length} bytes of input data are available, in which case a normal return is
     *       made.
     *   <li>End of file is detected, in which case an {@code EOFException} is thrown.
     *   <li>An I/O error occurs, in which case an {@code IOException} other than {@code
     *       EOFException} is thrown.
     * </ul>
     *
     * <p>If {@code b} is {@code null}, a {@code NullPointerException} is thrown. If {@code
     * b.length} is zero, then no bytes are read. Otherwise, the first byte read is stored into
     * element {@code b[0]}, the next one into {@code b[1]}, and so on. If an exception is thrown
     * from this method, then it may be that some but not all bytes of {@code b} have been updated
     * with data from the input stream.
     *
     * @param b the buffer into which the data is read.
     * @exception EOFException if this stream reaches the end before reading all the bytes.
     * @exception IOException if an I/O error occurs.
     */
    void readFully(byte[] b) throws IOException;

    /**
     * Reads {@code len} bytes from an input stream.
     *
     * <p>This method blocks until one of the following conditions occurs:
     *
     * <ul>
     *   <li>{@code len} bytes of input data are available, in which case a normal return is made.
     *   <li>End of file is detected, in which case an {@code EOFException} is thrown.
     *   <li>An I/O error occurs, in which case an {@code IOException} other than {@code
     *       EOFException} is thrown.
     * </ul>
     *
     * <p>If {@code b} is {@code null}, a {@code NullPointerException} is thrown. If {@code off} is
     * negative, or {@code len} is negative, or {@code off+len} is greater than the length of the
     * array {@code b}, then an {@code IndexOutOfBoundsException} is thrown. If {@code len} is zero,
     * then no bytes are read. Otherwise, the first byte read is stored into element {@code b[off]},
     * the next one into {@code b[off+1]}, and so on. The number of bytes read is, at most, equal to
     * {@code len}.
     *
     * @param b the buffer into which the data is read.
     * @param offset an int specifying the offset into the data.
     * @param len an int specifying the number of bytes to read.
     * @exception EOFException if this stream reaches the end before reading all the bytes.
     * @exception IOException if an I/O error occurs.
     */
    void readFully(byte[] b, int offset, int len) throws IOException;
}
