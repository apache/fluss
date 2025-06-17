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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** An utility class for I/O related functionality. */
public class IOUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

    /** The block size for byte operations in byte. */
    private static final int BLOCKSIZE = 4096;

    // ------------------------------------------------------------------------
    //  Byte copy operations
    // ------------------------------------------------------------------------

    /**
     * Copies from one stream to another.
     *
     * @param in InputStream to read from
     * @param out OutputStream to write to
     * @param buffSize the size of the buffer
     * @param close whether or not close the InputStream and OutputStream at the end. The streams
     *     are closed in the finally clause.
     * @return the number of bytes copied
     * @throws IOException thrown if an error occurred while writing to the output stream
     */
    private static long copyBytes(
            final InputStream in, final OutputStream out, final int buffSize, final boolean close)
            throws IOException {

        final PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        final byte[] buf = new byte[buffSize];
        try {
            long totalBytes = 0;
            int bytesRead = in.read(buf);
            while (bytesRead >= 0) {
                out.write(buf, 0, bytesRead);
                totalBytes += bytesRead;
                if ((ps != null) && ps.checkError()) {
                    throw new IOException("Unable to write to output stream.");
                }
                bytesRead = in.read(buf);
            }
            return totalBytes;
        } finally {
            if (close) {
                out.close();
                in.close();
            }
        }
    }

    /**
     * Copies from one stream to another. <strong>closes the input and output streams at the
     * end</strong>.
     *
     * @param in InputStream to read from
     * @param out OutputStream to write to
     * @return the number of bytes copied
     * @throws IOException thrown if an I/O error occurs while copying
     */
    public static long copyBytes(final InputStream in, final OutputStream out) throws IOException {
        return copyBytes(in, out, BLOCKSIZE, true);
    }

    /**
     * Copies from one stream to another.
     *
     * @param in InputStream to read from
     * @param out OutputStream to write to
     * @param close whether or not close the InputStream and OutputStream at the end. The streams
     *     are closed in the finally clause.
     * @return the number of bytes copied
     * @throws IOException thrown if an I/O error occurs while copying
     */
    public static long copyBytes(final InputStream in, final OutputStream out, final boolean close)
            throws IOException {
        return copyBytes(in, out, BLOCKSIZE, close);
    }

    /** Closes all elements in the iterable with closeQuietly(). */
    public static void closeAllQuietly(Iterable<? extends AutoCloseable> closeables) {
        if (null != closeables) {
            for (AutoCloseable closeable : closeables) {
                closeQuietly(closeable);
            }
        }
    }

    /**
     * Closes the given AutoCloseable.
     *
     * <p><b>Important:</b> This method is expected to never throw an exception.
     */
    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * Closes {@code closeable} and if an exception is thrown, it is logged at the WARN level. <b>Be
     * cautious when passing method references as an argument.</b> For example:
     *
     * <p>{@code closeQuietly(task::stop, "source task");}
     *
     * <p>Although this method gracefully handles null {@link AutoCloseable} objects, attempts to
     * take a method reference from a null object will result in a {@link NullPointerException}. In
     * the example code above, it would be the caller's responsibility to ensure that {@code task}
     * was non-null before attempting to use a method reference from it.
     */
    public static void closeQuietly(AutoCloseable closeable, String name) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                LOG.warn(
                        "Failed to close {} with type {}", name, closeable.getClass().getName(), t);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Stream input skipping
    // ------------------------------------------------------------------------

    /**
     * Reads len bytes in a loop.
     *
     * @param in The InputStream to read from
     * @param buf The buffer to fill
     * @throws IOException if it could not read requested number of bytes for any reason (including
     *     EOF)
     */
    public static void readFully(final InputStream in, final byte[] buf) throws IOException {
        readFully(in, buf, 0, buf.length);
    }

    /**
     * Reads len bytes in a loop.
     *
     * @param in The InputStream to read from
     * @param buf The buffer to fill
     * @param off offset from the buffer
     * @param len the length of bytes to read
     * @throws IOException if it could not read requested number of bytes for any reason (including
     *     EOF)
     */
    public static void readFully(final InputStream in, final byte[] buf, int off, final int len)
            throws IOException {
        int toRead = len;
        while (toRead > 0) {
            final int ret = in.read(buf, off, toRead);
            if (ret < 0) {
                throw new IOException("Premature EOF from inputStream");
            }
            toRead -= ret;
            off += ret;
        }
    }

    /**
     * Read data from the input stream to the given byte buffer until there are no bytes remaining
     * in the buffer or the end of the stream has been reached.
     *
     * @param inputStream Input stream to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred (it must be backed
     *     by an array)
     * @return number of byte read from the input stream
     * @throws IOException If an I/O error occurs
     */
    public static int readFully(InputStream inputStream, ByteBuffer destinationBuffer)
            throws IOException {
        if (!destinationBuffer.hasArray()) {
            throw new IllegalArgumentException("destinationBuffer must be backed by an array");
        }
        int initialOffset = destinationBuffer.arrayOffset() + destinationBuffer.position();
        byte[] array = destinationBuffer.array();
        int length = destinationBuffer.remaining();
        int totalBytesRead = 0;
        do {
            int bytesRead =
                    inputStream.read(
                            array, initialOffset + totalBytesRead, length - totalBytesRead);
            if (bytesRead == -1) {
                break;
            }
            totalBytesRead += bytesRead;
        } while (length > totalBytesRead);
        destinationBuffer.position(destinationBuffer.position() + totalBytesRead);
        return totalBytesRead;
    }
}
