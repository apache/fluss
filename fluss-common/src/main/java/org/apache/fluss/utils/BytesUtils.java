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

package org.apache.fluss.utils;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.utils.crc.Java;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Utils for bytes. */
@Internal
public final class BytesUtils {
    private BytesUtils() {}

    /**
     * Read the given byte buffer from its current position to its limit into a byte array.
     *
     * @param buffer The buffer to read from
     */
    public static byte[] toArray(ByteBuffer buffer) {
        return toArray(buffer, 0, buffer.remaining());
    }

    /**
     * Read a byte array from the given offset and size in the buffer.
     *
     * @param buffer The buffer to read from
     * @param offset The offset relative to the current position of the buffer
     * @param size The number of bytes to read into the array
     */
    public static byte[] toArray(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(
                    buffer.array(),
                    buffer.position() + buffer.arrayOffset() + offset,
                    dest,
                    0,
                    size);
        } else {
            int pos = buffer.position();
            buffer.position(pos + offset);
            buffer.get(dest);
            buffer.position(pos);
        }
        return dest;
    }

    /**
     * Check if the given first byte array ({@code prefix}) is a prefix of the second byte array
     * ({@code bytes}).
     *
     * @param prefix the prefix byte array
     * @param bytes the byte array to check if it has the prefix
     * @return true if the given bytes has the given prefix, false otherwise.
     */
    public static boolean prefixEquals(byte[] prefix, byte[] bytes) {
        return BEST_EQUAL_COMPARER.prefixEquals(prefix, bytes);
    }

    // -------------------------------------------------------------------------------------------

    private static final BytesPrefixComparer BEST_EQUAL_COMPARER;

    static {
        if (Java.IS_JAVA9_COMPATIBLE) {
            BEST_EQUAL_COMPARER = new Java9BytesPrefixComparer();
        } else {
            BEST_EQUAL_COMPARER = new PureJavaBytesPrefixComparer();
        }
    }

    /** Compare two byte arrays for equality. */
    private interface BytesPrefixComparer {

        /**
         * Check if the given first byte array ({@code prefix}) is a prefix of the second byte array
         * ({@code bytes}).
         *
         * @param prefix The prefix byte array
         * @param bytes The byte array to check if it has the prefix
         * @return true if the given bytes has the given prefix, false otherwise
         */
        boolean prefixEquals(byte[] prefix, byte[] bytes);
    }

    private static final class Java9BytesPrefixComparer implements BytesPrefixComparer {
        private static final Method EQUALS_METHOD;

        static {
            try {
                EQUALS_METHOD =
                        Class.forName(Arrays.class.getName())
                                .getMethod(
                                        "equals",
                                        byte[].class,
                                        int.class,
                                        int.class,
                                        byte[].class,
                                        int.class,
                                        int.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load Arrays.equals method", e);
            }
        }

        @Override
        public boolean prefixEquals(byte[] prefix, byte[] bytes) {
            if (prefix.length > bytes.length) {
                return false;
            }
            try {
                int fromIndex = 0; // inclusive
                int toIndex = prefix.length; // exclusive
                return (boolean)
                        EQUALS_METHOD.invoke(
                                null, prefix, fromIndex, toIndex, bytes, fromIndex, toIndex);
            } catch (Throwable e) {
                // should never happen
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * A pure Java implementation of the {@link BytesPrefixComparer} that does not rely on Java 9
     * APIs and compares bytes one by one which is slower.
     */
    private static final class PureJavaBytesPrefixComparer implements BytesPrefixComparer {

        @Override
        public boolean prefixEquals(byte[] prefix, byte[] bytes) {
            if (prefix.length > bytes.length) {
                return false;
            }
            for (int i = 0; i < prefix.length; i++) {
                if (prefix[i] != bytes[i]) {
                    return false;
                }
            }
            return true;
        }
    }
}
