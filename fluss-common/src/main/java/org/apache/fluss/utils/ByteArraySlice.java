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

import java.util.Arrays;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An immutable view over a contiguous range of a byte array. */
@Internal
public final class ByteArraySlice {

    private final byte[] array;
    private final int offset;
    private final int length;

    private ByteArraySlice(byte[] array, int offset, int length) {
        this.array = array;
        this.offset = offset;
        this.length = length;
    }

    /** Creates a slice that covers the entire byte array. */
    public static ByteArraySlice wrap(byte[] array) {
        checkNotNull(array, "array must not be null");
        return new ByteArraySlice(array, 0, array.length);
    }

    /** Creates a slice over the specified range of the byte array. */
    public static ByteArraySlice wrap(byte[] array, int offset, int length) {
        checkNotNull(array, "array must not be null");
        checkArgument(offset >= 0, "offset must not be negative");
        checkArgument(length >= 0, "length must not be negative");
        checkArgument(
                offset <= array.length - length,
                "offset + length must not exceed the array length");
        return new ByteArraySlice(array, offset, length);
    }

    /** Returns the backing byte array. */
    public byte[] array() {
        return array;
    }

    /** Returns the start offset in the backing byte array. */
    public int offset() {
        return offset;
    }

    /** Returns the number of bytes in this slice. */
    public int length() {
        return length;
    }

    /** Returns this slice as a byte array, reusing the backing array when possible. */
    public byte[] toByteArray() {
        if (offset == 0 && length == array.length) {
            return array;
        }
        return Arrays.copyOfRange(array, offset, offset + length);
    }
}
