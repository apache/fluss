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

import java.util.Arrays;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A wrapper for byte[] that provides proper equals() and hashCode() implementations for use as Map
 * keys.
 *
 * <p>The hashCode is pre-computed at construction time for better performance when used in
 * hash-based collections.
 */
public final class ByteArrayWrapper {

    private final byte[] data;
    private final int hashCode;

    public ByteArrayWrapper(byte[] data) {
        this.data = checkNotNull(data, "data cannot be null");
        this.hashCode = Arrays.hashCode(data);
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ByteArrayWrapper)) {
            return false;
        }
        return Arrays.equals(data, ((ByteArrayWrapper) o).data);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "ByteArrayWrapper{length=" + data.length + "}";
    }
}
