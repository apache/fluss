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

package org.apache.fluss.row;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * A Variant represents a type that can contain one of: 1) Primitive: A type and corresponding value
 * (e.g. INT, STRING); 2) Array: An ordered list of Variant values; 3) Object: An unordered
 * collection of string/Variant pairs (i.e. key/value pairs). An object may not contain duplicate
 * keys.
 *
 * <p>A Variant is encoded with 2 binary values, the value and the metadata.
 *
 * <p>The Variant Binary Encoding allows representation of semi-structured data (e.g. JSON) in a
 * form that can be efficiently queried by path. The design is intended to allow efficient access to
 * nested data even in the presence of very wide or deep structures.
 *
 * @since 0.9
 */
@PublicEvolving
public interface Variant {

    String METADATA = "metadata";

    String VALUE = "value";

    /** Returns the variant metadata (string dictionary). */
    byte[] metadata();

    /** Returns the variant value (binary-encoded variant value). */
    byte[] value();

    /** Returns the size of the variant in bytes. */
    long sizeInBytes();

    /** Returns a copy of the variant. */
    Variant copy();

    // ------------------------------------------------------------------------------------------
    // Static Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Converts a byte array in the format [4-byte value length (big-endian)][value][metadata] into
     * a {@link Variant} object.
     */
    static Variant bytesToVariant(byte[] bytes) {
        if (bytes.length < 4) {
            throw new IllegalArgumentException(
                    "Invalid variant bytes: length must be at least 4, got " + bytes.length);
        }
        int valueLength =
                ((bytes[0] & 0xFF) << 24)
                        | ((bytes[1] & 0xFF) << 16)
                        | ((bytes[2] & 0xFF) << 8)
                        | (bytes[3] & 0xFF);
        byte[] value = new byte[valueLength];
        System.arraycopy(bytes, 4, value, 0, valueLength);
        int metadataLength = bytes.length - 4 - valueLength;
        byte[] metadata = new byte[metadataLength];
        System.arraycopy(bytes, 4 + valueLength, metadata, 0, metadataLength);
        return new GenericVariant(value, metadata);
    }

    /**
     * Converts a {@link Variant} object into a byte array in the format [4-byte value length
     * (big-endian)][value][metadata].
     */
    static byte[] variantToBytes(Variant variant) {
        byte[] value = variant.value();
        byte[] metadata = variant.metadata();
        int totalSize = 4 + value.length + metadata.length;
        byte[] combined = new byte[totalSize];
        combined[0] = (byte) ((value.length >> 24) & 0xFF);
        combined[1] = (byte) ((value.length >> 16) & 0xFF);
        combined[2] = (byte) ((value.length >> 8) & 0xFF);
        combined[3] = (byte) (value.length & 0xFF);
        System.arraycopy(value, 0, combined, 4, value.length);
        System.arraycopy(metadata, 0, combined, 4 + value.length, metadata.length);
        return combined;
    }
}
