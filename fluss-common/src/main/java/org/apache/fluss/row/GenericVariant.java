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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * An internal data structure implementing {@link Variant}.
 *
 * <p>A Variant consists of two byte arrays:
 *
 * <ul>
 *   <li><b>value</b>: the binary-encoded variant value (header + data), supports nested JSON
 *       structures.
 *   <li><b>metadata</b>: the string dictionary (version number + deduplicated list of all object
 *       key names).
 * </ul>
 *
 * @since 0.9
 */
@PublicEvolving
public final class GenericVariant implements Variant, Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] value;
    private final byte[] metadata;

    public GenericVariant(byte[] value, byte[] metadata) {
        this.value = value;
        this.metadata = metadata;
    }

    @Override
    public byte[] value() {
        return value;
    }

    @Override
    public byte[] metadata() {
        return metadata;
    }

    @Override
    public long sizeInBytes() {
        return value.length + metadata.length;
    }

    @Override
    public Variant copy() {
        return new GenericVariant(
                Arrays.copyOf(value, value.length), Arrays.copyOf(metadata, metadata.length));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericVariant that = (GenericVariant) o;
        return Objects.deepEquals(value, that.value) && Objects.deepEquals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(value), Arrays.hashCode(metadata));
    }

    @Override
    public String toString() {
        return "GenericVariant{value="
                + Arrays.toString(value)
                + ", metadata="
                + Arrays.toString(metadata)
                + "}";
    }
}
