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

package org.apache.fluss.types;

import org.apache.fluss.annotation.PublicStable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Data type of an associative array that maps keys to values (including {@code NULL}). A map cannot
 * contain duplicate keys; each key can map to at most one value. Map keys are always non-nullable
 * and will be automatically converted to non-nullable types if a nullable key type is provided.
 * There is no restriction of key types; it is the responsibility of the user to ensure uniqueness.
 * The map type is an extension to the SQL standard.
 *
 * @since 0.1
 */
@PublicStable
public final class MapType extends DataType {
    private static final long serialVersionUID = 1L;

    public static final String FORMAT = "MAP<%s, %s>";

    private final DataType keyType;

    private final DataType valueType;

    public MapType(boolean isNullable, DataType keyType, DataType valueType) {
        super(isNullable, DataTypeRoot.MAP);
        checkNotNull(keyType, "Key type must not be null.");
        checkNotNull(valueType, "Value type must not be null.");
        // Map keys are always non-nullable, convert automatically if needed
        this.keyType = keyType.isNullable() ? keyType.copy(false) : keyType;
        this.valueType = valueType;
    }

    public MapType(DataType keyType, DataType valueType) {
        this(true, keyType, valueType);
    }

    public DataType getKeyType() {
        return keyType;
    }

    public DataType getValueType() {
        return valueType;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new MapType(isNullable, keyType.copy(), valueType.copy());
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT, keyType.asSummaryString(), valueType.asSummaryString());
    }

    @Override
    public String asSerializableString() {
        return withNullability(
                FORMAT, keyType.asSerializableString(), valueType.asSerializableString());
    }

    @Override
    public List<DataType> getChildren() {
        return Collections.unmodifiableList(Arrays.asList(keyType, valueType));
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MapType mapType = (MapType) o;
        return keyType.equals(mapType.keyType) && valueType.equals(mapType.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyType, valueType);
    }
}
