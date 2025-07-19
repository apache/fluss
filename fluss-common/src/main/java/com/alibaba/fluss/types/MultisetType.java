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

package com.alibaba.fluss.types;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Data type of a multiset (=bag). Unlike a set, it allows for multiple instances for each of its
 * elements with a common subtype. Each unique value (including {@code NULL}) is mapped to some
 * multiplicity. There is no restriction of element types; it is the responsibility of the user to
 * ensure uniqueness.
 *
 * <p>A conversion is possible through a map that assigns each value to an integer multiplicity
 * ({@code Map<t, Integer>}).
 *
 * @since 0.8
 */
@PublicEvolving
public class MultisetType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final String FORMAT = "MULTISET<%s>";

    private final DataType elementType;

    public MultisetType(boolean isNullable, DataType elementType) {
        super(isNullable, DataTypeRoot.MAP);
        this.elementType = checkNotNull(elementType, "Element type must not be null.");
    }

    public MultisetType(DataType elementType) {
        this(true, elementType);
    }

    public DataType getElementType() {
        return elementType;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new MultisetType(isNullable, elementType);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT, elementType.asSerializableString());
    }

    @Override
    public List<DataType> getChildren() {
        return Collections.emptyList();
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
        MultisetType that = (MultisetType) o;
        return elementType.equals(that.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), elementType);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
