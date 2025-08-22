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

/**
 * Data type of a variable-length character string.
 *
 * <p>A conversion from and to {@code byte[]} assumes UTF-8 encoding.
 *
 * @since 0.8
 */
@PublicEvolving
public final class VarCharType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final int MIN_LENGTH = 1;

    public static final int MAX_LENGTH = Integer.MAX_VALUE;

    public static final int DEFAULT_LENGTH = 1;

    public static final VarCharType STRING_TYPE = new VarCharType(MAX_LENGTH);

    private static final String FORMAT = "VARCHAR(%d)";

    private static final String MAX_FORMAT = "STRING";

    private final int length;

    public VarCharType(boolean isNullable, int length) {
        super(isNullable, DataTypeRoot.STRING);
        if (length < MIN_LENGTH) {
            throw new IllegalArgumentException(
                    String.format(
                            "Variable character string length must be between %d and %d (both inclusive).",
                            MIN_LENGTH, MAX_LENGTH));
        }
        this.length = length;
    }

    public VarCharType(int length) {
        this(true, length);
    }

    public VarCharType() {
        this(DEFAULT_LENGTH);
    }

    public int getLength() {
        return length;
    }

    public int defaultSize() {
        return length == MAX_LENGTH ? 20 : length;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new VarCharType(isNullable, length);
    }

    @Override
    public String asSerializableString() {
        if (length == MAX_LENGTH) {
            return withNullability(MAX_FORMAT);
        }
        return withNullability(FORMAT, length);
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
        VarCharType that = (VarCharType) o;
        return length == that.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), length);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public static VarCharType stringType(boolean isNullable) {
        return new VarCharType(isNullable, MAX_LENGTH);
    }
}
