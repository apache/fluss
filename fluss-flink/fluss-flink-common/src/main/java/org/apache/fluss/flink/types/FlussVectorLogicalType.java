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

package org.apache.fluss.flink.types;

import org.apache.fluss.annotation.PublicEvolving;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Custom Flink {@link LogicalType} representing a fixed-dimension dense vector {@code VECTOR(n)}.
 *
 * <p>This type is Fluss-specific and allows users to declare VECTOR columns in Flink SQL DDL when
 * using the Fluss connector. At the Flink planner level, VECTOR uses {@link LogicalTypeRoot#ARRAY}
 * as the type root, preserving Flink compatibility. The original {@code VECTOR(n)} descriptor —
 * including the dimension — is preserved so that the Fluss catalog can round-trip it to {@link
 * org.apache.fluss.types.VectorType}.
 *
 * <p>Equality comparisons on VECTOR values are not supported. The Flink planner will reject
 * equality predicates ({@code =}, {@code <>}, {@code IN}, {@code NOT IN}) on VECTOR columns because
 * {@link #supportsEquality()} returns {@code false}.
 *
 * <p>Children: {@code [FLOAT NOT NULL]} — one non-nullable FLOAT child element type.
 *
 * @since 0.7
 */
@PublicEvolving
public final class FlussVectorLogicalType extends LogicalType {

    private static final long serialVersionUID = 1L;

    /** Format string for the serializable string representation of this type. */
    public static final String FORMAT = "VECTOR(%d)";

    /** The non-nullable FLOAT element type used as the child. */
    private static final FloatType ELEMENT_TYPE = new FloatType(false);

    /** Default Java class used at the edges of the Flink table ecosystem. */
    private static final Class<?> DEFAULT_CONVERSION = float[].class;

    /** Supported input/output conversion classes. */
    private static final Set<String> INPUT_OUTPUT_CONVERSION =
            new HashSet<String>(Arrays.asList(float[].class.getName(), ArrayData.class.getName()));

    private final int dimension;

    /**
     * Creates a new {@code FlussVectorLogicalType} with the given dimension.
     *
     * @param isNullable whether values of this type may be {@code null}
     * @param dimension the number of elements in the vector; must be positive
     */
    public FlussVectorLogicalType(boolean isNullable, int dimension) {
        super(isNullable, LogicalTypeRoot.ARRAY);
        if (dimension <= 0) {
            throw new IllegalArgumentException(
                    "VECTOR dimension must be positive, got: " + dimension);
        }
        this.dimension = dimension;
    }

    /**
     * Creates a nullable {@code FlussVectorLogicalType} with the given dimension.
     *
     * @param dimension the number of elements in the vector; must be positive
     */
    public FlussVectorLogicalType(int dimension) {
        this(true, dimension);
    }

    /**
     * Returns the fixed number of elements in the vector.
     *
     * @return the dimension (positive integer)
     */
    public int getDimension() {
        return dimension;
    }

    /**
     * Returns the single child type: {@code FLOAT NOT NULL}.
     *
     * <p>VECTOR is conceptually an array of non-nullable FLOAT32 elements.
     */
    @Override
    public List<LogicalType> getChildren() {
        return Collections.singletonList((LogicalType) ELEMENT_TYPE);
    }

    /**
     * Returns {@code false} — equality comparisons on VECTOR values are not supported.
     *
     * <p>Dense vectors have no well-defined equality semantics in a SQL context (floating-point
     * precision issues; semantically users want similarity distance, not bit-exact equality). The
     * Flink planner will reject {@code WHERE embedding = ...} predicates for VECTOR columns.
     */
    public boolean supportsEquality() {
        return false;
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return DEFAULT_CONVERSION;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new FlussVectorLogicalType(isNullable, dimension);
    }

    /**
     * Returns the summary string representation: {@code VECTOR(n)} or {@code VECTOR(n) NOT NULL}.
     */
    @Override
    public String asSummaryString() {
        return withNullability(String.format(FORMAT, dimension));
    }

    /**
     * Returns the serializable string representation: {@code VECTOR(n)} or {@code VECTOR(n) NOT
     * NULL}.
     *
     * <p>This format is recognized by the Fluss catalog's {@link
     * org.apache.fluss.types.DataTypeParser} and can be used in DDL statements stored in Fluss
     * metadata.
     */
    @Override
    public String asSerializableString() {
        return withNullability(String.format(FORMAT, dimension));
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
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
        FlussVectorLogicalType that = (FlussVectorLogicalType) o;
        return dimension == that.dimension;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimension);
    }
}
