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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.InternalArray;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Data type of a fixed-dimension dense vector {@code VECTOR(n)} where {@code n} is the number of
 * elements. {@code n} must be between 1 and {@link Integer#MAX_VALUE}.
 *
 * <p>Internally represented as an Arrow {@code FixedSizeList<Float32>} for zero-copy handoff to
 * downstream lake storage (e.g. Lance).
 *
 * <p>Vector values are held in {@link InternalArray} with float elements at runtime.
 *
 * <p>Equality comparisons between VECTOR values (=, !=, IN, NOT IN) are not supported.
 *
 * @since 0.7
 */
@PublicEvolving
public final class VectorType extends DataType {

    private static final long serialVersionUID = 1L;

    /** String format for serialization: {@code VECTOR(n)}. */
    public static final String FORMAT = "VECTOR(%d)";

    /** Default element type when not specified. */
    public static final VectorElementType DEFAULT_ELEMENT_TYPE = VectorElementType.FLOAT32;

    private final int dimension;
    private final VectorElementType elementType;

    /**
     * Creates a {@link VectorType} with the given nullability, dimension, and element type.
     *
     * @param isNullable whether this type allows null values
     * @param dimension the number of elements in the vector; must be positive
     * @param elementType the precision of each element
     */
    public VectorType(boolean isNullable, int dimension, VectorElementType elementType) {
        super(isNullable, DataTypeRoot.VECTOR);
        checkArgument(dimension > 0, "Dimension must be positive, got: %s", dimension);
        if (elementType != VectorElementType.FLOAT32) {
            throw new UnsupportedOperationException(
                    elementType + " is reserved for future use. Only FLOAT32 is supported.");
        }
        this.dimension = dimension;
        this.elementType = elementType;
    }

    /**
     * Creates a nullable {@link VectorType} with {@link VectorElementType#FLOAT32} elements.
     *
     * @param dimension the number of elements in the vector; must be positive
     */
    public VectorType(int dimension) {
        this(true, dimension, DEFAULT_ELEMENT_TYPE);
    }

    /** Returns the fixed number of elements in this vector type. */
    public int getDimension() {
        return dimension;
    }

    /** Returns the element precision of this vector type. */
    public VectorElementType getElementType() {
        return elementType;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new VectorType(isNullable, dimension, elementType);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT, dimension);
    }

    @Override
    public List<DataType> getChildren() {
        return Collections.emptyList();
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
        VectorType that = (VectorType) o;
        return dimension == that.dimension && elementType == that.elementType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimension, elementType);
    }
}
