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

package org.apache.fluss.row.arrow.vectors;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.columnar.VariantColumnVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.BigIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.BitVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.DateDayVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.Float4Vector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.Float8Vector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.SmallIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TinyIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ValueVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarBinaryVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarCharVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.variant.ShreddedField;
import org.apache.fluss.types.variant.ShreddedVariant;
import org.apache.fluss.types.variant.ShreddingSchema;
import org.apache.fluss.types.variant.Variant;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Arrow column vector for shredded Variant columns. Reads from a StructVector with the layout:
 *
 * <pre>
 * StructVector (Variant column)
 *   ├── VarBinaryVector "metadata"
 *   ├── VarBinaryVector "value"           (residual)
 *   └── StructVector "typed_value"
 *         ├── StructVector "fieldName"
 *         │     ├── VarBinaryVector "value"       (per-field fallback binary)
 *         │     └── &lt;TypedVector&gt; "typed_value"   (typed value)
 *         └── ...
 * </pre>
 *
 * <p>For each shredded field:
 *
 * <ul>
 *   <li>typed_value non-null → use typed value directly
 *   <li>typed_value null, per-field value non-null → decode from binary using metadata
 *   <li>both null → field not present
 * </ul>
 *
 * <p>The complete Variant is reconstructed by merging typed_value fields with the residual.
 */
@Internal
public class ShreddedVariantColumnVector implements VariantColumnVector {

    private final StructVector variantStructVector;
    private final VarBinaryVector metadataVector;
    private final VarBinaryVector valueVector;

    private final VarBinaryVector[] fieldValueVectors;
    private final ValueVector[] fieldTypedValueVectors;

    private final DataType[] shreddedTypes;
    private final String[] fieldPaths;

    // Field name → index lookup (lazily built)
    private Map<String, Integer> fieldPathIndex;

    public ShreddedVariantColumnVector(
            StructVector variantStructVector, ShreddingSchema shreddingSchema) {
        this.variantStructVector = variantStructVector;

        this.metadataVector = (VarBinaryVector) variantStructVector.getChild("metadata");
        this.valueVector = (VarBinaryVector) variantStructVector.getChild("value");
        StructVector typedValueVector = (StructVector) variantStructVector.getChild("typed_value");

        List<ShreddedField> fields = shreddingSchema.getFields();
        int numFields = fields.size();
        // Per-field vectors
        StructVector[] fieldStructVectors = new StructVector[numFields];
        this.fieldValueVectors = new VarBinaryVector[numFields];
        this.fieldTypedValueVectors = new ValueVector[numFields];
        this.shreddedTypes = new DataType[numFields];
        this.fieldPaths = new String[numFields];

        for (int i = 0; i < numFields; i++) {
            ShreddedField sf = fields.get(i);
            fieldPaths[i] = sf.getFieldPath();
            shreddedTypes[i] = sf.getShreddedType();
            fieldStructVectors[i] = (StructVector) typedValueVector.getChild(sf.getFieldPath());
            fieldValueVectors[i] = (VarBinaryVector) fieldStructVectors[i].getChild("value");
            fieldTypedValueVectors[i] = fieldStructVectors[i].getChild("typed_value");
        }
    }

    @Override
    public Variant getVariant(int i) {
        // Eagerly materialize the variant to ensure the returned value is self-contained
        // and does not reference Arrow buffers (which may be released after iterator close).
        // This is O(N) per row where N = number of shredded fields, consistent with other
        // ColumnVector implementations that copy data on access.
        byte[] metadata = metadataVector.isNull(i) ? null : metadataVector.get(i);
        byte[] residual = valueVector.isNull(i) ? null : valueVector.get(i);
        return materializeVariant(metadata, residual, i);
    }

    @Override
    public boolean isNullAt(int i) {
        return variantStructVector.isNull(i);
    }

    private int getFieldPathIndex(String fieldName) {
        if (fieldPathIndex == null) {
            fieldPathIndex = new HashMap<>(fieldPaths.length);
            for (int i = 0; i < fieldPaths.length; i++) {
                fieldPathIndex.put(fieldPaths[i], i);
            }
        }
        Integer idx = fieldPathIndex.get(fieldName);
        return idx != null ? idx : -1;
    }

    /**
     * Builds a fully materialized ShreddedVariant by extracting all typed values from Arrow
     * vectors. Used as fallback when full Variant binary is needed (e.g., metadata()/value()).
     */
    private ShreddedVariant materializeVariant(
            byte[] metadata, @Nullable byte[] residualValue, int rowIndex) {
        Object[] typedValues = new Object[fieldPaths.length];
        byte[][] fallbackValues = new byte[fieldPaths.length][];
        for (int si = 0; si < fieldPaths.length; si++) {
            if (!fieldTypedValueVectors[si].isNull(rowIndex)) {
                typedValues[si] = extractTypedObject(si, rowIndex);
            } else if (!fieldValueVectors[si].isNull(rowIndex)) {
                fallbackValues[si] = fieldValueVectors[si].get(rowIndex);
            }
        }
        return new ShreddedVariant(
                metadata, residualValue, fieldPaths, shreddedTypes, typedValues, fallbackValues);
    }

    /**
     * Extracts a typed value from an Arrow vector as a Java object. This avoids encoding to Variant
     * binary, enabling downstream consumers (e.g., Flink's FlussVariant) to directly access the
     * typed value without decoding overhead.
     *
     * @return the typed Java object: Boolean, Byte, Short, Integer, Long, Float, Double, String,
     *     etc.
     */
    private Object extractTypedObject(int fieldIndex, int rowIndex) {
        ValueVector vector = fieldTypedValueVectors[fieldIndex];
        DataTypeRoot root = shreddedTypes[fieldIndex].getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return ((BitVector) vector).get(rowIndex) != 0;
            case TINYINT:
                return ((TinyIntVector) vector).get(rowIndex);
            case SMALLINT:
                return ((SmallIntVector) vector).get(rowIndex);
            case INTEGER:
                return ((IntVector) vector).get(rowIndex);
            case BIGINT:
                return ((BigIntVector) vector).get(rowIndex);
            case FLOAT:
                return ((Float4Vector) vector).get(rowIndex);
            case DOUBLE:
                return ((Float8Vector) vector).get(rowIndex);
            case STRING:
            case CHAR:
                {
                    byte[] utf8 = ((VarCharVector) vector).get(rowIndex);
                    return new String(utf8, StandardCharsets.UTF_8);
                }
            case DATE:
                return ((DateDayVector) vector).get(rowIndex);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimeStampMicroVector) vector).get(rowIndex);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported shredded type for extracting: " + shreddedTypes[fieldIndex]);
        }
    }
}
