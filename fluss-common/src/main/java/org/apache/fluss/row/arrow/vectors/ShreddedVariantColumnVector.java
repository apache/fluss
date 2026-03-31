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
import org.apache.fluss.types.variant.VariantUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    private final StructVector typedValueVector;

    // Per-field vectors
    private final StructVector[] fieldStructVectors;
    private final VarBinaryVector[] fieldValueVectors;
    private final ValueVector[] fieldTypedValueVectors;

    private final ShreddingSchema shreddingSchema;
    private final DataType[] shreddedTypes;
    private final String[] fieldPaths;

    public ShreddedVariantColumnVector(
            StructVector variantStructVector, ShreddingSchema shreddingSchema) {
        this.variantStructVector = variantStructVector;
        this.shreddingSchema = shreddingSchema;

        this.metadataVector = (VarBinaryVector) variantStructVector.getChild("metadata");
        this.valueVector = (VarBinaryVector) variantStructVector.getChild("value");
        this.typedValueVector = (StructVector) variantStructVector.getChild("typed_value");

        List<ShreddedField> fields = shreddingSchema.getFields();
        int numFields = fields.size();
        this.fieldStructVectors = new StructVector[numFields];
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
            fieldTypedValueVectors[i] = (ValueVector) fieldStructVectors[i].getChild("typed_value");
        }
    }

    @Override
    public Variant getVariant(int i) {
        byte[] metadata = metadataVector.isNull(i) ? null : metadataVector.get(i);

        // Collect shredded field values
        // Field structs are notNullable (required); field presence is determined by
        // checking children: both value and typed_value null = "field not present".
        boolean hasShreddedValues = false;
        for (int si = 0; si < fieldPaths.length; si++) {
            if (!fieldValueVectors[si].isNull(i) || !fieldTypedValueVectors[si].isNull(i)) {
                hasShreddedValues = true;
                break;
            }
        }

        boolean hasResidual = !valueVector.isNull(i);

        if (!hasResidual && !hasShreddedValues) {
            // No data at all
            if (metadata == null) {
                return Variant.ofNull();
            }
            // Metadata exists but no value - shouldn't happen, but handle gracefully
            return Variant.ofNull();
        }

        if (!hasShreddedValues) {
            // Only residual - return as Variant(metadata, value)
            return new Variant(metadata, valueVector.get(i));
        }

        // Build ShreddedVariant: extract per-field typed objects and fallback values
        byte[] residualValue = hasResidual ? valueVector.get(i) : null;

        Object[] typedValues = new Object[fieldPaths.length];
        byte[][] fallbackValues = new byte[fieldPaths.length][];

        for (int si = 0; si < fieldPaths.length; si++) {
            if (!fieldTypedValueVectors[si].isNull(i)) {
                typedValues[si] = extractTypedObject(si, i);
            } else if (!fieldValueVectors[si].isNull(i)) {
                fallbackValues[si] = fieldValueVectors[si].get(i);
            }
        }

        return new ShreddedVariant(
                metadata, residualValue, fieldPaths, shreddedTypes, typedValues, fallbackValues);
    }

    @Override
    public boolean isNullAt(int i) {
        return variantStructVector.isNull(i);
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

    /**
     * Builds a Variant metadata dictionary from a list of field names. The names are sorted for
     * binary search support.
     */
    static byte[] buildMetadata(List<String> fieldNames) {
        List<String> sorted = new ArrayList<>(fieldNames);
        Collections.sort(sorted);

        byte[][] nameBytes = new byte[sorted.size()][];
        int totalStringBytes = 0;
        for (int i = 0; i < sorted.size(); i++) {
            nameBytes[i] = sorted.get(i).getBytes(StandardCharsets.UTF_8);
            totalStringBytes += nameBytes[i].length;
        }

        int dictSize = sorted.size();
        int totalSize = 1 + 4 + (dictSize + 1) * 4 + totalStringBytes;
        byte[] metadata = new byte[totalSize];

        metadata[0] =
                (byte)
                        (VariantUtil.METADATA_VERSION
                                | VariantUtil.METADATA_SORTED_STRINGS_BIT
                                | VariantUtil.METADATA_OFFSET_SIZE_BITS);

        VariantUtil.writeIntLE(metadata, 1, dictSize);

        int offsetPos = 5;
        int strOffset = 0;
        int strBytesStart = 5 + (dictSize + 1) * 4;
        for (int i = 0; i < dictSize; i++) {
            VariantUtil.writeIntLE(metadata, offsetPos, strOffset);
            offsetPos += 4;
            System.arraycopy(
                    nameBytes[i], 0, metadata, strBytesStart + strOffset, nameBytes[i].length);
            strOffset += nameBytes[i].length;
        }
        VariantUtil.writeIntLE(metadata, offsetPos, strOffset);

        return metadata;
    }
}
