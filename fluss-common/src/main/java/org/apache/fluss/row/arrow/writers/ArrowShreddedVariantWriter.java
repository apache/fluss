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

package org.apache.fluss.row.arrow.writers;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.DataGetters;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.BigIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.BitVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.DateDayVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.Float4Vector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.Float8Vector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.SmallIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TinyIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarBinaryVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarCharVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.variant.ShreddedField;
import org.apache.fluss.types.variant.ShreddingSchema;
import org.apache.fluss.types.variant.Variant;
import org.apache.fluss.types.variant.VariantUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@link ArrowFieldWriter} for shredded Variant columns. Writes a Variant value into a StructVector
 * with the following layout:
 *
 * <pre>
 * StructVector (Variant column)
 *   ├── VarBinaryVector "metadata"       (Variant metadata dictionary)
 *   ├── VarBinaryVector "value"          (residual Variant value binary)
 *   └── StructVector "typed_value"
 *         ├── StructVector "fieldName1"
 *         │     ├── VarBinaryVector "value"       (per-field fallback binary)
 *         │     └── &lt;TypedVector&gt; "typed_value"   (typed value when type matches)
 *         └── StructVector "fieldName2"
 *               ├── VarBinaryVector "value"
 *               └── &lt;TypedVector&gt; "typed_value"
 * </pre>
 *
 * <p>For each shredded field, if the Variant value type matches the target type, the value is
 * written to the typed_value vector; if there's a type mismatch, the raw binary is written to the
 * per-field value vector as a fallback; if the field is absent, both are null.
 *
 * <p>The residual (metadata + value) contains the remaining non-shredded fields.
 */
@Internal
public class ArrowShreddedVariantWriter extends ArrowFieldWriter {

    private final VarBinaryVector metadataVector;
    private final VarBinaryVector valueVector;
    private final StructVector typedValueVector;

    // Per-field vectors extracted from typed_value children
    private final StructVector[] fieldStructVectors;
    private final VarBinaryVector[] fieldValueVectors;
    private final FieldVector[] fieldTypedValueVectors;

    private final ShreddingSchema shreddingSchema;
    private final DataType[] shreddedTypes;
    private final String[] fieldPaths;

    public ArrowShreddedVariantWriter(
            StructVector variantStructVector, ShreddingSchema shreddingSchema) {
        super(variantStructVector);
        this.shreddingSchema = shreddingSchema;

        // Extract top-level children
        this.metadataVector = (VarBinaryVector) variantStructVector.getChild("metadata");
        this.valueVector = (VarBinaryVector) variantStructVector.getChild("value");
        this.typedValueVector = (StructVector) variantStructVector.getChild("typed_value");

        // Extract per-field vectors from typed_value
        List<ShreddedField> fields = shreddingSchema.getFields();
        int numFields = fields.size();
        this.fieldStructVectors = new StructVector[numFields];
        this.fieldValueVectors = new VarBinaryVector[numFields];
        this.fieldTypedValueVectors = new FieldVector[numFields];
        this.shreddedTypes = new DataType[numFields];
        this.fieldPaths = new String[numFields];

        for (int i = 0; i < numFields; i++) {
            ShreddedField sf = fields.get(i);
            fieldPaths[i] = sf.getFieldPath();
            shreddedTypes[i] = sf.getShreddedType();
            fieldStructVectors[i] = (StructVector) typedValueVector.getChild(sf.getFieldPath());
            fieldValueVectors[i] = (VarBinaryVector) fieldStructVectors[i].getChild("value");
            fieldTypedValueVectors[i] = (FieldVector) fieldStructVectors[i].getChild("typed_value");
        }
    }

    @Override
    public void write(int rowIndex, DataGetters getters, int ordinal, boolean handleSafe) {
        StructVector variantStruct = (StructVector) fieldVector;
        if (getters.isNullAt(ordinal)) {
            // Set null for the entire Variant struct
            variantStruct.setNull(rowIndex);
            metadataVector.setNull(rowIndex);
            valueVector.setNull(rowIndex);
            typedValueVector.setNull(rowIndex);
            for (int i = 0; i < fieldStructVectors.length; i++) {
                fieldStructVectors[i].setNull(rowIndex);
                fieldValueVectors[i].setNull(rowIndex);
                fieldTypedValueVectors[i].setNull(rowIndex);
            }
        } else {
            doWrite(rowIndex, getters, ordinal, handleSafe);
        }
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        StructVector variantStruct = (StructVector) fieldVector;
        Variant variant = row.getVariant(ordinal);

        // Always mark the top-level Variant struct as defined
        variantStruct.setIndexDefined(rowIndex);

        if (!variant.isObject()) {
            // Not an object - write entire variant as residual, typed_value all "not present"
            metadataVector.setSafe(rowIndex, variant.metadata());
            valueVector.setSafe(rowIndex, variant.value());
            typedValueVector.setIndexDefined(rowIndex);
            for (int i = 0; i < fieldStructVectors.length; i++) {
                fieldStructVectors[i].setIndexDefined(rowIndex);
                fieldValueVectors[i].setNull(rowIndex);
                fieldTypedValueVectors[i].setNull(rowIndex);
            }
            return;
        }

        byte[] metadata = variant.metadata();
        byte[] value = variant.value();
        int numObjFields = VariantUtil.objectSize(value, 0);

        // Track which original fields are shredded successfully
        boolean[] fieldShredded = new boolean[numObjFields];

        // Mark typed_value struct as defined
        typedValueVector.setIndexDefined(rowIndex);

        // For each shredded field, try to extract and write
        for (int si = 0; si < fieldPaths.length; si++) {
            String fieldPath = fieldPaths[si];
            int fieldId = VariantUtil.findFieldId(metadata, fieldPath);

            // Field struct is notNullable (required), always mark as defined
            fieldStructVectors[si].setIndexDefined(rowIndex);

            if (fieldId < 0) {
                // Field not present: both children null = "field absent" per semantic matrix
                fieldValueVectors[si].setNull(rowIndex);
                fieldTypedValueVectors[si].setNull(rowIndex);
                continue;
            }

            int valueOffset = VariantUtil.findFieldValueOffset(value, 0, fieldId);
            if (valueOffset < 0) {
                fieldValueVectors[si].setNull(rowIndex);
                fieldTypedValueVectors[si].setNull(rowIndex);
                continue;
            }

            // Check for Variant null values - kept in residual only.
            // Both children null = "field not present" in the shredding layer,
            // while the actual null value is preserved in residual.
            int basicType = VariantUtil.basicType(value, valueOffset);
            if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE
                    && VariantUtil.primitiveTypeId(value, valueOffset)
                            == VariantUtil.PRIMITIVE_TYPE_NULL) {
                fieldValueVectors[si].setNull(rowIndex);
                fieldTypedValueVectors[si].setNull(rowIndex);
                continue;
            }

            // Try to write the value to the typed_value column
            boolean written =
                    writeTypedValue(
                            fieldTypedValueVectors[si],
                            rowIndex,
                            value,
                            valueOffset,
                            shreddedTypes[si]);

            if (written) {
                // Type matched: typed_value is set, per-field value is null
                fieldValueVectors[si].setNull(rowIndex);
                markFieldShredded(value, fieldId, fieldShredded);
            } else {
                // Type mismatch: write raw binary to per-field value, typed_value is null
                int fieldValueSize = VariantUtil.valueSize(value, valueOffset);
                byte[] rawBytes =
                        Arrays.copyOfRange(value, valueOffset, valueOffset + fieldValueSize);
                fieldValueVectors[si].setSafe(rowIndex, rawBytes);
                fieldTypedValueVectors[si].setNull(rowIndex);
                markFieldShredded(value, fieldId, fieldShredded);
            }
        }

        // Build residual variant (original fields minus shredded ones)
        buildAndWriteResidual(rowIndex, metadata, value, numObjFields, fieldShredded);
    }

    private void markFieldShredded(byte[] value, int fieldId, boolean[] fieldShredded) {
        int numFields = VariantUtil.objectSize(value, 0);
        for (int i = 0; i < numFields; i++) {
            if (VariantUtil.objectFieldId(value, 0, i) == fieldId) {
                fieldShredded[i] = true;
                return;
            }
        }
    }

    /**
     * Attempts to write a variant field value to a typed Arrow vector.
     *
     * @return true if the value was successfully written (type compatible), false otherwise
     */
    private boolean writeTypedValue(
            FieldVector vector, int rowIndex, byte[] value, int valueOffset, DataType targetType) {
        int basicType = VariantUtil.basicType(value, valueOffset);

        DataTypeRoot root = targetType.getTypeRoot();
        try {
            switch (root) {
                case BOOLEAN:
                    return writeBooleanValue(vector, rowIndex, value, valueOffset, basicType);
                case TINYINT:
                    return writeTinyIntValue(vector, rowIndex, value, valueOffset, basicType);
                case SMALLINT:
                    return writeSmallIntValue(vector, rowIndex, value, valueOffset, basicType);
                case INTEGER:
                    return writeIntValue(vector, rowIndex, value, valueOffset, basicType);
                case BIGINT:
                    return writeBigIntValue(vector, rowIndex, value, valueOffset, basicType);
                case FLOAT:
                    return writeFloatValue(vector, rowIndex, value, valueOffset, basicType);
                case DOUBLE:
                    return writeDoubleValue(vector, rowIndex, value, valueOffset, basicType);
                case STRING:
                case CHAR:
                    return writeStringValue(vector, rowIndex, value, valueOffset, basicType);
                case DATE:
                    return writeDateValue(vector, rowIndex, value, valueOffset, basicType);
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return writeTimestampValue(
                            vector,
                            rowIndex,
                            value,
                            valueOffset,
                            basicType,
                            VariantUtil.PRIMITIVE_TYPE_TIMESTAMP);
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return writeTimestampValue(
                            vector,
                            rowIndex,
                            value,
                            valueOffset,
                            basicType,
                            VariantUtil.PRIMITIVE_TYPE_TIMESTAMP_NTZ);
                default:
                    return false;
            }
        } catch (Exception e) {
            return false;
        }
    }

    private boolean writeBooleanValue(
            FieldVector vector, int rowIndex, byte[] value, int valueOffset, int basicType) {
        if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE) {
            int tid = VariantUtil.primitiveTypeId(value, valueOffset);
            if (tid == VariantUtil.PRIMITIVE_TYPE_TRUE) {
                ((BitVector) vector).setSafe(rowIndex, 1);
                return true;
            } else if (tid == VariantUtil.PRIMITIVE_TYPE_FALSE) {
                ((BitVector) vector).setSafe(rowIndex, 0);
                return true;
            }
        }
        return false;
    }

    private boolean writeTinyIntValue(
            FieldVector vector, int rowIndex, byte[] value, int valueOffset, int basicType) {
        if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE
                && VariantUtil.primitiveTypeId(value, valueOffset)
                        == VariantUtil.PRIMITIVE_TYPE_INT8) {
            ((TinyIntVector) vector).setSafe(rowIndex, VariantUtil.getByte(value, valueOffset));
            return true;
        }
        return false;
    }

    private boolean writeSmallIntValue(
            FieldVector vector, int rowIndex, byte[] value, int valueOffset, int basicType) {
        if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE) {
            int tid = VariantUtil.primitiveTypeId(value, valueOffset);
            if (tid == VariantUtil.PRIMITIVE_TYPE_INT16) {
                ((SmallIntVector) vector)
                        .setSafe(rowIndex, VariantUtil.getShort(value, valueOffset));
                return true;
            } else if (tid == VariantUtil.PRIMITIVE_TYPE_INT8) {
                ((SmallIntVector) vector)
                        .setSafe(rowIndex, VariantUtil.getByte(value, valueOffset));
                return true;
            }
        }
        return false;
    }

    private boolean writeIntValue(
            FieldVector vector, int rowIndex, byte[] value, int valueOffset, int basicType) {
        if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE) {
            int tid = VariantUtil.primitiveTypeId(value, valueOffset);
            if (tid == VariantUtil.PRIMITIVE_TYPE_INT32) {
                ((IntVector) vector).setSafe(rowIndex, VariantUtil.getInt(value, valueOffset));
                return true;
            } else if (tid == VariantUtil.PRIMITIVE_TYPE_INT16) {
                ((IntVector) vector).setSafe(rowIndex, VariantUtil.getShort(value, valueOffset));
                return true;
            } else if (tid == VariantUtil.PRIMITIVE_TYPE_INT8) {
                ((IntVector) vector).setSafe(rowIndex, VariantUtil.getByte(value, valueOffset));
                return true;
            }
        }
        return false;
    }

    private boolean writeBigIntValue(
            FieldVector vector, int rowIndex, byte[] value, int valueOffset, int basicType) {
        if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE) {
            int tid = VariantUtil.primitiveTypeId(value, valueOffset);
            if (tid == VariantUtil.PRIMITIVE_TYPE_INT64) {
                ((BigIntVector) vector).setSafe(rowIndex, VariantUtil.getLong(value, valueOffset));
                return true;
            } else if (tid == VariantUtil.PRIMITIVE_TYPE_INT32) {
                ((BigIntVector) vector).setSafe(rowIndex, VariantUtil.getInt(value, valueOffset));
                return true;
            } else if (tid == VariantUtil.PRIMITIVE_TYPE_INT16) {
                ((BigIntVector) vector).setSafe(rowIndex, VariantUtil.getShort(value, valueOffset));
                return true;
            } else if (tid == VariantUtil.PRIMITIVE_TYPE_INT8) {
                ((BigIntVector) vector).setSafe(rowIndex, VariantUtil.getByte(value, valueOffset));
                return true;
            }
        }
        return false;
    }

    private boolean writeFloatValue(
            FieldVector vector, int rowIndex, byte[] value, int valueOffset, int basicType) {
        if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE
                && VariantUtil.primitiveTypeId(value, valueOffset)
                        == VariantUtil.PRIMITIVE_TYPE_FLOAT) {
            ((Float4Vector) vector).setSafe(rowIndex, VariantUtil.getFloat(value, valueOffset));
            return true;
        }
        return false;
    }

    private boolean writeDoubleValue(
            FieldVector vector, int rowIndex, byte[] value, int valueOffset, int basicType) {
        if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE
                && VariantUtil.primitiveTypeId(value, valueOffset)
                        == VariantUtil.PRIMITIVE_TYPE_DOUBLE) {
            ((Float8Vector) vector).setSafe(rowIndex, VariantUtil.getDouble(value, valueOffset));
            return true;
        }
        return false;
    }

    private boolean writeStringValue(
            FieldVector vector, int rowIndex, byte[] value, int valueOffset, int basicType) {
        if (basicType == VariantUtil.BASIC_TYPE_SHORT_STRING
                || (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE
                        && VariantUtil.primitiveTypeId(value, valueOffset)
                                == VariantUtil.PRIMITIVE_TYPE_STRING)) {
            String str = VariantUtil.getString(value, valueOffset);
            byte[] utf8 = str.getBytes(StandardCharsets.UTF_8);
            ((VarCharVector) vector).setSafe(rowIndex, utf8);
            return true;
        }
        return false;
    }

    private boolean writeDateValue(
            FieldVector vector, int rowIndex, byte[] value, int valueOffset, int basicType) {
        if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE
                && VariantUtil.primitiveTypeId(value, valueOffset)
                        == VariantUtil.PRIMITIVE_TYPE_DATE) {
            ((DateDayVector) vector).setSafe(rowIndex, VariantUtil.getDate(value, valueOffset));
            return true;
        }
        return false;
    }

    private boolean writeTimestampValue(
            FieldVector vector,
            int rowIndex,
            byte[] value,
            int valueOffset,
            int basicType,
            int expectedPrimitiveTypeId) {
        if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE
                && VariantUtil.primitiveTypeId(value, valueOffset) == expectedPrimitiveTypeId) {
            long micros =
                    expectedPrimitiveTypeId == VariantUtil.PRIMITIVE_TYPE_TIMESTAMP
                            ? VariantUtil.getTimestamp(value, valueOffset)
                            : VariantUtil.getTimestampNtz(value, valueOffset);
            ((TimeStampMicroVector) vector).setSafe(rowIndex, micros);
            return true;
        }
        return false;
    }

    /**
     * Builds and writes the residual Variant (original object minus successfully shredded fields).
     * The original metadata dictionary is preserved to keep nested object field IDs valid.
     */
    private void buildAndWriteResidual(
            int rowIndex, byte[] metadata, byte[] value, int numFields, boolean[] fieldShredded) {
        // Always write metadata - it's needed for decoding both residual and per-field value
        metadataVector.setSafe(rowIndex, metadata);

        // Special-case: empty object {} has numFields == 0
        if (numFields == 0) {
            byte[] emptyObjValue =
                    VariantUtil.encodeObject(
                            java.util.Collections.emptyList(), java.util.Collections.emptyList());
            valueVector.setSafe(rowIndex, emptyObjValue);
            return;
        }

        // Check if all fields are shredded
        boolean allShredded = true;
        for (boolean s : fieldShredded) {
            if (!s) {
                allShredded = false;
                break;
            }
        }

        if (allShredded) {
            // All fields are in typed_value - no residual value needed
            valueVector.setNull(rowIndex);
            return;
        }

        // Collect non-shredded fields
        List<Integer> residualFieldIds = new ArrayList<>();
        List<byte[]> residualFieldValues = new ArrayList<>();

        for (int i = 0; i < numFields; i++) {
            if (!fieldShredded[i]) {
                int fieldId = VariantUtil.objectFieldId(value, 0, i);
                int fieldValueOffset = VariantUtil.objectFieldValueOffset(value, 0, i);
                int fieldValueSize = VariantUtil.valueSize(value, fieldValueOffset);
                byte[] fieldValueBytes =
                        Arrays.copyOfRange(
                                value, fieldValueOffset, fieldValueOffset + fieldValueSize);
                residualFieldIds.add(fieldId);
                residualFieldValues.add(fieldValueBytes);
            }
        }

        // Re-encode residual object with original metadata's field IDs
        byte[] residualValue = VariantUtil.encodeObject(residualFieldIds, residualFieldValues);
        valueVector.setSafe(rowIndex, residualValue);
    }

    @Override
    public void reset() {
        super.reset();
        metadataVector.reset();
        valueVector.reset();
        typedValueVector.reset();
        for (int i = 0; i < fieldStructVectors.length; i++) {
            fieldStructVectors[i].reset();
            fieldValueVectors[i].reset();
            fieldTypedValueVectors[i].reset();
        }
    }
}
