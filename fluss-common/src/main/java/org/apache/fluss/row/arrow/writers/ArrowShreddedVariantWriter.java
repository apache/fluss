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
 * {@link ArrowFieldWriter} for shredded Variant columns. Writes a Variant value by extracting
 * shredded fields into typed Arrow columns and encoding the remaining fields as a residual Variant
 * binary.
 *
 * <p>The residual Variant retains the original metadata dictionary to avoid invalidating nested
 * object field IDs. Only the top-level object value is re-encoded with the shredded fields removed.
 */
@Internal
public class ArrowShreddedVariantWriter extends ArrowFieldWriter {

    private final ShreddingSchema shreddingSchema;
    private final FieldVector[] shreddedVectors;
    private final DataType[] shreddedTypes;
    private final String[] fieldPaths;

    public ArrowShreddedVariantWriter(
            VarBinaryVector residualVector,
            ShreddingSchema shreddingSchema,
            FieldVector[] shreddedVectors,
            DataType[] shreddedTypes) {
        super(residualVector);
        this.shreddingSchema = shreddingSchema;
        this.shreddedVectors = shreddedVectors;
        this.shreddedTypes = shreddedTypes;

        List<ShreddedField> fields = shreddingSchema.getFields();
        this.fieldPaths = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            fieldPaths[i] = fields.get(i).getFieldPath();
        }
    }

    @Override
    public void write(int rowIndex, DataGetters getters, int ordinal, boolean handleSafe) {
        if (getters.isNullAt(ordinal)) {
            // Set null for residual and all shredded columns
            fieldVector.setNull(rowIndex);
            for (FieldVector sv : shreddedVectors) {
                sv.setNull(rowIndex);
            }
        } else {
            doWrite(rowIndex, getters, ordinal, handleSafe);
        }
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        VarBinaryVector residualVector = (VarBinaryVector) fieldVector;
        Variant variant = row.getVariant(ordinal);

        if (!variant.isObject()) {
            // Not an object - write entire variant as residual, set shredded columns to null
            residualVector.setSafe(rowIndex, variant.toBytes());
            for (FieldVector sv : shreddedVectors) {
                sv.setNull(rowIndex);
            }
            return;
        }

        byte[] metadata = variant.metadata();
        byte[] value = variant.value();
        int numFields = VariantUtil.objectSize(value, 0);

        // Track which original fields are shredded successfully
        boolean[] fieldShredded = new boolean[numFields];

        // For each shredded field, try to extract and write
        for (int si = 0; si < fieldPaths.length; si++) {
            String fieldPath = fieldPaths[si];
            int fieldId = VariantUtil.findFieldId(metadata, fieldPath);

            if (fieldId < 0) {
                // Field not present in this variant
                shreddedVectors[si].setNull(rowIndex);
                continue;
            }

            int valueOffset = VariantUtil.findFieldValueOffset(value, 0, fieldId);
            if (valueOffset < 0) {
                shreddedVectors[si].setNull(rowIndex);
                continue;
            }

            // Try to write the value to the typed column
            boolean written =
                    writeShreddedValue(
                            shreddedVectors[si], rowIndex, value, valueOffset, shreddedTypes[si]);

            if (written) {
                // Mark this field as shredded by finding its index in the object
                markFieldShredded(value, fieldId, fieldShredded);
            } else {
                // Type mismatch - keep in residual, set shredded null
                shreddedVectors[si].setNull(rowIndex);
            }
        }

        // Build residual variant (original fields minus shredded ones)
        buildAndWriteResidual(residualVector, rowIndex, metadata, value, numFields, fieldShredded);
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
    private boolean writeShreddedValue(
            FieldVector vector, int rowIndex, byte[] value, int valueOffset, DataType targetType) {
        int basicType = VariantUtil.basicType(value, valueOffset);

        // Variant null values must NOT be shredded: a null in the shredded column is
        // indistinguishable from "field not present", which would silently drop the
        // explicit null from the reconstructed object. Keep null values in the residual
        // so that mergeVariant / buildFromShreddedOnly preserves them correctly.
        if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE
                && VariantUtil.primitiveTypeId(value, valueOffset)
                        == VariantUtil.PRIMITIVE_TYPE_NULL) {
            return false;
        }

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
        if (basicType == VariantUtil.BASIC_TYPE_PRIMITIVE) {
            int tid = VariantUtil.primitiveTypeId(value, valueOffset);
            if (tid == VariantUtil.PRIMITIVE_TYPE_DOUBLE) {
                ((Float8Vector) vector)
                        .setSafe(rowIndex, VariantUtil.getDouble(value, valueOffset));
                return true;
            } else if (tid == VariantUtil.PRIMITIVE_TYPE_FLOAT) {
                ((Float8Vector) vector)
                        .setSafe(rowIndex, (double) VariantUtil.getFloat(value, valueOffset));
                return true;
            }
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

    /**
     * Writes a timestamp variant value to a {@link TimeStampMicroVector}.
     *
     * @param expectedPrimitiveTypeId either {@link VariantUtil#PRIMITIVE_TYPE_TIMESTAMP} (with TZ)
     *     or {@link VariantUtil#PRIMITIVE_TYPE_TIMESTAMP_NTZ} (without TZ)
     */
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
            VarBinaryVector residualVector,
            int rowIndex,
            byte[] metadata,
            byte[] value,
            int numFields,
            boolean[] fieldShredded) {
        // Special-case: empty object {} has numFields == 0, so the loop below is vacuously
        // "all shredded", which would incorrectly write null instead of an empty-object residual.
        // An empty-object Variant is NOT the same as SQL NULL (null Variant), so we must write
        // the encoded {} residual explicitly to prevent data corruption on read-back.
        if (numFields == 0) {
            byte[] emptyObjValue =
                    VariantUtil.encodeObject(
                            java.util.Collections.emptyList(), java.util.Collections.emptyList());
            Variant emptyResidual = new Variant(metadata, emptyObjValue);
            residualVector.setSafe(rowIndex, emptyResidual.toBytes());
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
            // All fields are in shredded columns - no residual needed.
            // The reader (ShreddedVariantColumnVector) handles null residuals via
            // buildFromShreddedOnly(), reconstructing the Variant from shredded columns alone.
            residualVector.setNull(rowIndex);
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

        // Re-encode residual object with original metadata
        byte[] residualValue = VariantUtil.encodeObject(residualFieldIds, residualFieldValues);
        Variant residual = new Variant(metadata, residualValue);
        residualVector.setSafe(rowIndex, residual.toBytes());
    }

    @Override
    public void reset() {
        super.reset();
        for (FieldVector sv : shreddedVectors) {
            sv.reset();
        }
    }
}
