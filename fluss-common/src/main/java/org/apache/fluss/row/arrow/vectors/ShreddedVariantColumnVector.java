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
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.variant.ShreddedField;
import org.apache.fluss.types.variant.ShreddingSchema;
import org.apache.fluss.types.variant.Variant;
import org.apache.fluss.types.variant.VariantUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Arrow column vector for shredded Variant columns. Reconstructs complete {@link Variant} values by
 * merging the residual Variant binary with values from typed shredded columns.
 *
 * <p>When reading, if shredded columns have non-null values, they are merged back into the residual
 * Variant object to reconstruct the complete original value. If no shredded columns have values,
 * the residual is returned as-is.
 */
@Internal
public class ShreddedVariantColumnVector implements VariantColumnVector {

    private final VarBinaryVector residualVector;
    private final ValueVector[] shreddedValueVectors;
    private final ShreddingSchema shreddingSchema;
    private final DataType[] shreddedTypes;
    private final String[] fieldPaths;

    public ShreddedVariantColumnVector(
            VarBinaryVector residualVector,
            ValueVector[] shreddedValueVectors,
            ShreddingSchema shreddingSchema,
            DataType[] shreddedTypes) {
        this.residualVector = residualVector;
        this.shreddedValueVectors = shreddedValueVectors;
        this.shreddingSchema = shreddingSchema;
        this.shreddedTypes = shreddedTypes;

        List<ShreddedField> fields = shreddingSchema.getFields();
        this.fieldPaths = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            fieldPaths[i] = fields.get(i).getFieldPath();
        }
    }

    @Override
    public Variant getVariant(int i) {
        // Check if any shredded column has non-null value
        boolean hasShreddedValues = false;
        for (ValueVector sv : shreddedValueVectors) {
            if (!sv.isNull(i)) {
                hasShreddedValues = true;
                break;
            }
        }

        if (residualVector.isNull(i)) {
            if (!hasShreddedValues) {
                // Both residual and all shredded are null - shouldn't reach here
                // if isNullAt was checked first
                return Variant.ofNull();
            }
            // Only shredded values, no residual - build from shredded only
            return buildFromShreddedOnly(i);
        }

        Variant residual = Variant.fromBytes(residualVector.get(i));

        if (!hasShreddedValues) {
            return residual;
        }

        // Merge residual + shredded values
        return mergeVariant(residual, i);
    }

    @Override
    public boolean isNullAt(int i) {
        // Null only if both residual and all shredded columns are null
        if (!residualVector.isNull(i)) {
            return false;
        }
        for (ValueVector sv : shreddedValueVectors) {
            if (!sv.isNull(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Builds a Variant from shredded columns only (when residual is null). Creates a new metadata
     * dictionary containing all present field names.
     */
    private Variant buildFromShreddedOnly(int rowIndex) {
        List<String> fieldNames = new ArrayList<>();
        List<byte[]> fieldValues = new ArrayList<>();

        for (int si = 0; si < shreddedValueVectors.length; si++) {
            if (!shreddedValueVectors[si].isNull(rowIndex)) {
                fieldNames.add(fieldPaths[si]);
                fieldValues.add(
                        extractAndEncode(shreddedValueVectors[si], rowIndex, shreddedTypes[si]));
            }
        }

        if (fieldNames.isEmpty()) {
            return Variant.ofNull();
        }

        // Build metadata dictionary
        byte[] metadata = buildMetadata(fieldNames);

        // Build field IDs based on the metadata dictionary
        List<Integer> fieldIds = new ArrayList<>();
        for (String name : fieldNames) {
            fieldIds.add(VariantUtil.findFieldId(metadata, name));
        }

        byte[] value = VariantUtil.encodeObject(fieldIds, fieldValues);
        return new Variant(metadata, value);
    }

    /**
     * Merges a residual Variant with values from shredded columns. The original metadata dictionary
     * is extended if new field names are needed.
     */
    private Variant mergeVariant(Variant residual, int rowIndex) {
        byte[] metadata = residual.metadata();
        byte[] value = residual.value();

        if (!residual.isObject()) {
            // Residual is not an object - return as-is
            return residual;
        }

        // Collect fields from residual
        int numResidualFields = VariantUtil.objectSize(value, 0);

        // Collect all field names and values from both residual and shredded
        Set<String> allFieldNames = new LinkedHashSet<>();
        Map<String, byte[]> fieldValueMap = new LinkedHashMap<>();

        // Add residual fields first
        for (int i = 0; i < numResidualFields; i++) {
            int fieldId = VariantUtil.objectFieldId(value, 0, i);
            String fieldName = VariantUtil.metadataFieldName(metadata, fieldId);
            int fieldValueOffset = VariantUtil.objectFieldValueOffset(value, 0, i);
            int fieldValueSize = VariantUtil.valueSize(value, fieldValueOffset);
            byte[] fieldValueBytes =
                    Arrays.copyOfRange(value, fieldValueOffset, fieldValueOffset + fieldValueSize);
            allFieldNames.add(fieldName);
            fieldValueMap.put(fieldName, fieldValueBytes);
        }

        // Add shredded fields (overrides residual if field exists in both)
        boolean needNewMetadata = false;
        for (int si = 0; si < shreddedValueVectors.length; si++) {
            if (!shreddedValueVectors[si].isNull(rowIndex)) {
                String fieldPath = fieldPaths[si];
                byte[] encodedValue =
                        extractAndEncode(shreddedValueVectors[si], rowIndex, shreddedTypes[si]);
                if (!allFieldNames.contains(fieldPath)) {
                    // Check if field name exists in metadata dictionary
                    if (VariantUtil.findFieldId(metadata, fieldPath) < 0) {
                        needNewMetadata = true;
                    }
                }
                allFieldNames.add(fieldPath);
                fieldValueMap.put(fieldPath, encodedValue);
            }
        }

        // If we need new field names in metadata, rebuild it
        byte[] mergedMetadata;
        if (needNewMetadata) {
            // Build new metadata containing ALL field names from the original dictionary
            // (including nested object field names) plus any new shredded field names.
            // We must preserve all original dictionary entries because nested objects in the
            // residual may reference field IDs that are only present in the original metadata.
            Set<String> allMetadataNames = new LinkedHashSet<>();
            int origDictSize = VariantUtil.metadataDictSize(metadata);
            for (int d = 0; d < origDictSize; d++) {
                allMetadataNames.add(VariantUtil.metadataFieldName(metadata, d));
            }
            allMetadataNames.addAll(allFieldNames);
            mergedMetadata = buildMetadata(new ArrayList<>(allMetadataNames));
        } else {
            mergedMetadata = metadata;
        }

        // Build merged object
        List<Integer> fieldIds = new ArrayList<>();
        List<byte[]> fieldValuesList = new ArrayList<>();
        for (Map.Entry<String, byte[]> entry : fieldValueMap.entrySet()) {
            int fieldId = VariantUtil.findFieldId(mergedMetadata, entry.getKey());
            fieldIds.add(fieldId);
            fieldValuesList.add(entry.getValue());
        }

        byte[] mergedValue = VariantUtil.encodeObject(fieldIds, fieldValuesList);
        return new Variant(mergedMetadata, mergedValue);
    }

    /**
     * Extracts a typed value from an Arrow vector and encodes it as Variant value bytes.
     *
     * @param vector the Arrow vector to read from
     * @param rowIndex the row index
     * @param type the expected data type
     * @return the encoded Variant value bytes
     */
    private byte[] extractAndEncode(ValueVector vector, int rowIndex, DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                {
                    int val = ((BitVector) vector).get(rowIndex);
                    return VariantUtil.encodeBoolean(val != 0);
                }
            case TINYINT:
                {
                    byte val = ((TinyIntVector) vector).get(rowIndex);
                    return VariantUtil.encodeByte(val);
                }
            case SMALLINT:
                {
                    short val = ((SmallIntVector) vector).get(rowIndex);
                    return VariantUtil.encodeShort(val);
                }
            case INTEGER:
                {
                    int val = ((IntVector) vector).get(rowIndex);
                    return VariantUtil.encodeInt(val);
                }
            case BIGINT:
                {
                    long val = ((BigIntVector) vector).get(rowIndex);
                    return VariantUtil.encodeLong(val);
                }
            case FLOAT:
                {
                    float val = ((Float4Vector) vector).get(rowIndex);
                    return VariantUtil.encodeFloat(val);
                }
            case DOUBLE:
                {
                    double val = ((Float8Vector) vector).get(rowIndex);
                    return VariantUtil.encodeDouble(val);
                }
            case STRING:
            case CHAR:
                {
                    byte[] utf8 = ((VarCharVector) vector).get(rowIndex);
                    return VariantUtil.encodeString(utf8);
                }
            case DATE:
                {
                    int daysSinceEpoch = ((DateDayVector) vector).get(rowIndex);
                    return VariantUtil.encodeDate(daysSinceEpoch);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    long micros = ((TimeStampMicroVector) vector).get(rowIndex);
                    return VariantUtil.encodeTimestamp(micros);
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    long micros = ((TimeStampMicroVector) vector).get(rowIndex);
                    return VariantUtil.encodeTimestampNtz(micros);
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported shredded type for encoding: " + type);
        }
    }

    /**
     * Builds a Variant metadata dictionary from a list of field names. The names are sorted for
     * binary search support.
     */
    static byte[] buildMetadata(List<String> fieldNames) {
        // Sort names for sorted_strings optimization
        List<String> sorted = new ArrayList<>(fieldNames);
        Collections.sort(sorted);

        // Calculate sizes
        byte[][] nameBytes = new byte[sorted.size()][];
        int totalStringBytes = 0;
        for (int i = 0; i < sorted.size(); i++) {
            nameBytes[i] = sorted.get(i).getBytes(StandardCharsets.UTF_8);
            totalStringBytes += nameBytes[i].length;
        }

        int dictSize = sorted.size();
        // header(1) + dictSize(4) + offsets((dictSize+1)*4) + string_bytes
        int totalSize = 1 + 4 + (dictSize + 1) * 4 + totalStringBytes;
        byte[] metadata = new byte[totalSize];

        // Header: version=1, sorted_strings=true
        metadata[0] =
                (byte) (VariantUtil.METADATA_VERSION | VariantUtil.METADATA_SORTED_STRINGS_BIT);

        // Dictionary size
        VariantUtil.writeIntLE(metadata, 1, dictSize);

        // Offsets and string bytes
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
        // Last offset
        VariantUtil.writeIntLE(metadata, offsetPos, strOffset);

        return metadata;
    }
}
