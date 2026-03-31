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

package org.apache.fluss.types.variant;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A lazily-merged Variant that carries shredded field data alongside residual binary data. Instead
 * of immediately merging typed_value columns back into Variant binary encoding (which is then
 * re-parsed by downstream consumers), this class defers the merge until {@link #metadata()} or
 * {@link #value()} is actually called.
 *
 * <p>This enables downstream consumers (e.g., Flink's FlussVariant) to directly access shredded
 * field values as Java objects (String, Long, Boolean, etc.) without any binary encoding/decoding
 * overhead when querying individual fields.
 *
 * <p>Data layout:
 *
 * <ul>
 *   <li><b>residualMetadata</b>: metadata dictionary from the residual Variant (may be null if no
 *       residual)
 *   <li><b>residualValue</b>: residual value bytes (may be null if all fields are shredded)
 *   <li><b>fieldNames</b>: parallel array of shredded field names
 *   <li><b>fieldTypes</b>: parallel array of shredded field data types
 *   <li><b>typedValues</b>: parallel array of Java objects (non-null if typed_value matched)
 *   <li><b>fallbackValues</b>: parallel array of Variant binary (non-null if typed_value didn't
 *       match)
 * </ul>
 *
 * @since 0.7
 */
@Internal
public class ShreddedVariant extends Variant {

    private static final long serialVersionUID = 1L;

    // Residual data (from the non-shredded portion)
    @Nullable private final byte[] residualMetadata;
    @Nullable private final byte[] residualValue;

    // Shredded fields: parallel arrays
    private final String[] fieldNames;
    private final DataType[] fieldTypes;
    private final Object[] typedValues; // Java objects: String, Long, Boolean, etc.
    private final byte[][] fallbackValues; // Variant binary when type didn't match

    // Field name → index lookup (lazily built)
    private transient Map<String, Integer> fieldIndexMap;

    // Lazily merged full Variant binary
    private transient byte[] mergedMetadata;
    private transient byte[] mergedValue;
    private transient boolean merged;

    /**
     * Creates a ShreddedVariant with residual data and shredded field data.
     *
     * @param residualMetadata metadata bytes from the residual (may be null)
     * @param residualValue residual value bytes (may be null if all fields are shredded)
     * @param fieldNames parallel array of shredded field names
     * @param fieldTypes parallel array of shredded field data types
     * @param typedValues parallel array of typed Java objects (null entry = not typed-matched)
     * @param fallbackValues parallel array of fallback Variant binary (null entry = not present)
     */
    public ShreddedVariant(
            @Nullable byte[] residualMetadata,
            @Nullable byte[] residualValue,
            String[] fieldNames,
            DataType[] fieldTypes,
            Object[] typedValues,
            byte[][] fallbackValues) {
        super(); // protected no-arg constructor
        this.residualMetadata = residualMetadata;
        this.residualValue = residualValue;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.typedValues = typedValues;
        this.fallbackValues = fallbackValues;
        this.merged = false;
    }

    // --------------------------------------------------------------------------------------------
    // Lazy merge: override metadata() and value() to trigger merge on demand
    // --------------------------------------------------------------------------------------------

    @Override
    public byte[] metadata() {
        ensureMerged();
        return mergedMetadata;
    }

    @Override
    public byte[] value() {
        ensureMerged();
        return mergedValue;
    }

    // --------------------------------------------------------------------------------------------
    // Override getFieldByName() to avoid expensive ensureMerged() for shredded fields
    // --------------------------------------------------------------------------------------------

    /**
     * Overrides {@link Variant#getFieldByName(String)} to provide a fast path for shredded fields.
     * Instead of triggering a full merge (encoding ALL shredded fields into a merged binary), this
     * method directly checks shredded fields first and encodes only the requested field.
     *
     * <p>This reduces the per-field lookup cost from O(N) (encoding all N shredded fields) to O(1)
     * (encoding only the requested field).
     */
    @Override
    public Variant getFieldByName(String fieldName) {
        // Fast path: check shredded fields first (avoids expensive ensureMerged)
        int idx = getFieldIndex(fieldName);
        if (idx >= 0) {
            if (typedValues[idx] != null) {
                byte[] encoded = encodeFieldValue(idx);
                if (encoded != null) {
                    byte[] meta = residualMetadata != null ? residualMetadata : emptyMetadata();
                    return new Variant(meta, encoded);
                }
            }
            if (fallbackValues[idx] != null) {
                byte[] meta = residualMetadata != null ? residualMetadata : emptyMetadata();
                return new Variant(meta, fallbackValues[idx]);
            }
            // Shredded field not present in this row - fall through to check residual
        }

        // Slow path: check residual value directly (still avoids full merge)
        if (residualValue == null || residualMetadata == null) {
            return null;
        }
        if (VariantUtil.basicType(residualValue, 0) != VariantUtil.BASIC_TYPE_OBJECT) {
            return null;
        }
        int fieldId = VariantUtil.findFieldId(residualMetadata, fieldName);
        if (fieldId < 0) {
            return null;
        }
        int valueOffset = VariantUtil.findFieldValueOffset(residualValue, 0, fieldId);
        if (valueOffset < 0) {
            return null;
        }
        int valueSize = VariantUtil.valueSize(residualValue, valueOffset);
        byte[] fieldValue = Arrays.copyOfRange(residualValue, valueOffset, valueOffset + valueSize);
        return new Variant(residualMetadata, fieldValue);
    }

    // --------------------------------------------------------------------------------------------
    // Internal: lazy merge logic
    // --------------------------------------------------------------------------------------------

    private void ensureMerged() {
        if (merged) {
            return;
        }
        doMerge();
        merged = true;
    }

    private void doMerge() {
        boolean hasResidual = residualValue != null;
        boolean hasShreddedValues = false;
        for (int i = 0; i < fieldNames.length; i++) {
            if (typedValues[i] != null || fallbackValues[i] != null) {
                hasShreddedValues = true;
                break;
            }
        }

        if (!hasResidual && !hasShreddedValues) {
            // No data at all, produce null variant
            mergedMetadata = Variant.emptyMetadata();
            mergedValue = VariantUtil.encodeNull();
            return;
        }

        if (!hasShreddedValues) {
            // Only residual
            mergedMetadata = residualMetadata;
            mergedValue = residualValue;
            return;
        }

        if (!hasResidual) {
            // Only shredded values
            buildFromShreddedOnly();
            return;
        }

        // Both residual and shredded - merge
        mergeWithResidual();
    }

    private void buildFromShreddedOnly() {
        List<String> presentNames = new ArrayList<>();
        List<byte[]> presentValues = new ArrayList<>();

        for (int i = 0; i < fieldNames.length; i++) {
            byte[] encoded = encodeFieldValue(i);
            if (encoded != null) {
                presentNames.add(fieldNames[i]);
                presentValues.add(encoded);
            }
        }

        if (presentNames.isEmpty()) {
            mergedMetadata = Variant.emptyMetadata();
            mergedValue = VariantUtil.encodeNull();
            return;
        }

        mergedMetadata =
                (residualMetadata != null) ? residualMetadata : buildMetadata(presentNames);

        List<Integer> fieldIds = new ArrayList<>();
        for (String name : presentNames) {
            fieldIds.add(VariantUtil.findFieldId(mergedMetadata, name));
        }
        mergedValue = VariantUtil.encodeObject(fieldIds, presentValues);
    }

    private void mergeWithResidual() {
        // Check if residual is an object
        int basicType = VariantUtil.basicType(residualValue, 0);
        if (basicType != VariantUtil.BASIC_TYPE_OBJECT) {
            // Residual is not an object - return as-is
            mergedMetadata = residualMetadata;
            mergedValue = residualValue;
            return;
        }

        // Collect fields from residual
        int numResidualFields = VariantUtil.objectSize(residualValue, 0);
        Set<String> allFieldNames = new LinkedHashSet<>();
        Map<String, byte[]> fieldValueMap = new LinkedHashMap<>();

        for (int i = 0; i < numResidualFields; i++) {
            int fieldId = VariantUtil.objectFieldId(residualValue, 0, i);
            String fieldName = VariantUtil.metadataFieldName(residualMetadata, fieldId);
            int fieldValueOffset = VariantUtil.objectFieldValueOffset(residualValue, 0, i);
            int fieldValueSize = VariantUtil.valueSize(residualValue, fieldValueOffset);
            byte[] fieldValueBytes =
                    Arrays.copyOfRange(
                            residualValue, fieldValueOffset, fieldValueOffset + fieldValueSize);
            allFieldNames.add(fieldName);
            fieldValueMap.put(fieldName, fieldValueBytes);
        }

        // Add shredded fields (overwrite residual if same name)
        boolean needNewMetadata = false;
        for (int i = 0; i < fieldNames.length; i++) {
            byte[] encoded = encodeFieldValue(i);
            if (encoded == null) {
                continue;
            }
            String name = fieldNames[i];
            if (!allFieldNames.contains(name)) {
                if (VariantUtil.findFieldId(residualMetadata, name) < 0) {
                    needNewMetadata = true;
                }
            }
            allFieldNames.add(name);
            fieldValueMap.put(name, encoded);
        }

        // Build metadata
        if (needNewMetadata) {
            Set<String> allMetadataNames = new LinkedHashSet<>();
            int origDictSize = VariantUtil.metadataDictSize(residualMetadata);
            for (int d = 0; d < origDictSize; d++) {
                allMetadataNames.add(VariantUtil.metadataFieldName(residualMetadata, d));
            }
            allMetadataNames.addAll(allFieldNames);
            mergedMetadata = buildMetadata(new ArrayList<>(allMetadataNames));
        } else {
            mergedMetadata = residualMetadata;
        }

        // Build merged object value
        List<Integer> fieldIds = new ArrayList<>();
        List<byte[]> fieldValuesList = new ArrayList<>();
        for (Map.Entry<String, byte[]> entry : fieldValueMap.entrySet()) {
            int fieldId = VariantUtil.findFieldId(mergedMetadata, entry.getKey());
            fieldIds.add(fieldId);
            fieldValuesList.add(entry.getValue());
        }
        mergedValue = VariantUtil.encodeObject(fieldIds, fieldValuesList);
    }

    // --------------------------------------------------------------------------------------------
    // Encode a shredded field's typed value or fallback to Variant binary
    // --------------------------------------------------------------------------------------------

    @Nullable
    private byte[] encodeFieldValue(int fieldIndex) {
        Object typed = typedValues[fieldIndex];
        if (typed != null) {
            return encodeTypedObject(typed, fieldTypes[fieldIndex]);
        }
        return fallbackValues[fieldIndex]; // field not present
    }

    /** Encodes a typed Java object to Variant value binary. */
    public static byte[] encodeTypedObject(Object value, DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return VariantUtil.encodeBoolean((Boolean) value);
            case TINYINT:
                return VariantUtil.encodeByte((Byte) value);
            case SMALLINT:
                return VariantUtil.encodeShort((Short) value);
            case INTEGER:
                return VariantUtil.encodeInt((Integer) value);
            case BIGINT:
                return VariantUtil.encodeLong((Long) value);
            case FLOAT:
                return VariantUtil.encodeFloat((Float) value);
            case DOUBLE:
                return VariantUtil.encodeDouble((Double) value);
            case STRING:
            case CHAR:
                return VariantUtil.encodeString(((String) value).getBytes(StandardCharsets.UTF_8));
            case DATE:
                return VariantUtil.encodeDate((Integer) value);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return VariantUtil.encodeTimestamp((Long) value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return VariantUtil.encodeTimestampNtz((Long) value);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported shredded type for encoding: " + type);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Metadata builder (same logic as ShreddedVariantColumnVector.buildMetadata)
    // --------------------------------------------------------------------------------------------

    static byte[] buildMetadata(List<String> fieldNames) {
        List<String> sorted = new ArrayList<>(fieldNames);
        java.util.Collections.sort(sorted);

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

    // --------------------------------------------------------------------------------------------
    // Field index lookup
    // --------------------------------------------------------------------------------------------

    private int getFieldIndex(String name) {
        if (fieldIndexMap == null) {
            Map<String, Integer> map = new HashMap<>(fieldNames.length);
            for (int i = 0; i < fieldNames.length; i++) {
                map.put(fieldNames[i], i);
            }
            fieldIndexMap = map;
        }
        Integer idx = fieldIndexMap.get(name);
        return idx != null ? idx : -1;
    }
}
