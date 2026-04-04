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
import org.apache.fluss.types.DataTypeRoot;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects statistics about fields within Variant records. Analyzes each Variant value to track
 * field presence and type distribution, which is used by {@link ShreddingSchemaInferrer} to
 * determine optimal shredding strategies.
 *
 * <p>This collector supports:
 *
 * <ul>
 *   <li>Single record analysis via {@link #collect(Variant)}
 *   <li>Batch analysis via {@link #collectBatch(List)}
 *   <li>Distributed merging via {@link #merge(VariantStatisticsCollector)}
 * </ul>
 *
 * @since 0.7
 */
@Internal
public class VariantStatisticsCollector {

    /** Per-field statistics, keyed by field path. */
    private final Map<String, FieldStatistics> fieldStats;

    /** Total number of records analyzed. */
    private long totalRecords;

    public VariantStatisticsCollector() {
        this.fieldStats = new HashMap<>();
        this.totalRecords = 0;
    }

    /**
     * Analyzes a single Variant record and updates field statistics.
     *
     * <p>Only top-level object fields are analyzed in the current implementation. Nested objects
     * and arrays are not recursively traversed.
     *
     * @param variant the Variant value to analyze
     */
    public void collect(Variant variant) {
        totalRecords++;
        if (variant == null || variant.isNull()) {
            return;
        }
        if (!variant.isObject()) {
            // Only analyze object-type variants for shredding
            return;
        }
        collectObjectFields(variant, "");
    }

    /**
     * Analyzes a batch of Variant records.
     *
     * @param variants the list of Variant values to analyze
     */
    public void collectBatch(List<Variant> variants) {
        for (Variant variant : variants) {
            collect(variant);
        }
    }

    /**
     * Merges statistics from another collector into this one. Used for distributed scenarios where
     * different partitions/buckets collect statistics independently.
     *
     * @param other the other collector to merge
     */
    public void merge(VariantStatisticsCollector other) {
        this.totalRecords += other.totalRecords;
        for (Map.Entry<String, FieldStatistics> entry : other.fieldStats.entrySet()) {
            FieldStatistics existing = this.fieldStats.get(entry.getKey());
            if (existing != null) {
                existing.merge(entry.getValue());
            } else {
                this.fieldStats.put(entry.getKey(), new FieldStatistics(entry.getValue()));
            }
        }
    }

    /** Returns the total number of records analyzed. */
    public long getTotalRecords() {
        return totalRecords;
    }

    /** Returns an unmodifiable view of the current field statistics. */
    public Map<String, FieldStatistics> getStatistics() {
        return Collections.unmodifiableMap(fieldStats);
    }

    /** Resets the collector to its initial state. */
    public void reset() {
        fieldStats.clear();
        totalRecords = 0;
    }

    // --------------------------------------------------------------------------------------------
    // Internal methods
    // --------------------------------------------------------------------------------------------

    /**
     * Recursively collects field statistics from an object variant.
     *
     * @param variant the object variant to analyze
     * @param prefix the field path prefix (empty string for top-level)
     */
    private void collectObjectFields(Variant variant, String prefix) {
        // Iterate over all fields in the object using metadata
        byte[] metadata = variant.metadata();
        byte[] value = variant.value();

        int numFields = variant.objectSize();
        for (int i = 0; i < numFields; i++) {
            // Get field name from metadata using the field ID stored in the object
            int fieldId = VariantUtil.objectFieldId(value, 0, i);
            String fieldName = VariantUtil.metadataFieldName(metadata, fieldId);
            String fieldPath = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;

            // Get the field value
            int fieldValueOffset = VariantUtil.objectFieldValueOffset(value, 0, i);
            int basicType = VariantUtil.basicType(value, fieldValueOffset);

            DataTypeRoot typeRoot = mapToDataTypeRoot(value, fieldValueOffset, basicType);
            if (typeRoot != null) {
                getOrCreateStats(fieldPath).record(typeRoot);
            }
        }
    }

    /**
     * Maps a Variant value's type information to a Fluss DataTypeRoot.
     *
     * @param value the value bytes
     * @param offset the offset to the value
     * @param basicType the basic type (0-3)
     * @return the corresponding DataTypeRoot, or null if not mappable
     */
    private static DataTypeRoot mapToDataTypeRoot(byte[] value, int offset, int basicType) {
        switch (basicType) {
            case VariantUtil.BASIC_TYPE_PRIMITIVE:
                return mapPrimitiveType(value, offset);
            case VariantUtil.BASIC_TYPE_SHORT_STRING:
                return DataTypeRoot.STRING;
            case VariantUtil.BASIC_TYPE_OBJECT:
                // Nested objects are not shredded in v1
                return null;
            case VariantUtil.BASIC_TYPE_ARRAY:
                // Arrays are not shredded in v1
                return null;
            default:
                return null;
        }
    }

    /**
     * Maps a Variant primitive type to a Fluss DataTypeRoot.
     *
     * <p>All integer sub-types (INT8, INT16, INT32, INT64) are mapped to {@link
     * DataTypeRoot#BIGINT} to avoid false type-inconsistency when a field alternates between small
     * integers (encoded as INT8) and larger values (encoded as INT32 or INT64). This mirrors
     * Paimon's approach of widening all integers to a common type before shredding.
     *
     * @param value the value bytes
     * @param offset the offset to the value
     * @return the corresponding DataTypeRoot, or null for unsupported or null types
     */
    private static DataTypeRoot mapPrimitiveType(byte[] value, int offset) {
        int primitiveTypeId = VariantUtil.primitiveTypeId(value, offset);
        switch (primitiveTypeId) {
            case VariantUtil.PRIMITIVE_TYPE_NULL:
                return null; // null values don't contribute to type inference
            case VariantUtil.PRIMITIVE_TYPE_TRUE:
            case VariantUtil.PRIMITIVE_TYPE_FALSE:
                return DataTypeRoot.BOOLEAN;
            case VariantUtil.PRIMITIVE_TYPE_INT8:
            case VariantUtil.PRIMITIVE_TYPE_INT16:
            case VariantUtil.PRIMITIVE_TYPE_INT32:
            case VariantUtil.PRIMITIVE_TYPE_INT64:
                // Widen all integer sub-types to BIGINT to avoid spurious type-inconsistency.
                // A field that appears as INT8 in some rows and INT16/INT32 in others should still
                // be considered a consistent integer field and shredded as BIGINT.
                return DataTypeRoot.BIGINT;
            case VariantUtil.PRIMITIVE_TYPE_FLOAT:
                return DataTypeRoot.FLOAT;
            case VariantUtil.PRIMITIVE_TYPE_DOUBLE:
                return DataTypeRoot.DOUBLE;
            case VariantUtil.PRIMITIVE_TYPE_STRING:
                return DataTypeRoot.STRING;
            case VariantUtil.PRIMITIVE_TYPE_DATE:
                return DataTypeRoot.DATE;
            case VariantUtil.PRIMITIVE_TYPE_TIMESTAMP:
                return DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            case VariantUtil.PRIMITIVE_TYPE_TIMESTAMP_NTZ:
                return DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
            case VariantUtil.PRIMITIVE_TYPE_DECIMAL4:
            case VariantUtil.PRIMITIVE_TYPE_DECIMAL8:
            case VariantUtil.PRIMITIVE_TYPE_DECIMAL16:
                return DataTypeRoot.DECIMAL;
            case VariantUtil.PRIMITIVE_TYPE_BINARY:
                return DataTypeRoot.BYTES;
            default:
                return null;
        }
    }

    private FieldStatistics getOrCreateStats(String fieldPath) {
        return fieldStats.computeIfAbsent(fieldPath, FieldStatistics::new);
    }
}
