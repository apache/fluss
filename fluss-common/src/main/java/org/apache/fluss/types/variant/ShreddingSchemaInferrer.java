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
import org.apache.fluss.types.DataTypes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Infers a {@link ShreddingSchema} from field statistics collected by {@link
 * VariantStatisticsCollector}. The inference algorithm selects fields that appear frequently with
 * consistent types and creates typed columns for them.
 *
 * <h3>Algorithm</h3>
 *
 * <p>For each field in the statistics:
 *
 * <ol>
 *   <li>Compute {@code presence_ratio = presence_count / total_records}
 *   <li>Compute {@code dominant_type = argmax(type_counts)}
 *   <li>Compute {@code type_consistency = type_counts[dominant_type] / presence_count}
 *   <li>If {@code presence_ratio >= PRESENCE_THRESHOLD} AND {@code type_consistency >=
 *       TYPE_CONSISTENCY_THRESHOLD} AND {@code dominant_type} is shreddable: mark this field for
 *       shredding
 * </ol>
 *
 * <p>Fields are ranked by {@code presence_ratio * type_consistency} and limited to {@code
 * maxShreddedFields}.
 *
 * @since 0.7
 */
@Internal
public class ShreddingSchemaInferrer implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Default presence threshold: field must appear in >= 50% of records. */
    public static final float DEFAULT_PRESENCE_THRESHOLD = 0.5f;

    /** Default type consistency threshold: >= 90% of occurrences must have the same type. */
    public static final float DEFAULT_TYPE_CONSISTENCY_THRESHOLD = 0.9f;

    /** Default maximum number of shredded fields per Variant column. */
    public static final int DEFAULT_MAX_SHREDDED_FIELDS = 100;

    /** Default minimum number of records required before inference can run. */
    public static final int DEFAULT_MIN_SAMPLE_SIZE = 1000;

    /**
     * Types that can be shredded into independent typed columns.
     *
     * <p>All integer sub-types (TINYINT, SMALLINT, INTEGER, BIGINT) are included. When statistics
     * are collected via {@link VariantStatisticsCollector}, all Variant integer encodings (INT8,
     * INT16, INT32, INT64) are already widened to {@link DataTypeRoot#BIGINT} before being
     * recorded, so only BIGINT will appear in practice. The smaller types remain here to support
     * manually constructed {@link FieldStatistics} (e.g., in tests or external integrations).
     */
    private static final Set<DataTypeRoot> SHREDDABLE_TYPES =
            EnumSet.of(
                    DataTypeRoot.BOOLEAN,
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.BIGINT,
                    DataTypeRoot.FLOAT,
                    DataTypeRoot.DOUBLE,
                    DataTypeRoot.STRING,
                    DataTypeRoot.DATE,
                    DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

    private float presenceThreshold;
    private float typeConsistencyThreshold;
    private int maxShreddedFields;
    private int minSampleSize;

    public ShreddingSchemaInferrer() {
        this.presenceThreshold = DEFAULT_PRESENCE_THRESHOLD;
        this.typeConsistencyThreshold = DEFAULT_TYPE_CONSISTENCY_THRESHOLD;
        this.maxShreddedFields = DEFAULT_MAX_SHREDDED_FIELDS;
        this.minSampleSize = DEFAULT_MIN_SAMPLE_SIZE;
    }

    public ShreddingSchemaInferrer setPresenceThreshold(float presenceThreshold) {
        this.presenceThreshold = presenceThreshold;
        return this;
    }

    public ShreddingSchemaInferrer setTypeConsistencyThreshold(float typeConsistencyThreshold) {
        this.typeConsistencyThreshold = typeConsistencyThreshold;
        return this;
    }

    public ShreddingSchemaInferrer setMaxShreddedFields(int maxShreddedFields) {
        this.maxShreddedFields = maxShreddedFields;
        return this;
    }

    public ShreddingSchemaInferrer setMinSampleSize(int minSampleSize) {
        this.minSampleSize = minSampleSize;
        return this;
    }

    public float getPresenceThreshold() {
        return presenceThreshold;
    }

    public float getTypeConsistencyThreshold() {
        return typeConsistencyThreshold;
    }

    public int getMaxShreddedFields() {
        return maxShreddedFields;
    }

    public int getMinSampleSize() {
        return minSampleSize;
    }

    /**
     * Infers a shredding schema based on collected field statistics.
     *
     * @param variantColumnName the name of the Variant column
     * @param stats the field statistics map
     * @param totalRecords the total number of records analyzed
     * @return the inferred ShreddingSchema
     */
    public ShreddingSchema infer(
            String variantColumnName, Map<String, FieldStatistics> stats, long totalRecords) {
        if (totalRecords < minSampleSize) {
            // Not enough data to infer reliably
            return new ShreddingSchema(variantColumnName, new ArrayList<>());
        }

        List<CandidateField> candidates = new ArrayList<>();
        for (FieldStatistics fieldStats : stats.values()) {
            float presenceRatio = fieldStats.presenceRatio(totalRecords);
            float consistency = fieldStats.typeConsistency();
            DataTypeRoot dominantType = fieldStats.dominantType();

            if (presenceRatio >= presenceThreshold
                    && consistency >= typeConsistencyThreshold
                    && dominantType != null
                    && SHREDDABLE_TYPES.contains(dominantType)) {
                candidates.add(
                        new CandidateField(
                                fieldStats.getFieldPath(),
                                dominantType,
                                presenceRatio * consistency));
            }
        }

        // Sort by score descending, take top-N
        candidates.sort(Comparator.comparingDouble(c -> -c.score));
        if (candidates.size() > maxShreddedFields) {
            candidates = candidates.subList(0, maxShreddedFields);
        }

        // Build the shredded fields (columnId will be assigned by the schema evolution process)
        List<ShreddedField> fields = new ArrayList<>();
        for (int i = 0; i < candidates.size(); i++) {
            CandidateField candidate = candidates.get(i);
            DataType dataType = mapToDataType(candidate.typeRoot);
            // Use placeholder column ID (-1); actual ID assigned during schema evolution
            fields.add(new ShreddedField(candidate.fieldPath, dataType, -1));
        }

        return new ShreddingSchema(variantColumnName, fields);
    }

    /**
     * Incrementally updates a shredding schema based on new statistics. Determines if changes are
     * needed by comparing new statistics against the existing schema.
     *
     * @param current the current shredding schema
     * @param newStats the new field statistics
     * @param totalRecords the total number of records in the new statistics
     * @return an updated ShreddingSchema if changes are needed, or empty if no changes required
     */
    public Optional<ShreddingSchema> update(
            ShreddingSchema current, Map<String, FieldStatistics> newStats, long totalRecords) {
        if (totalRecords < minSampleSize) {
            return Optional.empty();
        }

        // Get currently shredded field paths
        Set<String> currentPaths = new HashSet<>();
        for (ShreddedField field : current.getFields()) {
            currentPaths.add(field.getFieldPath());
        }

        // Check if any existing field should be demoted (type changed or presence dropped)
        boolean hasChanges = false;
        List<ShreddedField> updatedFields = new ArrayList<>();
        for (ShreddedField existing : current.getFields()) {
            FieldStatistics stats = newStats.get(existing.getFieldPath());
            if (stats != null) {
                float presenceRatio = stats.presenceRatio(totalRecords);
                float consistency = stats.typeConsistency();
                DataTypeRoot dominantType = stats.dominantType();

                if (presenceRatio >= presenceThreshold
                        && consistency >= typeConsistencyThreshold
                        && dominantType != null
                        && SHREDDABLE_TYPES.contains(dominantType)
                        && mapToDataType(dominantType)
                                .equals(existing.getShreddedType().copy(true))) {
                    // Field still qualifies and type hasn't changed - keep it
                    updatedFields.add(existing);
                } else {
                    // Field no longer qualifies or type changed - demote
                    hasChanges = true;
                }
            } else {
                // Field not in new stats - demote
                hasChanges = true;
            }
        }

        // Check for new fields that should be promoted
        List<CandidateField> newCandidates = new ArrayList<>();
        for (FieldStatistics stats : newStats.values()) {
            if (currentPaths.contains(stats.getFieldPath())) {
                continue; // Already handled
            }
            float presenceRatio = stats.presenceRatio(totalRecords);
            float consistency = stats.typeConsistency();
            DataTypeRoot dominantType = stats.dominantType();

            if (presenceRatio >= presenceThreshold
                    && consistency >= typeConsistencyThreshold
                    && dominantType != null
                    && SHREDDABLE_TYPES.contains(dominantType)) {
                newCandidates.add(
                        new CandidateField(
                                stats.getFieldPath(), dominantType, presenceRatio * consistency));
            }
        }

        if (!newCandidates.isEmpty()) {
            hasChanges = true;
            // Sort new candidates by score descending
            newCandidates.sort(Comparator.comparingDouble(c -> -c.score));
            int remaining = maxShreddedFields - updatedFields.size();
            for (int i = 0; i < Math.min(newCandidates.size(), remaining); i++) {
                CandidateField candidate = newCandidates.get(i);
                DataType dataType = mapToDataType(candidate.typeRoot);
                updatedFields.add(new ShreddedField(candidate.fieldPath, dataType, -1));
            }
        }

        if (!hasChanges) {
            return Optional.empty();
        }

        return Optional.of(new ShreddingSchema(current.getVariantColumnName(), updatedFields));
    }

    // --------------------------------------------------------------------------------------------
    // Internal helpers
    // --------------------------------------------------------------------------------------------

    /**
     * Maps a DataTypeRoot to the corresponding nullable DataType for shredded columns.
     *
     * <p>When statistics are collected via {@link VariantStatisticsCollector}, all Variant integer
     * encodings are widened to {@link DataTypeRoot#BIGINT}. The smaller integer types are still
     * handled here to support manually constructed {@link FieldStatistics}.
     */
    private static DataType mapToDataType(DataTypeRoot typeRoot) {
        switch (typeRoot) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                return DataTypes.TINYINT();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INTEGER:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case STRING:
                return DataTypes.STRING();
            case DATE:
                return DataTypes.DATE();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return DataTypes.TIMESTAMP(6);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return DataTypes.TIMESTAMP_LTZ(6);
            default:
                throw new IllegalArgumentException("Unsupported shreddable type: " + typeRoot);
        }
    }

    /** Internal candidate field for ranking. */
    private static class CandidateField {
        final String fieldPath;
        final DataTypeRoot typeRoot;
        final double score;

        CandidateField(String fieldPath, DataTypeRoot typeRoot, double score) {
            this.fieldPath = fieldPath;
            this.typeRoot = typeRoot;
            this.score = score;
        }
    }
}
