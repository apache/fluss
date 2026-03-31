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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Statistics for a single field within Variant records. Tracks how often the field appears and the
 * distribution of data types observed for that field.
 *
 * <p>Used by {@link ShreddingSchemaInferrer} to determine whether a field should be shredded and
 * what type the shredded column should have.
 *
 * @since 0.7
 */
@Internal
public final class FieldStatistics implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The field path (e.g., "age", "address.city"). */
    private final String fieldPath;

    /** Number of records where this field is present. */
    private long presenceCount;

    /** Count of each observed data type for this field. */
    private final Map<DataTypeRoot, Long> typeCounts;

    public FieldStatistics(String fieldPath) {
        this.fieldPath = fieldPath;
        this.presenceCount = 0;
        this.typeCounts = new HashMap<>();
    }

    /** Creates a copy of this statistics for merging. */
    public FieldStatistics(FieldStatistics other) {
        this.fieldPath = other.fieldPath;
        this.presenceCount = other.presenceCount;
        this.typeCounts = new HashMap<>(other.typeCounts);
    }

    /** Returns the field path. */
    public String getFieldPath() {
        return fieldPath;
    }

    /** Returns the number of records where this field is present. */
    public long getPresenceCount() {
        return presenceCount;
    }

    /** Returns the type counts map. */
    public Map<DataTypeRoot, Long> getTypeCounts() {
        return Collections.unmodifiableMap(typeCounts);
    }

    /** Records an observation of this field with the given type. */
    public void record(DataTypeRoot typeRoot) {
        presenceCount++;
        typeCounts.merge(typeRoot, 1L, Long::sum);
    }

    /**
     * Returns the presence ratio of this field relative to total records.
     *
     * @param totalRecords the total number of records
     * @return the presence ratio (0.0 to 1.0)
     */
    public float presenceRatio(long totalRecords) {
        if (totalRecords == 0) {
            return 0.0f;
        }
        return (float) presenceCount / totalRecords;
    }

    /**
     * Returns the dominant type (the type with the highest count).
     *
     * @return the most frequently observed type, or null if no observations
     */
    public DataTypeRoot dominantType() {
        DataTypeRoot dominant = null;
        long maxCount = 0;
        for (Map.Entry<DataTypeRoot, Long> entry : typeCounts.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                dominant = entry.getKey();
            }
        }
        return dominant;
    }

    /**
     * Returns the type consistency ratio, i.e., how consistently this field appears with the same
     * type.
     *
     * @return the ratio of the dominant type count to the total presence count (0.0 to 1.0)
     */
    public float typeConsistency() {
        if (presenceCount == 0) {
            return 0.0f;
        }
        long maxCount = 0;
        for (Long count : typeCounts.values()) {
            if (count > maxCount) {
                maxCount = count;
            }
        }
        return (float) maxCount / presenceCount;
    }

    /**
     * Merges another FieldStatistics into this one.
     *
     * @param other the other statistics to merge
     */
    public void merge(FieldStatistics other) {
        if (!this.fieldPath.equals(other.fieldPath)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot merge statistics for different fields: '%s' vs '%s'",
                            this.fieldPath, other.fieldPath));
        }
        this.presenceCount += other.presenceCount;
        for (Map.Entry<DataTypeRoot, Long> entry : other.typeCounts.entrySet()) {
            this.typeCounts.merge(entry.getKey(), entry.getValue(), Long::sum);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FieldStatistics)) {
            return false;
        }
        FieldStatistics that = (FieldStatistics) o;
        return presenceCount == that.presenceCount
                && Objects.equals(fieldPath, that.fieldPath)
                && Objects.equals(typeCounts, that.typeCounts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldPath, presenceCount, typeCounts);
    }

    @Override
    public String toString() {
        return String.format(
                "FieldStatistics{path='%s', presence=%d, types=%s}",
                fieldPath, presenceCount, typeCounts);
    }
}
