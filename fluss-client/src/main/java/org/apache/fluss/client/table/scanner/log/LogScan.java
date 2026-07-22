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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.table.scanner.ProjectionParser.ProjectedField;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Used to describe the operation to scan log data by {@link LogScanner} to a table.
 *
 * @since 0.1
 */
@PublicEvolving
public class LogScan {

    /** The projected fields to do projection. No projection if is null. */
    @Nullable private final int[] projectedFields;

    /**
     * Variant sub-field projection hints. Maps top-level table column index to the list of
     * top-level Variant field names that should be projected. Null means no sub-field projection.
     */
    @Nullable private final Map<Integer, List<String>> variantFieldProjection;

    /**
     * The flat (logical) projected schema produced by {@link
     * org.apache.fluss.client.table.scanner.ProjectionParser}. When non-null, the rows surfaced by
     * the scanner are flattened so typed Variant sub-fields appear as top-level scalar columns.
     */
    @Nullable private final List<ProjectedField> projectedSubFields;

    public LogScan() {
        this(null, null, null);
    }

    private LogScan(
            @Nullable int[] projectedFields,
            @Nullable Map<Integer, List<String>> variantFieldProjection,
            @Nullable List<ProjectedField> projectedSubFields) {
        this.projectedFields = projectedFields;
        this.variantFieldProjection = variantFieldProjection;
        this.projectedSubFields = projectedSubFields;
    }

    /**
     * Returns a new instance of LogScan description with column projection.
     *
     * @param projectedFields the projection fields
     */
    public LogScan withProjectedFields(int[] projectedFields) {
        return new LogScan(projectedFields, variantFieldProjection, projectedSubFields);
    }

    /**
     * Returns a new instance of LogScan description with variant sub-field projection hints.
     *
     * @param variantFieldProjection mapping from Variant column index to desired field names
     */
    public LogScan withVariantFieldProjection(
            @Nullable Map<Integer, List<String>> variantFieldProjection) {
        return new LogScan(projectedFields, variantFieldProjection, projectedSubFields);
    }

    /**
     * Returns a new instance of LogScan description with the parsed flat projected sub-field
     * descriptors.
     */
    public LogScan withProjectedSubFields(@Nullable List<ProjectedField> projectedSubFields) {
        return new LogScan(projectedFields, variantFieldProjection, projectedSubFields);
    }

    @Nullable
    public int[] getProjectedFields() {
        return projectedFields;
    }

    @Nullable
    public Map<Integer, List<String>> getVariantFieldProjection() {
        return variantFieldProjection;
    }

    @Nullable
    public List<ProjectedField> getProjectedSubFields() {
        return projectedSubFields;
    }
}
