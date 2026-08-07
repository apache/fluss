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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Converts Fluss lookup results to projected and filtered Flink rows. */
final class LookupResultConverter {

    private final int[] projection;
    private final FlussRowToFlinkRowConverter rowConverter;

    LookupResultConverter(RowType outputRowType, int[] projection) {
        this.projection = projection;
        this.rowConverter = new FlussRowToFlinkRowConverter(outputRowType);
    }

    Collection<RowData> convert(
            @Nullable List<InternalRow> lookupRows,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter) {
        if (lookupRows == null || lookupRows.isEmpty()) {
            return Collections.emptyList();
        }

        List<RowData> projectedRows = new ArrayList<>(lookupRows.size());
        // ProjectedRow must not be shared between concurrent async lookup requests.
        ProjectedRow projectedRow = ProjectedRow.from(projection);
        for (InternalRow row : lookupRows) {
            if (row != null) {
                RowData flinkRow = rowConverter.toFlinkRowData(projectedRow.replaceRow(row));
                if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                    projectedRows.add(flinkRow);
                }
            }
        }
        return projectedRows;
    }
}
