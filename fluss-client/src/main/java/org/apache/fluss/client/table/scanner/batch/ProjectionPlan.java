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

package org.apache.fluss.client.table.scanner.batch;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

final class ProjectionPlan {
    final int[] scanProjectedFields;
    final int[] keyIndexesInScanRow;
    @Nullable final int[] adjustProjectedFields;

    private ProjectionPlan(
            int[] scanProjectedFields,
            int[] keyIndexesInScanRow,
            @Nullable int[] adjustProjectedFields) {
        this.scanProjectedFields = scanProjectedFields;
        this.keyIndexesInScanRow = keyIndexesInScanRow;
        this.adjustProjectedFields = adjustProjectedFields;
    }

    static ProjectionPlan create(
            int fieldCount, int[] primaryKeyIndexes, @Nullable int[] projectedFields) {
        if (projectedFields == null) {
            return new ProjectionPlan(
                    IntStream.range(0, fieldCount).toArray(), primaryKeyIndexes, null);
        }

        List<Integer> scanProjectedFields =
                Arrays.stream(projectedFields).boxed().collect(Collectors.toList());
        int[] keyIndexesInScanRow = new int[primaryKeyIndexes.length];
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            int primaryKeyIndex = primaryKeyIndexes[i];
            int indexInProjectedFields = findIndex(projectedFields, primaryKeyIndex);
            if (indexInProjectedFields >= 0) {
                keyIndexesInScanRow[i] = indexInProjectedFields;
            } else {
                scanProjectedFields.add(primaryKeyIndex);
                keyIndexesInScanRow[i] = scanProjectedFields.size() - 1;
            }
        }

        int[] scanProjection = scanProjectedFields.stream().mapToInt(Integer::intValue).toArray();
        int[] adjustProjectedFields = new int[projectedFields.length];
        for (int i = 0; i < projectedFields.length; i++) {
            adjustProjectedFields[i] = findIndex(scanProjection, projectedFields[i]);
        }
        return new ProjectionPlan(scanProjection, keyIndexesInScanRow, adjustProjectedFields);
    }

    private static int findIndex(int[] array, int target) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] == target) {
                return i;
            }
        }
        return -1;
    }
}
