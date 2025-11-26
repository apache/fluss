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

package org.apache.fluss.utils;

import org.apache.fluss.exception.SchemaChangeException;
import org.apache.fluss.metadata.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.fluss.row.ProjectedRow.UNEXIST_MAPPING;

/** Schema util. */
public class SchemaUtil {

    /**
     * get the target columns in expected schema.
     *
     * @param targetColumns
     * @param originSchema
     * @param expectedSchema
     * @return
     */
    public static int[] getTargetColumns(
            int[] targetColumns, Schema originSchema, Schema expectedSchema) {
        if (targetColumns == null) {
            return null;
        }

        // get the mapping from expected schema to origin schema
        int[] indexMapping = getIndexMapping(expectedSchema, originSchema);
        ArrayList<Integer> targetColumnsInExpectedSchema = new ArrayList<>();
        for (int i = 0; i < targetColumns.length; i++) {
            if (indexMapping[targetColumns[i]] != UNEXIST_MAPPING) {
                targetColumnsInExpectedSchema.add(indexMapping[targetColumns[i]]);
            }
        }
        return targetColumnsInExpectedSchema.stream().mapToInt(value -> value).toArray();
    }

    /**
     * Get the index of each value of expectedSchema from originSchema.
     *
     * @param originSchema
     * @param expectedSchema
     * @return indexMapping the value of i-th means the expectedSchema's i-th column is the
     *     originSchema's indexMapping[i]-th column.
     */
    public static int[] getIndexMapping(Schema originSchema, Schema expectedSchema) {
        List<Schema.Column> originColumns = originSchema.getColumns();
        Map<Integer, Integer> originColumnIdToIndex = new HashMap<>();
        for (int i = 0; i < originColumns.size(); i++) {
            originColumnIdToIndex.put(originColumns.get(i).getColumnId(), i);
        }
        List<Schema.Column> expectedColumns = expectedSchema.getColumns();

        int[] indexMapping = new int[expectedColumns.size()];
        for (int i = 0; i < expectedColumns.size(); i++) {
            Schema.Column expectedColumn = expectedColumns.get(i);
            indexMapping[i] =
                    originColumnIdToIndex.getOrDefault(
                            expectedColumn.getColumnId(), UNEXIST_MAPPING);
            Schema.Column originColumn =
                    indexMapping[i] == UNEXIST_MAPPING ? null : originColumns.get(indexMapping[i]);

            if (originColumn != null
                    && !Objects.equals(
                            expectedColumn.getDataType().copy(true),
                            originColumns.get(indexMapping[i]).getDataType().copy(true))) {

                throw new SchemaChangeException(
                        String.format(
                                "Expected datatype of column(id=%s,name=%s) is [%s], while the actual datatype is [%s]",
                                expectedColumn.getColumnId(),
                                expectedColumn.getName(),
                                expectedColumn.getDataType(),
                                originColumn.getDataType()));
            }
        }

        return indexMapping;
    }
}
