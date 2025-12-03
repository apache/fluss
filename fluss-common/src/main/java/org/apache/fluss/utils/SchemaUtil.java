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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.fluss.row.ProjectedRow.UNEXIST_MAPPING;

/** Schema util. */
public class SchemaUtil {

    /**
     * Get the index mapping from origin schema to expected schema. This mapping can be used to
     * convert the row with origin schema to the row with expected schema via {@code
     * ProjectedRow.from(getIndexMapping(inputSchema, targetSchema)).replace(inputRow)}.
     *
     * @param originSchema The origin schema.
     * @param expectedSchema The expected schema.
     * @return The index mapping array. The length of the array is the number of columns in the
     *     expected schema. Each element in the array is the index of the corresponding column in
     *     the origin schema. If a column in the expected schema does not exist in the origin
     *     schema, the corresponding element is UNEXIST_MAPPING (-1).
     * @throws SchemaChangeException if there is a datatype mismatch between the two schemas.
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
                            originColumn.getDataType().copy(true))) {

                throw new SchemaChangeException(
                        String.format(
                                "Expected datatype of column(id=%s,name=%s) is [%s], while the actual datatype is [%s]",
                                expectedColumn.getColumnId(),
                                expectedColumn.getName(),
                                expectedColumn.getDataType(),
                                originColumn.getDataType()),
                        true);
            }
        }

        return indexMapping;
    }
}
