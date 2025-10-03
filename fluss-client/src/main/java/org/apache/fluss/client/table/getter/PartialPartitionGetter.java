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

package org.apache.fluss.client.table.getter;

import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A partial partition getter that can extract available partition values from lookup key. */
public class PartialPartitionGetter {
    private final List<String> availablePartitionKeys;
    private final List<InternalRow.FieldGetter> partitionFieldGetters;

    public PartialPartitionGetter(RowType rowType, List<String> availablePartitionKeys) {
        this.availablePartitionKeys = availablePartitionKeys;
        List<String> fieldNames = rowType.getFieldNames();
        this.partitionFieldGetters = new ArrayList<>();
        for (String partitionKey : availablePartitionKeys) {
            int partitionColumnIndex = fieldNames.indexOf(partitionKey);
            if (partitionColumnIndex >= 0) {
                DataType partitionColumnDataType = rowType.getTypeAt(partitionColumnIndex);
                partitionFieldGetters.add(
                        InternalRow.createFieldGetter(
                                partitionColumnDataType, partitionColumnIndex));
            }
        }
    }

    public Map<String, String> getPartialPartitionSpec(InternalRow row) {
        Map<String, String> partitionSpec = new HashMap<>();
        for (int i = 0;
                i < availablePartitionKeys.size() && i < partitionFieldGetters.size();
                i++) {
            Object partitionValue = partitionFieldGetters.get(i).getFieldOrNull(row);
            if (partitionValue != null) {
                partitionSpec.put(availablePartitionKeys.get(i), partitionValue.toString());
            }
        }
        return partitionSpec;
    }
}
