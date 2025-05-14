/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table.getter;

import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** A getter to get partition name from a row. */
public class PartitionGetter {

    private final List<String> partitionKeys;
    private final List<InternalRow.FieldGetter> partitionFieldGetters;

    public PartitionGetter(RowType rowType, List<String> partitionKeys) {
        // check the partition column
        List<String> fieldNames = rowType.getFieldNames();
        this.partitionKeys = partitionKeys;
        partitionFieldGetters = new ArrayList<>();
        for (String partitionKey : partitionKeys) {
            int partitionColumnIndex = fieldNames.indexOf(partitionKey);
            checkArgument(
                    partitionColumnIndex >= 0,
                    "The partition column %s is not in the row %s.",
                    partitionKey,
                    rowType);

            // check the data type of the partition column
            DataType partitionColumnDataType = rowType.getTypeAt(partitionColumnIndex);
            partitionFieldGetters.add(
                    InternalRow.createFieldGetter(partitionColumnDataType, partitionColumnIndex));
        }
    }

    public String getPartition(InternalRow row) {
        List<String> partitionValues = new ArrayList<>();
        for (InternalRow.FieldGetter partitionFieldGetter : partitionFieldGetters) {
            Object partitionValue = partitionFieldGetter.getFieldOrNull(row);
            checkNotNull(partitionValue, "Partition value shouldn't be null.");
            partitionValues.add(partitionValue.toString());
        }
        ResolvedPartitionSpec resolvedPartitionSpec =
                new ResolvedPartitionSpec(partitionKeys, partitionValues);
        return resolvedPartitionSpec.getPartitionName();
    }
}
