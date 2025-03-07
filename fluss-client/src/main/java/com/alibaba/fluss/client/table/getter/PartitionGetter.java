/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

import java.util.List;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** A getter to get partition name from a row. */
public class PartitionGetter {

    // TODO currently, only support one partition key.
    private final String partitionKey;
    private final InternalRow.FieldGetter partitionFieldGetter;

    public PartitionGetter(RowType rowType, List<String> partitionKeys) {
        if (partitionKeys.size() != 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Currently, partitioned table only supports one partition key, but got partition keys %s.",
                            partitionKeys));
        }

        // check the partition column
        List<String> fieldNames = rowType.getFieldNames();
        this.partitionKey = partitionKeys.get(0);
        int partitionColumnIndex = fieldNames.indexOf(partitionKey);
        checkArgument(
                partitionColumnIndex >= 0,
                "The partition column %s is not in the row %s.",
                partitionKey,
                rowType);

        // check the data type of the partition column
        DataType partitionColumnDataType = rowType.getTypeAt(partitionColumnIndex);
        this.partitionFieldGetter =
                InternalRow.createFieldGetter(partitionColumnDataType, partitionColumnIndex);
    }

    public String getPartition(InternalRow row) {
        Object partitionValue = partitionFieldGetter.getFieldOrNull(row);
        checkNotNull(partitionValue, "Partition value shouldn't be null.");
        ResolvedPartitionSpec resolvedPartitionSpec =
                ResolvedPartitionSpec.fromPartitionValue(partitionKey, partitionValue.toString());
        return resolvedPartitionSpec.getPartitionName();
    }
}
