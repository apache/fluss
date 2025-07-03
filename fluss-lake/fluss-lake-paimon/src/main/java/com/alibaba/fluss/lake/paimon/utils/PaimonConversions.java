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

package com.alibaba.fluss.lake.paimon.utils;

import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.util.List;

/** Utils for conversion between Paimon and Fluss. */
public class PaimonConversions {

    public static RowKind toRowKind(ChangeType changeType) {
        switch (changeType) {
            case APPEND_ONLY:
            case INSERT:
                return RowKind.INSERT;
            case UPDATE_BEFORE:
                return RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return RowKind.UPDATE_AFTER;
            case DELETE:
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException("Unsupported change type: " + changeType);
        }
    }

    public static Identifier toPaimon(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    public static BinaryRow toPaimonPartitionBinaryRow(
            List<String> partitionKeys, @Nullable String partitionName) {
        if (partitionName == null || partitionKeys.isEmpty()) {
            return BinaryRow.EMPTY_ROW;
        }

        //  Fluss's existing utility
        ResolvedPartitionSpec resolvedPartitionSpec =
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);

        BinaryRow partitionBinaryRow = new BinaryRow(partitionKeys.size());
        BinaryRowWriter writer = new BinaryRowWriter(partitionBinaryRow);

        List<String> partitionValues = resolvedPartitionSpec.getPartitionValues();
        for (int i = 0; i < partitionKeys.size(); i++) {
            // Todo Currently, partition column must be String datatype, so we can always use
            // `BinaryString.fromString` to convert to Paimon's data structure. Revisit here when
            // #489 is finished.
            writer.writeString(i, BinaryString.fromString(partitionValues.get(i)));
        }

        writer.complete();
        return partitionBinaryRow;
    }
}
