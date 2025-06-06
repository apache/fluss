/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.utils;

import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    // NEW METHOD: Handle multiple partitions
    public static BinaryRow toPaimonPartitionBinaryRow(
            List<String> partitionKeys, @Nullable String partitionName) {
        if (partitionName == null || partitionKeys.isEmpty()) {
            return BinaryRow.EMPTY_ROW;
        }

        // Parse partition specification manually
        Map<String, String> partitionSpec = parsePartitionName(partitionKeys, partitionName);

        BinaryRow partitionBinaryRow = new BinaryRow(partitionKeys.size());
        BinaryRowWriter writer = new BinaryRowWriter(partitionBinaryRow);

        // Convert each partition field
        for (int i = 0; i < partitionKeys.size(); i++) {
            String partitionKey = partitionKeys.get(i);
            String partitionValue = partitionSpec.get(partitionKey);

            if (partitionValue == null) {
                writer.setNullAt(i);
            } else {
                writer.writeString(i, BinaryString.fromString(partitionValue));
            }
        }

        writer.complete();
        return partitionBinaryRow;
    }

    // Helper method to parse partition name manually
    private static Map<String, String> parsePartitionName(
            List<String> partitionKeys, String partitionName) {
        Map<String, String> partitionSpec = new LinkedHashMap<>();

        if (partitionKeys.size() == 1) {
            // Simple case: single partition field, value is the entire string
            partitionSpec.put(partitionKeys.get(0), partitionName);
        } else {
            // Multi-partition case: parse "key1=value1/key2=value2/..."
            String[] parts = partitionName.split("/");
            for (String part : parts) {
                String[] keyValue = part.split("=", 2);
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();
                    partitionSpec.put(key, value);
                } else {
                    throw new IllegalArgumentException(
                            "Invalid partition part: " + part + " in partition: " + partitionName);
                }
            }

            // Validate that all required partition keys are present
            for (String partitionKey : partitionKeys) {
                if (!partitionSpec.containsKey(partitionKey)) {
                    throw new IllegalArgumentException(
                            "Missing partition key: "
                                    + partitionKey
                                    + " in partition: "
                                    + partitionName);
                }
            }
        }

        return partitionSpec;
    }

    // Keeping old method for backward compatibility
    @Deprecated
    public static BinaryRow toPaimonBinaryRow(@Nullable String value) {
        return toPaimonPartitionBinaryRow(Arrays.asList("partition"), value);
    }
}
