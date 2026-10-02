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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.metadata.BucketInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Procedure to describe bucket metadata for a table.
 *
 * <p>Usage examples:
 *
 * <pre>
 * CALL sys.describe_buckets('db.table');
 * CALL sys.describe_buckets('db.table', 'partition_key=partition_value');
 * </pre>
 */
public class DescribeBucketsProcedure extends ProcedureBase {

    private static final String OUTPUT_TYPE =
            "ROW<table_path STRING, table_id BIGINT, partition_id BIGINT, "
                    + "partition_name STRING, bucket_id INT, leader_id INT, leader_epoch INT, "
                    + "bucket_epoch INT, replicas ARRAY<INT>, isr ARRAY<INT>>";

    @ProcedureHint(
            argument = {@ArgumentHint(name = "table_path", type = @DataTypeHint("STRING"))},
            output = @DataTypeHint(OUTPUT_TYPE))
    public Row[] call(ProcedureContext context, String tablePath) throws Exception {
        TablePath parsedTablePath = parseTablePath(tablePath);
        return toRows(admin.describeBuckets(parsedTablePath).get());
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table_path", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "partition_spec", type = @DataTypeHint("STRING"))
            },
            output = @DataTypeHint(OUTPUT_TYPE))
    public Row[] call(ProcedureContext context, String tablePath, String partitionSpec)
            throws Exception {
        TablePath parsedTablePath = parseTablePath(tablePath);
        return toRows(
                admin.describeBuckets(parsedTablePath, parsePartitionSpec(partitionSpec)).get());
    }

    private static Row[] toRows(List<BucketInfo> bucketInfos) {
        return bucketInfos.stream().map(DescribeBucketsProcedure::toRow).toArray(Row[]::new);
    }

    private static Row toRow(BucketInfo bucketInfo) {
        return Row.of(
                bucketInfo.getTablePath().toString(),
                bucketInfo.getTableId(),
                optionalLong(bucketInfo.getPartitionId()),
                bucketInfo.getPartitionName(),
                bucketInfo.getBucketId(),
                optionalInt(bucketInfo.getLeaderId()),
                optionalInt(bucketInfo.getLeaderEpoch()),
                optionalInt(bucketInfo.getBucketEpoch()),
                bucketInfo.getReplicas().toArray(new Integer[0]),
                bucketInfo.getIsr().toArray(new Integer[0]));
    }

    private static Long optionalLong(OptionalLong value) {
        return value.isPresent() ? value.getAsLong() : null;
    }

    private static Integer optionalInt(OptionalInt value) {
        return value.isPresent() ? value.getAsInt() : null;
    }

    private static TablePath parseTablePath(String tablePath) {
        if (tablePath == null || tablePath.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "table_path cannot be null or empty. Expected format is 'database.table'.");
        }

        String normalizedTablePath = tablePath.trim();
        String[] parts = normalizedTablePath.split("\\.", -1);
        if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid table_path '" + tablePath + "'. Expected format is 'database.table'.");
        }
        TablePath parsedTablePath = TablePath.of(parts[0], parts[1]);
        parsedTablePath.validate();
        return parsedTablePath;
    }

    private static PartitionSpec parsePartitionSpec(String partitionSpec) {
        if (partitionSpec == null || partitionSpec.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "partition_spec cannot be null or empty. Expected format is "
                            + "'key=value[/key=value...]'.");
        }

        String normalizedPartitionSpec = partitionSpec.trim();
        ResolvedPartitionSpec resolvedPartitionSpec;
        try {
            String[] keyValuePairs = normalizedPartitionSpec.split("/", -1);
            for (String keyValuePair : keyValuePairs) {
                if (keyValuePair.isEmpty()) {
                    throw new IllegalArgumentException("Empty partition key-value pair.");
                }
            }
            resolvedPartitionSpec =
                    ResolvedPartitionSpec.fromPartitionQualifiedName(normalizedPartitionSpec);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid partition_spec '"
                            + partitionSpec
                            + "'. Expected format is 'key=value[/key=value...]'.",
                    e);
        }

        Map<String, String> spec = new LinkedHashMap<>();
        List<String> partitionKeys = resolvedPartitionSpec.getPartitionKeys();
        List<String> partitionValues = resolvedPartitionSpec.getPartitionValues();
        for (int i = 0; i < partitionKeys.size(); i++) {
            String partitionKey = partitionKeys.get(i);
            if (partitionKey.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Invalid partition_spec '"
                                + partitionSpec
                                + "': partition key cannot be empty.");
            }
            if (spec.containsKey(partitionKey)) {
                throw new IllegalArgumentException(
                        "Duplicate partition key '"
                                + partitionKey
                                + "' in partition_spec '"
                                + partitionSpec
                                + "'.");
            }
            spec.put(partitionKey, partitionValues.get(i));
        }
        return new PartitionSpec(spec);
    }
}
