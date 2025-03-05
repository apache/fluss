/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/**
 * Represents a partition, which is the resolved version of {@link PartitionSpec}. The partition
 * spec is re-arranged into the correct order by comparing it with a list of strictly ordered
 * partition keys.
 *
 * @since 0.6
 */
@PublicEvolving
public class ResolvedPartitionSpec {
    public static final String PARTITION_SPEC_SEPARATOR = "$";

    private final List<String> partitionKeys;
    private final List<String> partitionValues;

    private ResolvedPartitionSpec(List<String> partitionKeys, List<String> partitionValues) {
        checkArgument(
                partitionKeys.size() == partitionValues.size(),
                "The number of partition keys and partition values should be the same.");
        this.partitionKeys = partitionKeys;
        this.partitionValues = partitionValues;
    }

    public static ResolvedPartitionSpec fromPartitionSpec(
            List<String> partitionKeys, PartitionSpec partitionSpec) {
        return new ResolvedPartitionSpec(
                partitionKeys, getReorderedPartitionValues(partitionKeys, partitionSpec));
    }

    public static ResolvedPartitionSpec fromPartitionValue(
            String partitionKey, String partitionValue) {
        return new ResolvedPartitionSpec(
                Collections.singletonList(partitionKey), Collections.singletonList(partitionValue));
    }

    public static ResolvedPartitionSpec fromPartitionName(
            List<String> partitionKeys, String partitionName) {
        return new ResolvedPartitionSpec(partitionKeys, Arrays.asList(partitionName.split("\\$")));
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }

    /**
     * Generate the partition name for a partition table of specify partition values.
     *
     * <p>The partition name is in the following format:
     *
     * <pre>
     * value1$value2$...$valueN
     * </pre>
     *
     * <p>For example, if the partition keys are [a, b, c], and the partition values are [1, 2, 3],
     * the partition name is "1$2$3".
     *
     * <p>Currently, we only support one partition key. So the partition name is in the following
     * format:
     *
     * <pre>
     * value
     * </pre>
     *
     * <p>For example, if the partition keys are [a], and the partition value is [1], the partition
     * name will be "1".
     */
    public String getPartitionName() {
        return String.join(PARTITION_SPEC_SEPARATOR, partitionValues);
    }

    /**
     * Returns the qualified partition name for a partition spec (partition keys and partition
     * values). The qualified partition name is not used as the partition directory path, but is
     * used as a pretty display name of a partition. The qualified partition name is in the
     * following format:
     *
     * <pre>
     * key1=value1/key2=value2/.../keyN=valueN
     * </pre>
     */
    public String getPartitionQualifiedName() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionKeys.size(); i++) {
            sb.append(partitionKeys.get(i)).append("=").append(partitionValues.get(i));
            if (i != partitionKeys.size() - 1) {
                sb.append("/");
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return getPartitionQualifiedName();
    }

    private static List<String> getReorderedPartitionValues(
            List<String> partitionKeys, PartitionSpec partitionSpec) {
        Map<String, String> partitionSpecMap = partitionSpec.getPartitionSpec();
        List<String> reOrderedPartitionValues = new ArrayList<>(partitionKeys.size());
        partitionKeys.forEach(
                partitionKey -> reOrderedPartitionValues.add(partitionSpecMap.get(partitionKey)));
        return reOrderedPartitionValues;
    }
}
