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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.exception.InvalidPartitionException;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataTypeRoot;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.metadata.TablePath.detectInvalidName;

/** Utils for partition. */
public class PartitionUtils {

    // TODO Support other data types, trace by https://github.com/alibaba/fluss/issues/489
    public static final List<DataTypeRoot> PARTITION_KEY_SUPPORTED_TYPES =
            Collections.singletonList(DataTypeRoot.STRING);

    private static final String YEAR_FORMAT = "yyyy";
    private static final String QUARTER_FORMAT = "yyyyQ";
    private static final String MONTH_FORMAT = "yyyyMM";
    private static final String DAY_FORMAT = "yyyyMMdd";
    private static final String HOUR_FORMAT = "yyyyMMddHH";

    public static void validatePartitionSpec(
            TablePath tablePath, List<String> partitionKeys, PartitionSpec partitionSpec) {
        Map<String, String> partitionSpecMap = partitionSpec.getSpecMap();
        if (partitionKeys.size() != partitionSpecMap.size()) {
            throw new InvalidPartitionException(
                    String.format(
                            "PartitionSpec size is not equal to partition keys size for partitioned table %s.",
                            tablePath));
        }

        List<String> reOrderedPartitionValues = new ArrayList<>(partitionKeys.size());
        for (String partitionKey : partitionKeys) {
            if (!partitionSpecMap.containsKey(partitionKey)) {
                throw new InvalidPartitionException(
                        String.format(
                                "PartitionSpec %s does not contain partition key '%s' for partitioned table %s.",
                                partitionSpec, partitionKey, tablePath));
            } else {
                reOrderedPartitionValues.add(partitionSpecMap.get(partitionKey));
            }
        }

        validatePartitionValues(reOrderedPartitionValues);
    }

    @VisibleForTesting
    static void validatePartitionValues(List<String> partitionValues) {
        for (String value : partitionValues) {
            String invalidName = detectInvalidName(value);
            if (invalidName != null) {
                throw new InvalidPartitionException(
                        "The partition value " + value + " is invalid: " + invalidName);
            }
        }
    }

    /**
     * Generate {@link ResolvedPartitionSpec} for auto partition in server. When we auto creating a
     * partition, we need to first generate a {@link ResolvedPartitionSpec}.
     *
     * <p>The value is the formatted time with the specified time unit.
     *
     * @param partitionKeys the partition keys
     * @param current the current time
     * @param offset the offset
     * @param timeUnit the time unit
     * @return the resolved partition spec
     */
    public static ResolvedPartitionSpec generateAutoPartition(
            List<String> partitionKeys,
            ZonedDateTime current,
            int offset,
            AutoPartitionTimeUnit timeUnit) {
        String autoPartitionFieldSpec = generateAutoPartitionTime(current, offset, timeUnit);

        return ResolvedPartitionSpec.fromPartitionName(partitionKeys, autoPartitionFieldSpec);
    }

    public static String generateAutoPartitionTime(
            ZonedDateTime current, int offset, AutoPartitionTimeUnit timeUnit) {
        String autoPartitionFieldSpec;
        switch (timeUnit) {
            case YEAR:
                autoPartitionFieldSpec = getFormattedTime(current.plusYears(offset), YEAR_FORMAT);
                break;
            case QUARTER:
                autoPartitionFieldSpec =
                        getFormattedTime(current.plusMonths(offset * 3L), QUARTER_FORMAT);
                break;
            case MONTH:
                autoPartitionFieldSpec = getFormattedTime(current.plusMonths(offset), MONTH_FORMAT);
                break;
            case DAY:
                autoPartitionFieldSpec = getFormattedTime(current.plusDays(offset), DAY_FORMAT);
                break;
            case HOUR:
                autoPartitionFieldSpec = getFormattedTime(current.plusHours(offset), HOUR_FORMAT);
                break;
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
        }
        return autoPartitionFieldSpec;
    }

    private static String getFormattedTime(ZonedDateTime zonedDateTime, String format) {
        return DateTimeFormatter.ofPattern(format).format(zonedDateTime);
    }
}
