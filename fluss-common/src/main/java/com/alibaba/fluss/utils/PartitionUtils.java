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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.exception.PartitionSpecInvalidException;
import com.alibaba.fluss.types.DataTypeRoot;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/** Utils for partition. */
public class PartitionUtils {
    public static final String PARTITION_SPEC_SEPARATOR = "$";

    public static final List<DataTypeRoot> STATIC_PARTITION_KEY_SUPPORTED_TYPES =
            Arrays.asList(
                    DataTypeRoot.STRING,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.BOOLEAN,
                    DataTypeRoot.DATE,
                    DataTypeRoot.TIME_WITHOUT_TIME_ZONE);

    public static final List<DataTypeRoot> AUTO_PARTITION_KEY_SUPPORTED_TYPES =
            Collections.singletonList(DataTypeRoot.STRING);

    private static final String YEAR_FORMAT = "yyyy";
    private static final String QUARTER_FORMAT = "yyyyQ";
    private static final String MONTH_FORMAT = "yyyyMM";
    private static final String DAY_FORMAT = "yyyyMMdd";
    private static final String HOUR_FORMAT = "yyyyMMddHH";

    /**
     * Returns the partition name for a partition table of specify partition spec.
     *
     * <p>The partition name is in the following format:
     *
     * <pre>
     * spec1$spec2$...$specN
     * </pre>
     *
     * <p>For example, if the partition keys are [a, b, c], and the partition spec is [1, 2, 3], the
     * partition name is "1$2$3".
     *
     * <p>Currently, we only support one partition key. So the partition name is in the following
     * format:
     *
     * <pre>
     * spec
     * </pre>
     *
     * <p>For example, if the partition keys are [a], and the partition spec is [1], the partition
     * name is "1".
     *
     * @param partitionKeys the partition keys
     * @param partitionSpecs the partition specs
     */
    public static String getPartitionName(List<String> partitionKeys, List<String> partitionSpecs) {
        return getPartitionName(partitionKeys, partitionSpecs, true);
    }

    public static String getPartitionName(
            List<String> partitionKeys, List<String> partitionSpecs, boolean checkValid) {
        checkArgument(
                partitionKeys.size() == partitionSpecs.size(),
                "The number of partition keys and partition specs should be the same.");

        for (String value : partitionSpecs) {
            if (checkValid && value.contains(PARTITION_SPEC_SEPARATOR)) {
                throw new PartitionSpecInvalidException(
                        "The value of partition key should not contains separator: '"
                                + PARTITION_SPEC_SEPARATOR
                                + "'");
            }
        }
        return String.join(PARTITION_SPEC_SEPARATOR, partitionSpecs);
    }

    /**
     * Generate auto partition name in server. When we auto creating a partition, we need to
     * generate a partition name. As we only support one partition key now, the partition name is in
     * the following format:
     *
     * <pre>
     * value
     * </pre>
     *
     * <p>The value is the formatted time with the specified time unit.
     *
     * @param partitionKeys the partition keys
     * @param current the current time
     * @param offset the offset
     * @param timeUnit the time unit
     * @return the auto partition name
     */
    public static String generateAutoPartitionName(
            List<String> partitionKeys,
            ZonedDateTime current,
            int offset,
            AutoPartitionTimeUnit timeUnit) {
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

        return getPartitionName(partitionKeys, Collections.singletonList(autoPartitionFieldSpec));
    }

    private static String getFormattedTime(ZonedDateTime zonedDateTime, String format) {
        return DateTimeFormatter.ofPattern(format).format(zonedDateTime);
    }
}
