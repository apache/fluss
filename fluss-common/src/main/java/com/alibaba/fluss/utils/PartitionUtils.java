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
     * <p>For the table with single partition key. The partition name is in the following format:
     *
     * <pre>
     * spec
     * </pre>
     *
     * <p>For example, if the partition keys are [a], and the partition spec is [1], the partition
     * name is "1".
     *
     * @param partitionSpecs the partition specs
     */
    public static String getPartitionName(List<String> partitionSpecs) {
        return getPartitionName(partitionSpecs, true);
    }

    public static String getPartitionName(List<String> partitionSpecs, boolean checkValid) {
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
     * Generate auto spec name in server. When we auto creating a partition, we need to generate an
     * auto spec first.
     *
     * <pre>
     * value
     * </pre>
     *
     * <p>The value is the formatted time with the specified time unit.
     *
     * @param current the current time
     * @param offset the offset
     * @param timeUnit the time unit
     * @return the auto partition name
     */
    public static String generateAutoSpec(
            ZonedDateTime current, int offset, AutoPartitionTimeUnit timeUnit) {
        String autoSpec;
        switch (timeUnit) {
            case YEAR:
                autoSpec = getFormattedTime(current.plusYears(offset), YEAR_FORMAT);
                break;
            case QUARTER:
                autoSpec = getFormattedTime(current.plusMonths(offset * 3L), QUARTER_FORMAT);
                break;
            case MONTH:
                autoSpec = getFormattedTime(current.plusMonths(offset), MONTH_FORMAT);
                break;
            case DAY:
                autoSpec = getFormattedTime(current.plusDays(offset), DAY_FORMAT);
                break;
            case HOUR:
                autoSpec = getFormattedTime(current.plusHours(offset), HOUR_FORMAT);
                break;
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
        }

        return autoSpec;
    }

    public static String extractAutoSpecFromPartitionName(String partitionName) {
        String[] partitionSpec = partitionName.split("\\$");
        return partitionSpec[partitionSpec.length - 1];
    }

    public static String generateAutoPartitionName(
            String autoPartitionPrefix, String autoPartitionSpec) {
        return autoPartitionPrefix + PARTITION_SPEC_SEPARATOR + autoPartitionSpec;
    }

    private static String getFormattedTime(ZonedDateTime zonedDateTime, String format) {
        return DateTimeFormatter.ofPattern(format).format(zonedDateTime);
    }
}
