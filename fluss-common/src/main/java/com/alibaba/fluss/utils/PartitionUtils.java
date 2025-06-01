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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.exception.InvalidPartitionException;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.fluss.types.PartitionNameConverters;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.metadata.TablePath.detectInvalidName;

/** Utils for partition. */
public class PartitionUtils {

    // TODO Support other data types, trace by https://github.com/alibaba/fluss/issues/489
    public static final List<DataTypeRoot> PARTITION_KEY_SUPPORTED_TYPES =
            Arrays.asList(
                    DataTypeRoot.STRING,
                    DataTypeRoot.BOOLEAN,
                    DataTypeRoot.BINARY,
                    DataTypeRoot.BYTES,
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.DATE,
                    DataTypeRoot.TIME_WITHOUT_TIME_ZONE,
                    DataTypeRoot.BIGINT,
                    DataTypeRoot.FLOAT,
                    DataTypeRoot.DOUBLE,
                    DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

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

    public static String convertValueOfType(Object value, DataTypeRoot type) {
        String stringPartitionKey = "";
        switch (type) {
            case BOOLEAN:
                Boolean booleanValue = (Boolean) value;
                stringPartitionKey = booleanValue.toString();
                break;
            case BINARY:
            case BYTES:
                byte[] bytesValue = (byte[]) value;
                stringPartitionKey = PartitionNameConverters.hexString(bytesValue);
                break;
            case TINYINT:
                Byte tinyIntValue = (Byte) value;
                stringPartitionKey = tinyIntValue.toString();
                break;
            case SMALLINT:
                Short smallIntValue = (Short) value;
                stringPartitionKey = smallIntValue.toString();
                break;
            case INTEGER:
                Integer intValue = (Integer) value;
                stringPartitionKey = intValue.toString();
                break;
            case DATE:
                Integer dateValue = (Integer) value;
                stringPartitionKey = PartitionNameConverters.dayToString(dateValue);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                Integer timeValue = (Integer) value;
                stringPartitionKey = PartitionNameConverters.milliToString(timeValue);
                break;
            case FLOAT:
                Float floatValue = (Float) value;
                stringPartitionKey = PartitionNameConverters.reformatFloat(floatValue);
                break;
            case DOUBLE:
                Double doubleValue = (Double) value;
                stringPartitionKey = PartitionNameConverters.reformatDouble(doubleValue);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampLtz timeStampLTZValue = (TimestampLtz) value;
                stringPartitionKey = PartitionNameConverters.timestampToString(timeStampLTZValue);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampNtz timeStampNTZValue = (TimestampNtz) value;
                stringPartitionKey = PartitionNameConverters.timestampToString(timeStampNTZValue);
                break;
            case BIGINT:
                Long bigIntValue = (Long) value;
                stringPartitionKey = bigIntValue.toString();
                break;
        }
        return stringPartitionKey;
    }
}
