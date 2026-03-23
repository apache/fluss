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

package org.apache.fluss.utils;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypeRoot;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.metadata.TablePath.detectInvalidName;
import static org.apache.fluss.metadata.TablePath.validatePrefix;

/** Utils for partition. */
public class PartitionUtils {

    public static final List<DataTypeRoot> PARTITION_KEY_SUPPORTED_TYPES =
            Arrays.asList(
                    DataTypeRoot.CHAR,
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
            TablePath tablePath,
            List<String> partitionKeys,
            PartitionSpec partitionSpec,
            boolean isCreate) {
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

        validatePartitionValues(reOrderedPartitionValues, isCreate);
    }

    @VisibleForTesting
    static void validatePartitionValues(List<String> partitionValues, boolean isCreate) {
        for (String value : partitionValues) {
            String invalidNameError = detectInvalidName(value);
            if (invalidNameError != null || (isCreate && validatePrefix(value) != null)) {
                throw new InvalidPartitionException(
                        "The partition value "
                                + value
                                + " is invalid: "
                                + (invalidNameError != null
                                        ? invalidNameError
                                        : validatePrefix(value)));
            }
        }
    }

    /**
     * Validates that the partition time value in the given {@link PartitionSpec} is valid and not
     * out-of-date when auto-partition is enabled. Throws {@link InvalidPartitionException} if the
     * format doesn't match or the partition is older than the earliest retained one.
     */
    public static void validateAutoPartitionTime(
            PartitionSpec partitionSpec,
            List<String> partitionKeys,
            AutoPartitionStrategy autoPartitionStrategy) {
        if (!autoPartitionStrategy.isAutoPartitionEnabled()) {
            return;
        }
        String autoPartitionKey =
                autoPartitionStrategy.key() != null
                        ? autoPartitionStrategy.key()
                        : partitionKeys.get(0);
        String partitionTime = partitionSpec.getSpecMap().get(autoPartitionKey);
        AutoPartitionTimeUnit timeUnit = autoPartitionStrategy.timeUnit();
        if (partitionTime == null || !isValidPartitionTime(partitionTime, timeUnit)) {
            throw new InvalidPartitionException(
                    String.format(
                            "Partition value '%s' does not match the expected format '%s' "
                                    + "for auto-partition time unit '%s'.",
                            partitionTime, getPartitionTimeFormat(timeUnit), timeUnit));
        }
        ZonedDateTime currentZonedDateTime =
                ZonedDateTime.ofInstant(Instant.now(), autoPartitionStrategy.timeZone().toZoneId());
        // Get the earliest partition time that needs to be retained.
        String lastRetainPartitionTime =
                generateAutoPartitionTime(
                        currentZonedDateTime, -autoPartitionStrategy.numToRetain(), timeUnit);
        if (lastRetainPartitionTime.compareTo(partitionTime) > 0) {
            throw new InvalidPartitionException(
                    String.format(
                            "Partition value '%s' is out-of-date. The earliest retained "
                                    + "partition is '%s'.",
                            partitionTime, lastRetainPartitionTime));
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

    /** Returns the time string format pattern for the given time unit. */
    private static String getPartitionTimeFormat(AutoPartitionTimeUnit timeUnit) {
        switch (timeUnit) {
            case YEAR:
                return YEAR_FORMAT;
            case QUARTER:
                return QUARTER_FORMAT;
            case MONTH:
                return MONTH_FORMAT;
            case DAY:
                return DAY_FORMAT;
            case HOUR:
                return HOUR_FORMAT;
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
        }
    }

    /**
     * Returns true if the given time string matches the format expected for the given time unit.
     */
    private static boolean isValidPartitionTime(String time, AutoPartitionTimeUnit timeUnit) {
        try {
            DateTimeFormatter.ofPattern(getPartitionTimeFormat(timeUnit)).parse(time);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private static String getFormattedTime(ZonedDateTime zonedDateTime, String format) {
        return DateTimeFormatter.ofPattern(format).format(zonedDateTime);
    }

    public static String convertValueOfType(Object value, DataTypeRoot type) {
        String stringPartitionKey = "";
        switch (type) {
            case CHAR:
            case STRING:
                stringPartitionKey = ((BinaryString) value).toString();
                break;
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
            case BIGINT:
                Long bigIntValue = (Long) value;
                stringPartitionKey = bigIntValue.toString();
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
            default:
                throw new IllegalArgumentException("Unsupported DataTypeRoot: " + type);
        }
        return stringPartitionKey;
    }
}
