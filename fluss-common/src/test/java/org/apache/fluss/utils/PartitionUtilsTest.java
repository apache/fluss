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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypeRoot;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.metadata.TablePath.detectInvalidName;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.utils.PartitionUtils.convertValueOfType;
import static org.apache.fluss.utils.PartitionUtils.generateAutoPartition;
import static org.apache.fluss.utils.PartitionUtils.validatePartitionSpec;
import static org.apache.fluss.utils.PartitionUtils.validatePartitionValues;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PartitionUtils}. */
class PartitionUtilsTest {

    @Test
    void testValidatePartitionValues() {
        assertThatThrownBy(() -> validatePartitionValues(Arrays.asList("$1", "2"), true))
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining(
                        "The partition value $1 is invalid: '$1' contains one "
                                + "or more characters other than ASCII alphanumerics, '_' and '-'");

        assertThatThrownBy(() -> validatePartitionValues(Arrays.asList("?1", "2"), false))
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining(
                        "The partition value ?1 is invalid: '?1' contains one or more "
                                + "characters other than ASCII alphanumerics, '_' and '-'");

        assertThatThrownBy(() -> validatePartitionValues(Arrays.asList("__p1", "2"), true))
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining(
                        "The partition value __p1 is invalid: '__' is not allowed as prefix, "
                                + "since it is reserved for internal databases/internal tables/internal partitions in Fluss server");

        assertThatNoException()
                .isThrownBy(() -> validatePartitionValues(Arrays.asList("__p1", "2"), false));

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(3)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .build();
        TableInfo tableInfo = TableInfo.of(DATA1_TABLE_PATH, 1L, 1, descriptor, 1L, 1L);
        assertThatThrownBy(
                        () ->
                                validatePartitionSpec(
                                        tableInfo.getTablePath(),
                                        tableInfo.getPartitionKeys(),
                                        new PartitionSpec(Collections.emptyMap()),
                                        true))
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining(
                        "PartitionSpec size is not equal to partition keys size for "
                                + "partitioned table test_db_1.test_non_pk_table_1.");
    }

    @Test
    void testGenerateAutoPartitionName() {
        LocalDateTime localDateTime = LocalDateTime.of(2024, 11, 11, 11, 11);
        ZoneId zoneId = ZoneId.of("UTC-8");
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, zoneId);

        // for year
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionTimeUnit.YEAR,
                new int[] {-1, 0, 1, 2, 3},
                new String[] {"2023", "2024", "2025", "2026", "2027"});

        // for quarter
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionTimeUnit.QUARTER,
                new int[] {-1, 0, 1, 2, 3},
                new String[] {"20243", "20244", "20251", "20252", "20253"});

        // for month
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionTimeUnit.MONTH,
                new int[] {-1, 0, 1, 2, 3},
                new String[] {"202410", "202411", "202412", "202501", "202502"});

        // for day
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionTimeUnit.DAY,
                new int[] {-1, 0, 1, 2, 3, 20},
                new String[] {
                    "20241110", "20241111", "20241112", "20241113", "20241114", "20241201"
                });

        // for hour
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionTimeUnit.HOUR,
                new int[] {-2, -1, 0, 1, 2, 3, 13},
                new String[] {
                    "2024111109",
                    "2024111110",
                    "2024111111",
                    "2024111112",
                    "2024111113",
                    "2024111114",
                    "2024111200"
                });
    }

    void testGenerateAutoPartitionName(
            ZonedDateTime zonedDateTime,
            AutoPartitionTimeUnit autoPartitionTimeUnit,
            int[] offsets,
            String[] expected) {
        for (int i = 0; i < offsets.length; i++) {
            ResolvedPartitionSpec resolvedPartitionSpec =
                    generateAutoPartition(
                            Collections.singletonList("dt"),
                            zonedDateTime,
                            offsets[i],
                            autoPartitionTimeUnit);
            assertThat(resolvedPartitionSpec.getPartitionName()).isEqualTo(expected[i]);
        }
    }

    @Test
    void testString() {
        Object value = BinaryString.fromString("Fluss");
        DataTypeRoot type = DataTypeRoot.STRING;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("Fluss");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testCharConvert() {
        Object value = BinaryString.fromString("F");
        DataTypeRoot type = DataTypeRoot.CHAR;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("F");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testBooleanConvert() {
        Object value = true;
        DataTypeRoot type = DataTypeRoot.BOOLEAN;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("true");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testByteConvert() {
        Object value = new byte[] {0x10, 0x20, 0x30, 0x40, 0x50, (byte) 0b11111111};
        DataTypeRoot type = DataTypeRoot.BYTES;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("1020304050ff");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testBinaryConvert() {
        Object value = new byte[] {0x10, 0x20, 0x30, 0x40, 0x50, (byte) 0b11111111};
        DataTypeRoot type = DataTypeRoot.BINARY;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("1020304050ff");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testTinyInt() {
        Object value = (byte) 100;
        DataTypeRoot type = DataTypeRoot.TINYINT;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("100");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testSmallInt() {
        Object value = (short) -32760;
        DataTypeRoot type = DataTypeRoot.SMALLINT;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("-32760");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testInt() {
        Object value = 299000;
        DataTypeRoot type = DataTypeRoot.INTEGER;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("299000");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testDate() {
        Object value = 20235;
        DataTypeRoot type = DataTypeRoot.DATE;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("2025-05-27");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testTimeNTZ() {
        int value = 5402199;
        DataTypeRoot type = DataTypeRoot.TIME_WITHOUT_TIME_ZONE;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("01-30-02_199");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testFloat() {
        Object value = 5.73f;
        DataTypeRoot type = DataTypeRoot.FLOAT;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("5_73");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testDouble() {
        Object value = 5.73;
        DataTypeRoot type = DataTypeRoot.DOUBLE;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("5_73");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testTimestampNTZ() {
        long millis = 1748662955428L;
        int nanos = 99988;
        TimestampNtz timeStampNTZValue = TimestampNtz.fromMillis(millis, nanos);
        DataTypeRoot type = DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;

        String toStringResult = convertValueOfType(timeStampNTZValue, type);
        assertThat(toStringResult).isEqualTo("2025-05-31-03-42-35_428099988");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);

        // Zero nanos of millis
        millis = 1748662955428L;
        nanos = 0;
        timeStampNTZValue = TimestampNtz.fromMillis(millis, nanos);
        type = DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;

        toStringResult = convertValueOfType(timeStampNTZValue, type);
        assertThat(toStringResult).isEqualTo("2025-05-31-03-42-35_428");
        detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);

        // Zero millis
        millis = 1748662955000L;
        nanos = 99988;
        timeStampNTZValue = TimestampNtz.fromMillis(millis, nanos);
        type = DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;

        toStringResult = convertValueOfType(timeStampNTZValue, type);
        assertThat(toStringResult).isEqualTo("2025-05-31-03-42-35_000099988");
        detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);

        // Zero nanos and zero millis
        millis = 1748662955000L;
        nanos = 0;
        timeStampNTZValue = TimestampNtz.fromMillis(millis, nanos);
        type = DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;

        toStringResult = convertValueOfType(timeStampNTZValue, type);
        assertThat(toStringResult).isEqualTo("2025-05-31-03-42-35_");
        detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);

        // Negative millis
        millis = -1748662955428L;
        nanos = 99988;
        timeStampNTZValue = TimestampNtz.fromMillis(millis, nanos);
        type = DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;

        toStringResult = convertValueOfType(timeStampNTZValue, type);
        assertThat(toStringResult).isEqualTo("1914-08-03-20-17-24_572099988");
        detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testTimestampLTZ() {
        long millis = 1748662955428L;
        int nanos = 99988;
        TimestampLtz timestampLTZ = TimestampLtz.fromEpochMillis(millis, nanos);
        DataTypeRoot type = DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

        String toStringResult = convertValueOfType(timestampLTZ, type);
        assertThat(toStringResult).isEqualTo("2025-05-31-03-42-35_428099988");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);

        // Zero nanos
        millis = 1748662955428L;
        nanos = 0;
        timestampLTZ = TimestampLtz.fromEpochMillis(millis, nanos);
        type = DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

        toStringResult = convertValueOfType(timestampLTZ, type);
        assertThat(toStringResult).isEqualTo("2025-05-31-03-42-35_428");
        detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);

        // Zero millis
        millis = 1748662955000L;
        nanos = 99988;
        timestampLTZ = TimestampLtz.fromEpochMillis(millis, nanos);
        type = DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

        toStringResult = convertValueOfType(timestampLTZ, type);
        assertThat(toStringResult).isEqualTo("2025-05-31-03-42-35_000099988");
        detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);

        // Zero nanos and zero millis
        millis = 1748662955000L;
        nanos = 0;
        timestampLTZ = TimestampLtz.fromEpochMillis(millis, nanos);
        type = DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

        toStringResult = convertValueOfType(timestampLTZ, type);
        assertThat(toStringResult).isEqualTo("2025-05-31-03-42-35_");
        detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);

        // Negative millis
        millis = -1748662955428L;
        nanos = 99988;
        timestampLTZ = TimestampLtz.fromEpochMillis(millis, nanos);
        type = DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

        toStringResult = convertValueOfType(timestampLTZ, type);
        assertThat(toStringResult).isEqualTo("1914-08-03-20-17-24_572099988");
        detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    @Test
    void testBigInt() {
        long value = 1748662955428L;
        DataTypeRoot type = DataTypeRoot.BIGINT;

        String toStringResult = convertValueOfType(value, type);
        assertThat(toStringResult).isEqualTo("1748662955428");
        String detectInvalid = detectInvalidName(toStringResult);
        assertThat(detectInvalid).isEqualTo(null);
    }

    // ----------------------- IEEE 754 Special Values Partition Name Tests -----------------------

    /**
     * Tests that Float.NaN converts to partition name "NaN". This validates FR-010: System MUST
     * convert Float.NaN to valid partition name "NaN".
     *
     * <p>The partition name must be filesystem-safe and recognizable as representing NaN values.
     */
    @Test
    void testFloatNaNPartitionName() {
        String partitionName = PartitionNameConverters.reformatFloat(Float.NaN);

        assertThat(partitionName)
                .as("Float.NaN should convert to partition name 'NaN'")
                .isEqualTo("NaN");

        // Verify partition name is valid
        assertThat(detectInvalidName(partitionName))
                .as("'NaN' should be a valid partition name")
                .isNull();
    }

    /**
     * Tests that Double.NaN converts to partition name "NaN". This validates FR-011: System MUST
     * convert Double.NaN to valid partition name "NaN".
     */
    @Test
    void testDoubleNaNPartitionName() {
        String partitionName = PartitionNameConverters.reformatDouble(Double.NaN);

        assertThat(partitionName)
                .as("Double.NaN should convert to partition name 'NaN'")
                .isEqualTo("NaN");

        // Verify partition name is valid
        assertThat(detectInvalidName(partitionName))
                .as("'NaN' should be a valid partition name")
                .isNull();
    }

    /**
     * Tests that Float.POSITIVE_INFINITY converts to partition name "Inf". This validates FR-012:
     * System MUST convert Float.POSITIVE_INFINITY to valid partition name "Inf".
     */
    @Test
    void testFloatPositiveInfinityPartitionName() {
        String partitionName = PartitionNameConverters.reformatFloat(Float.POSITIVE_INFINITY);

        assertThat(partitionName)
                .as("Float.POSITIVE_INFINITY should convert to partition name 'Inf'")
                .isEqualTo("Inf");

        // Verify partition name is valid
        assertThat(detectInvalidName(partitionName))
                .as("'Inf' should be a valid partition name")
                .isNull();
    }

    /**
     * Tests that Float.NEGATIVE_INFINITY converts to partition name "-Inf". This validates FR-013:
     * System MUST convert Float.NEGATIVE_INFINITY to valid partition name "-Inf".
     */
    @Test
    void testFloatNegativeInfinityPartitionName() {
        String partitionName = PartitionNameConverters.reformatFloat(Float.NEGATIVE_INFINITY);

        assertThat(partitionName)
                .as("Float.NEGATIVE_INFINITY should convert to partition name '-Inf'")
                .isEqualTo("-Inf");

        // Verify partition name is valid
        assertThat(detectInvalidName(partitionName))
                .as("'-Inf' should be a valid partition name")
                .isNull();
    }

    /**
     * Tests that Double.POSITIVE_INFINITY converts to partition name "Inf". This validates FR-014:
     * System MUST convert Double.POSITIVE_INFINITY to valid partition name "Inf".
     */
    @Test
    void testDoublePositiveInfinityPartitionName() {
        String partitionName = PartitionNameConverters.reformatDouble(Double.POSITIVE_INFINITY);

        assertThat(partitionName)
                .as("Double.POSITIVE_INFINITY should convert to partition name 'Inf'")
                .isEqualTo("Inf");

        // Verify partition name is valid
        assertThat(detectInvalidName(partitionName))
                .as("'Inf' should be a valid partition name")
                .isNull();
    }

    /**
     * Tests that Double.NEGATIVE_INFINITY converts to partition name "-Inf". This validates FR-015:
     * System MUST convert Double.NEGATIVE_INFINITY to valid partition name "-Inf".
     */
    @Test
    void testDoubleNegativeInfinityPartitionName() {
        String partitionName = PartitionNameConverters.reformatDouble(Double.NEGATIVE_INFINITY);

        assertThat(partitionName)
                .as("Double.NEGATIVE_INFINITY should convert to partition name '-Inf'")
                .isEqualTo("-Inf");

        // Verify partition name is valid
        assertThat(detectInvalidName(partitionName))
                .as("'-Inf' should be a valid partition name")
                .isNull();
    }

    /**
     * Tests that special value partition names are recognized as valid by the filesystem path
     * validator. This verifies FR-016: Special value partition names must pass TablePath
     * validation.
     */
    @Test
    void testSpecialValuePartitionNamesAreValid() {
        // Test all special value partition names
        assertThat(detectInvalidName("NaN")).as("'NaN' partition name should be valid").isNull();

        assertThat(detectInvalidName("Inf")).as("'Inf' partition name should be valid").isNull();

        assertThat(detectInvalidName("-Inf")).as("'-Inf' partition name should be valid").isNull();
    }

    /**
     * Tests that normal Float values still convert correctly to partition names. This ensures
     * special value handling doesn't break normal value formatting.
     */
    @Test
    void testFloatNormalValuePartitionName() {
        String partitionName = PartitionNameConverters.reformatFloat(99.99f);

        assertThat(partitionName)
                .as("Normal Float values should convert with '.' replaced by '_'")
                .isEqualTo("99_99");

        assertThat(detectInvalidName(partitionName))
                .as("Normal Float partition name should be valid")
                .isNull();
    }
}
