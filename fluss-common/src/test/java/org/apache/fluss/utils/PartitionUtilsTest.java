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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionKey;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.metadata.TablePath.detectInvalidName;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.utils.PartitionUtils.convertValueOfType;
import static org.apache.fluss.utils.PartitionUtils.generateAutoPartition;
import static org.apache.fluss.utils.PartitionUtils.parseValueOfType;
import static org.apache.fluss.utils.PartitionUtils.validateAutoPartitionTime;
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
        TableInfo tableInfo =
                TableInfo.of(DATA1_TABLE_PATH, 1L, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
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
    void testAutoPartitionStrategyUsesConfiguredTimeFormat() {
        Configuration conf = new Configuration();
        assertThat(AutoPartitionStrategy.from(conf).timeFormat()).isNull();

        conf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "yyyy-MM-dd");
        assertThat(AutoPartitionStrategy.from(conf).timeFormat()).isEqualTo("yyyy-MM-dd");
    }

    @Test
    void testValidateCustomTimeFormat() {
        Configuration conf = new Configuration();
        AutoPartitionTimeUnit[] timeUnits = AutoPartitionTimeUnit.values();
        String[] validFormats = {"yyyy", "yyyy-'Q'Q", "yyyy-MM", "yyyy-MM-dd", "yyyy-MM-dd-HH"};
        for (int index = 0; index < timeUnits.length; index++) {
            conf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, validFormats[index]);
            AutoPartitionStrategy validStrategy = AutoPartitionStrategy.from(conf);
            AutoPartitionTimeUnit timeUnit = timeUnits[index];
            assertThatNoException()
                    .isThrownBy(() -> PartitionUtils.validateTimeFormat(timeUnit, validStrategy));
        }

        conf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "MM-yyyy");
        AutoPartitionStrategy invalidOrderStrategy = AutoPartitionStrategy.from(conf);
        assertThatThrownBy(
                        () ->
                                PartitionUtils.validateTimeFormat(
                                        AutoPartitionTimeUnit.MONTH, invalidOrderStrategy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain fields [year, month] in this order");

        conf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "yyyy-MM-dd");
        AutoPartitionStrategy insufficientPrecisionStrategy = AutoPartitionStrategy.from(conf);
        assertThatThrownBy(
                        () ->
                                PartitionUtils.validateTimeFormat(
                                        AutoPartitionTimeUnit.HOUR, insufficientPrecisionStrategy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("[year, month, day, hour]");

        conf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "yyyy-M-dd");
        AutoPartitionStrategy variableWidthStrategy = AutoPartitionStrategy.from(conf);
        assertThatThrownBy(
                        () ->
                                PartitionUtils.validateTimeFormat(
                                        AutoPartitionTimeUnit.DAY, variableWidthStrategy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must have fixed width 2");

        conf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "yyyy[-MM[-dd]]");
        AutoPartitionStrategy optionalSectionsStrategy = AutoPartitionStrategy.from(conf);
        assertThatThrownBy(
                        () ->
                                PartitionUtils.validateTimeFormat(
                                        AutoPartitionTimeUnit.DAY, optionalSectionsStrategy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not contain optional sections");

        conf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "yyyy/MM/dd");
        AutoPartitionStrategy invalidCharacterStrategy = AutoPartitionStrategy.from(conf);
        assertThatThrownBy(
                        () ->
                                PartitionUtils.validateTimeFormat(
                                        AutoPartitionTimeUnit.DAY, invalidCharacterStrategy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("generates invalid partition value '2024/11/11'")
                .hasMessageContaining("ASCII alphanumerics, '_' and '-'");

        conf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "__yyyy");
        AutoPartitionStrategy reservedPrefixStrategy = AutoPartitionStrategy.from(conf);
        assertThatThrownBy(
                        () ->
                                PartitionUtils.validateTimeFormat(
                                        AutoPartitionTimeUnit.YEAR, reservedPrefixStrategy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("generates invalid partition value '__2024'")
                .hasMessageContaining("reserved");

        conf.setString(
                ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT,
                "yyyy" + org.apache.commons.lang3.StringUtils.repeat("_", 197));
        AutoPartitionStrategy overlongValueStrategy = AutoPartitionStrategy.from(conf);
        assertThatThrownBy(
                        () ->
                                PartitionUtils.validateTimeFormat(
                                        AutoPartitionTimeUnit.YEAR, overlongValueStrategy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("longer than the max allowed length 200");
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
                AutoPartitionStrategy.from(new Configuration()),
                AutoPartitionTimeUnit.DAY,
                new int[] {-1, 0, 1, 2, 3, 20},
                new String[] {
                    "20241110", "20241111", "20241112", "20241113", "20241114", "20241201"
                });

        Configuration dashedDayConf = new Configuration();
        dashedDayConf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "yyyy-MM-dd");
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionStrategy.from(dashedDayConf),
                AutoPartitionTimeUnit.DAY,
                new int[] {-1, 0, 1, 2, 3, 20},
                new String[] {
                    "2024-11-10",
                    "2024-11-11",
                    "2024-11-12",
                    "2024-11-13",
                    "2024-11-14",
                    "2024-12-01"
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
        testGenerateAutoPartitionName(
                zonedDateTime,
                AutoPartitionStrategy.from(new Configuration()),
                autoPartitionTimeUnit,
                offsets,
                expected);
    }

    void testGenerateAutoPartitionName(
            ZonedDateTime zonedDateTime,
            AutoPartitionStrategy autoPartitionStrategy,
            AutoPartitionTimeUnit autoPartitionTimeUnit,
            int[] offsets,
            String[] expected) {
        for (int i = 0; i < offsets.length; i++) {
            ResolvedPartitionSpec resolvedPartitionSpec =
                    generateAutoPartition(
                            Collections.singletonList("dt"),
                            zonedDateTime,
                            offsets[i],
                            autoPartitionTimeUnit,
                            autoPartitionStrategy);
            assertThat(resolvedPartitionSpec.getPartitionName()).isEqualTo(expected[i]);
        }
    }

    @Test
    void testValidateAutoPartitionTimeWithConfiguredDayFormat() {
        Configuration dashedDayConf = new Configuration();
        dashedDayConf.setBoolean(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
        dashedDayConf.set(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.DAY);
        dashedDayConf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "yyyy-MM-dd");
        AutoPartitionStrategy dashedDayStrategy = AutoPartitionStrategy.from(dashedDayConf);

        LocalDate today =
                Instant.now().atZone(dashedDayStrategy.timeZone().toZoneId()).toLocalDate();

        assertThatNoException()
                .isThrownBy(
                        () ->
                                validateAutoPartitionTime(
                                        new PartitionSpec(
                                                Collections.singletonMap("dt", today.toString())),
                                        Collections.singletonList("dt"),
                                        dashedDayStrategy));

        assertThatThrownBy(
                        () ->
                                validateAutoPartitionTime(
                                        new PartitionSpec(
                                                Collections.singletonMap(
                                                        "dt",
                                                        today.format(
                                                                java.time.format.DateTimeFormatter
                                                                        .ofPattern("yyyyMMdd")))),
                                        Collections.singletonList("dt"),
                                        dashedDayStrategy))
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining("yyyy-MM-dd")
                .hasMessageContaining("DAY");
    }

    @Test
    void testValidateConfiguredTimeFormatWhenAutoPartitionIsDisabled() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "yyyy-MM-dd");
        AutoPartitionStrategy strategy = AutoPartitionStrategy.from(conf);

        assertThatNoException()
                .isThrownBy(
                        () ->
                                validateAutoPartitionTime(
                                        new PartitionSpec(
                                                Collections.singletonMap("dt", "2000-01-01")),
                                        Collections.singletonList("dt"),
                                        strategy));
        assertThatThrownBy(
                        () ->
                                validateAutoPartitionTime(
                                        new PartitionSpec(
                                                Collections.singletonMap("dt", "20000101")),
                                        Collections.singletonList("dt"),
                                        strategy))
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining("yyyy-MM-dd");
    }

    @Test
    void testValidateAutoPartitionTimeRetentionBoundaryWithConfiguredDayFormat() {
        Configuration dashedDayConf = new Configuration();
        dashedDayConf.setBoolean(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
        dashedDayConf.set(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.DAY);
        dashedDayConf.setString(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT, "yyyy-MM-dd");
        AutoPartitionStrategy dashedDayStrategy = AutoPartitionStrategy.from(dashedDayConf);

        LocalDate today =
                Instant.now().atZone(dashedDayStrategy.timeZone().toZoneId()).toLocalDate();
        LocalDate earliestRetained = today.minusDays(7);
        LocalDate outOfDate = today.minusDays(8);

        assertThatNoException()
                .isThrownBy(
                        () ->
                                validateAutoPartitionTime(
                                        new PartitionSpec(
                                                Collections.singletonMap(
                                                        "dt", earliestRetained.toString())),
                                        Collections.singletonList("dt"),
                                        dashedDayStrategy));

        assertThatThrownBy(
                        () ->
                                validateAutoPartitionTime(
                                        new PartitionSpec(
                                                Collections.singletonMap(
                                                        "dt", outOfDate.toString())),
                                        Collections.singletonList("dt"),
                                        dashedDayStrategy))
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining(outOfDate.toString())
                .hasMessageContaining(earliestRetained.toString());
    }

    @Test
    void testImplicitPartitionRowTypeUsesVirtualPartitionKeyAsString() {
        Schema schema =
                Schema.newBuilder()
                        .column("region", DataTypes.STRING().copy(false))
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .column("payload", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.column("region"),
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day",
                                                DateTruncPartitionTransform.of(
                                                        "event_time", AutoPartitionTimeUnit.DAY))))
                        .distributedBy(1)
                        .build();
        TableInfo tableInfo =
                TableInfo.of(DATA1_TABLE_PATH, 1L, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);

        RowType partitionRowType = PartitionUtils.partitionRowType(tableInfo);
        GenericRow partitionRow =
                PartitionUtils.toPartitionRow(Arrays.asList("us", "20240315"), partitionRowType);

        assertThat(partitionRowType.getFieldNames()).containsExactly("region", "event_day");
        assertThat(partitionRowType.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
        assertThat(partitionRowType.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
        assertThat(partitionRowType.getTypeAt(1).isNullable()).isFalse();
        assertThat(partitionRow.getString(0)).isEqualTo(BinaryString.fromString("us"));
        assertThat(partitionRow.getString(1)).isEqualTo(BinaryString.fromString("20240315"));
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

    // ---- Round-trip tests for parseValueOfType ----

    @Test
    void testRoundTripString() {
        assertRoundTrip(BinaryString.fromString("Fluss"), DataTypeRoot.STRING);
    }

    @Test
    void testRoundTripChar() {
        assertRoundTrip(BinaryString.fromString("F"), DataTypeRoot.CHAR);
    }

    @Test
    void testRoundTripBoolean() {
        assertRoundTrip(true, DataTypeRoot.BOOLEAN);
        assertRoundTrip(false, DataTypeRoot.BOOLEAN);
    }

    @Test
    void testRoundTripBytes() {
        byte[] value = new byte[] {0x10, 0x20, 0x30, 0x40, 0x50, (byte) 0xFF};
        String str = convertValueOfType(value, DataTypeRoot.BYTES);
        byte[] parsed = (byte[]) parseValueOfType(str, DataTypeRoot.BYTES);
        assertThat(parsed).isEqualTo(value);
    }

    @Test
    void testRoundTripBinary() {
        byte[] value = new byte[] {0x10, 0x20, 0x30, 0x40, 0x50, (byte) 0xFF};
        String str = convertValueOfType(value, DataTypeRoot.BINARY);
        byte[] parsed = (byte[]) parseValueOfType(str, DataTypeRoot.BINARY);
        assertThat(parsed).isEqualTo(value);
    }

    @Test
    void testRoundTripTinyInt() {
        assertRoundTrip((byte) 100, DataTypeRoot.TINYINT);
        assertRoundTrip((byte) -100, DataTypeRoot.TINYINT);
    }

    @Test
    void testRoundTripSmallInt() {
        assertRoundTrip((short) -32760, DataTypeRoot.SMALLINT);
    }

    @Test
    void testRoundTripInteger() {
        assertRoundTrip(299000, DataTypeRoot.INTEGER);
        assertRoundTrip(-299000, DataTypeRoot.INTEGER);
    }

    @Test
    void testRoundTripBigInt() {
        assertRoundTrip(1748662955428L, DataTypeRoot.BIGINT);
        assertRoundTrip(-1748662955428L, DataTypeRoot.BIGINT);
    }

    @Test
    void testRoundTripDate() {
        assertRoundTrip(20235, DataTypeRoot.DATE);
        assertRoundTrip(0, DataTypeRoot.DATE);
    }

    @Test
    void testRoundTripTime() {
        assertRoundTrip(5402199, DataTypeRoot.TIME_WITHOUT_TIME_ZONE);
        assertRoundTrip(0, DataTypeRoot.TIME_WITHOUT_TIME_ZONE);
    }

    @Test
    void testRoundTripFloat() {
        assertRoundTrip(5.73f, DataTypeRoot.FLOAT);
        assertRoundTrip(Float.NaN, DataTypeRoot.FLOAT);
        assertRoundTrip(Float.POSITIVE_INFINITY, DataTypeRoot.FLOAT);
        assertRoundTrip(Float.NEGATIVE_INFINITY, DataTypeRoot.FLOAT);
    }

    @Test
    void testRoundTripDouble() {
        assertRoundTrip(5.73, DataTypeRoot.DOUBLE);
        assertRoundTrip(Double.NaN, DataTypeRoot.DOUBLE);
        assertRoundTrip(Double.POSITIVE_INFINITY, DataTypeRoot.DOUBLE);
        assertRoundTrip(Double.NEGATIVE_INFINITY, DataTypeRoot.DOUBLE);
    }

    @Test
    void testRoundTripTimestampNtz() {
        // With nanos
        TimestampNtz ts1 = TimestampNtz.fromMillis(1748662955428L, 99988);
        assertRoundTrip(ts1, DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);

        // Zero nanos
        TimestampNtz ts2 = TimestampNtz.fromMillis(1748662955428L, 0);
        assertRoundTrip(ts2, DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);

        // Zero millis-of-second with nanos
        TimestampNtz ts3 = TimestampNtz.fromMillis(1748662955000L, 99988);
        assertRoundTrip(ts3, DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);

        // Zero millis-of-second and zero nanos
        TimestampNtz ts4 = TimestampNtz.fromMillis(1748662955000L, 0);
        assertRoundTrip(ts4, DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);

        // Negative millis
        TimestampNtz ts5 = TimestampNtz.fromMillis(-1748662955428L, 99988);
        assertRoundTrip(ts5, DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
    }

    @Test
    void testRoundTripTimestampLtz() {
        // With nanos
        TimestampLtz ts1 = TimestampLtz.fromEpochMillis(1748662955428L, 99988);
        assertRoundTrip(ts1, DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

        // Zero nanos
        TimestampLtz ts2 = TimestampLtz.fromEpochMillis(1748662955428L, 0);
        assertRoundTrip(ts2, DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

        // Negative millis
        TimestampLtz ts3 = TimestampLtz.fromEpochMillis(-1748662955428L, 99988);
        assertRoundTrip(ts3, DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    private void assertRoundTrip(Object originalValue, DataTypeRoot type) {
        String str = convertValueOfType(originalValue, type);
        Object parsed = parseValueOfType(str, type);
        assertThat(parsed).isEqualTo(originalValue);
    }
}
