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
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionKey;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PartitionComputer}. */
class PartitionComputerTest {

    @Test
    void testComputePhysicalOnlyPartition() {
        TableInfo tableInfo =
                tableInfo(
                        Schema.newBuilder()
                                .column("region", DataTypes.STRING().copy(false))
                                .column("dt", DataTypes.DATE().copy(false))
                                .build(),
                        TableDescriptor.builder().partitionedBy("region", "dt").distributedBy(1));
        PartitionComputer partitionComputer =
                new PartitionComputer(tableInfo, tableInfo.getRowType());

        assertThat(
                        partitionComputer.getResolvedPartitionSpec(
                                GenericRow.of(
                                        BinaryString.fromString("us"),
                                        (int) LocalDate.of(2024, 3, 15).toEpochDay())))
                .hasToString("region=us/dt=2024-03-15");
        assertThat(
                        partitionComputer.getPartition(
                                GenericRow.of(
                                        BinaryString.fromString("us"),
                                        (int) LocalDate.of(2024, 3, 15).toEpochDay())))
                .isEqualTo("us$2024-03-15");
    }

    @Test
    void testComputeMixedPhysicalAndImplicitPartition() {
        TableInfo tableInfo =
                tableInfo(
                        Schema.newBuilder()
                                .column("region", DataTypes.STRING().copy(false))
                                .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                .build(),
                        TableDescriptor.builder()
                                .partitionedByKeys(
                                        PartitionKey.column("region"),
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_month",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.MONTH))))
                                .distributedBy(1));

        PartitionComputer partitionComputer =
                new PartitionComputer(tableInfo, tableInfo.getRowType());

        assertThat(
                        partitionComputer.getResolvedPartitionSpec(
                                GenericRow.of(
                                        BinaryString.fromString("us"),
                                        TimestampNtz.fromLocalDateTime(
                                                LocalDateTime.of(2024, 3, 15, 10, 30)))))
                .hasToString("region=us/event_month=202403");
        assertThat(
                        partitionComputer.getPartition(
                                GenericRow.of(
                                        BinaryString.fromString("us"),
                                        TimestampNtz.fromLocalDateTime(
                                                LocalDateTime.of(2024, 3, 15, 10, 30)))))
                .isEqualTo("us$202403");
    }

    @Test
    void testPartitionRowTypeForImplicitPartition() {
        TableInfo tableInfo =
                tableInfo(
                        Schema.newBuilder()
                                .column("region", DataTypes.STRING().copy(false))
                                .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                .build(),
                        TableDescriptor.builder()
                                .partitionedByKeys(
                                        PartitionKey.column("region"),
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY))))
                                .distributedBy(1));

        RowType partitionRowType = PartitionUtils.partitionRowType(tableInfo);

        assertThat(partitionRowType.getFieldNames()).containsExactly("region", "event_day");
        assertThat(partitionRowType.getTypeAt(0).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
        assertThat(partitionRowType.getTypeAt(1).getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
        assertThat(
                        PartitionUtils.toPartitionRow(
                                        Arrays.asList("us", "20240315"), partitionRowType)
                                .getString(1)
                                .toString())
                .isEqualTo("20240315");
    }

    @Test
    void testComputeDateSourcePartition() {
        TableInfo tableInfo =
                tableInfo(
                        Schema.newBuilder().column("dt", DataTypes.DATE().copy(false)).build(),
                        TableDescriptor.builder()
                                .partitionedByKeys(
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "dt_month",
                                                        DateTruncPartitionTransform.of(
                                                                "dt",
                                                                AutoPartitionTimeUnit.MONTH))))
                                .distributedBy(1));

        PartitionComputer partitionComputer =
                new PartitionComputer(tableInfo, tableInfo.getRowType());

        assertThat(
                        partitionComputer.getResolvedPartitionSpec(
                                GenericRow.of((int) LocalDate.of(2024, 3, 15).toEpochDay())))
                .hasToString("dt_month=202403");
        assertThat(
                        partitionComputer.getPartition(
                                GenericRow.of((int) LocalDate.of(2024, 12, 31).toEpochDay())))
                .isEqualTo("202412");
    }

    @Test
    void testComputeMultipleVirtualKeysAndCanonicalFormats() {
        TableInfo tableInfo =
                tableInfo(
                        Schema.newBuilder()
                                .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                .build(),
                        TableDescriptor.builder()
                                .partitionedByKeys(
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_hour",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.HOUR))),
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY))),
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_month",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.MONTH))),
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_quarter",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.QUARTER))),
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_year",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.YEAR))))
                                .distributedBy(1));

        PartitionComputer partitionComputer =
                new PartitionComputer(tableInfo, tableInfo.getRowType());

        assertThat(
                        partitionComputer
                                .getResolvedPartitionSpec(
                                        GenericRow.of(
                                                TimestampNtz.fromLocalDateTime(
                                                        LocalDateTime.of(2024, 11, 11, 11, 30))))
                                .getPartitionValues())
                .containsExactly("2024111111", "20241111", "202411", "20244", "2024");
    }

    @Test
    void testExpressionOrderDoesNotOverridePartitionKeyOrder() {
        RowType inputRowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING().copy(false), DataTypes.TIMESTAMP()},
                        new String[] {"region", "event_time"});
        PartitionComputer partitionComputer =
                new PartitionComputer(
                        Arrays.asList("event_day", "region", "event_month"),
                        Arrays.asList(
                                PartitionExpression.of(
                                        "event_month",
                                        DateTruncPartitionTransform.of(
                                                "event_time", AutoPartitionTimeUnit.MONTH)),
                                PartitionExpression.of(
                                        "event_day",
                                        DateTruncPartitionTransform.of(
                                                "event_time", AutoPartitionTimeUnit.DAY))),
                        inputRowType);

        assertThat(
                        partitionComputer.getResolvedPartitionSpec(
                                GenericRow.of(
                                        BinaryString.fromString("eu"),
                                        TimestampNtz.fromLocalDateTime(
                                                LocalDateTime.of(2024, 3, 15, 10, 30)))))
                .hasToString("event_day=20240315/region=eu/event_month=202403");
    }

    @Test
    void testComputeFromProjectedInputRowTypeByName() {
        TableInfo tableInfo =
                tableInfo(
                        Schema.newBuilder()
                                .column("region", DataTypes.STRING().copy(false))
                                .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                .build(),
                        TableDescriptor.builder()
                                .partitionedByKeys(
                                        PartitionKey.column("region"),
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY))))
                                .distributedBy(1));
        RowType projectedInputRowType =
                RowType.of(
                        new DataType[] {DataTypes.TIMESTAMP(), DataTypes.STRING()},
                        new String[] {"event_time", "region"});
        PartitionComputer partitionComputer =
                new PartitionComputer(tableInfo, projectedInputRowType);

        assertThat(
                        partitionComputer.getResolvedPartitionSpec(
                                GenericRow.of(
                                        TimestampNtz.fromLocalDateTime(
                                                LocalDateTime.of(2024, 3, 15, 10, 30)),
                                        BinaryString.fromString("eu"))))
                .hasToString("region=eu/event_day=20240315");
    }

    @Test
    void testMissingProjectedInputColumnsFailFast() {
        TableInfo tableInfo =
                tableInfo(
                        Schema.newBuilder()
                                .column("region", DataTypes.STRING().copy(false))
                                .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                .build(),
                        TableDescriptor.builder()
                                .partitionedByKeys(
                                        PartitionKey.column("region"),
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY))))
                                .distributedBy(1));

        assertThatThrownBy(
                        () ->
                                new PartitionComputer(
                                        tableInfo,
                                        RowType.of(
                                                new DataType[] {DataTypes.TIMESTAMP()},
                                                new String[] {"event_time"})))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The partition column region is not in the row");

        assertThatThrownBy(
                        () ->
                                new PartitionComputer(
                                        tableInfo,
                                        RowType.of(
                                                new DataType[] {DataTypes.STRING()},
                                                new String[] {"region"})))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The partition transform source column event_time is not in the row");
    }

    @Test
    void testOldClientIgnoringPartitionExpressionsFailsFast() {
        RowType inputRowType =
                RowType.of(
                        new DataType[] {DataTypes.TIMESTAMP().copy(false)},
                        new String[] {"event_time"});

        assertThatThrownBy(
                        () ->
                                new PartitionComputer(
                                        Arrays.asList("event_day"), Arrays.asList(), inputRowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The partition column event_day is not in the row")
                .hasMessageContaining("event_time");
    }

    @Test
    void testRejectInconsistentPartitionExpressions() {
        RowType inputRowType =
                RowType.of(
                        new DataType[] {DataTypes.TIMESTAMP().copy(false)},
                        new String[] {"event_time"});

        assertThatThrownBy(
                        () ->
                                new PartitionComputer(
                                        Arrays.asList("event_day"),
                                        Arrays.asList(
                                                PartitionExpression.of(
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY))),
                                        inputRowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Virtual partition expression must have a resolved partition spec key.");

        assertThatThrownBy(
                        () ->
                                new PartitionComputer(
                                        Arrays.asList("event_day"),
                                        Arrays.asList(
                                                PartitionExpression.of(
                                                        "event_month",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.MONTH))),
                                        inputRowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Virtual partition spec key 'event_month' is not present in partition keys [event_day].");

        assertThatThrownBy(
                        () ->
                                new PartitionComputer(
                                        Arrays.asList("event_day"),
                                        Arrays.asList(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY)),
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.MONTH))),
                                        inputRowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate virtual partition spec key 'event_day'.");
    }

    @Test
    void testNullRuntimePartitionValuesFailFast() {
        TableInfo mixedTableInfo =
                tableInfo(
                        Schema.newBuilder()
                                .column("region", DataTypes.STRING().copy(false))
                                .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                .build(),
                        TableDescriptor.builder()
                                .partitionedByKeys(
                                        PartitionKey.column("region"),
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY))))
                                .distributedBy(1));
        PartitionComputer mixedPartitionComputer =
                new PartitionComputer(mixedTableInfo, mixedTableInfo.getRowType());

        assertThatThrownBy(
                        () ->
                                mixedPartitionComputer.getPartition(
                                        GenericRow.of(
                                                null,
                                                TimestampNtz.fromLocalDateTime(
                                                        LocalDateTime.of(2024, 3, 15, 10, 30)))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Partition value for 'region' shouldn't be null.");

        TableInfo virtualOnlyTableInfo =
                tableInfo(
                        Schema.newBuilder()
                                .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                .build(),
                        TableDescriptor.builder()
                                .partitionedByKeys(
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY))))
                                .distributedBy(1));
        PartitionComputer virtualOnlyPartitionComputer =
                new PartitionComputer(virtualOnlyTableInfo, virtualOnlyTableInfo.getRowType());

        assertThatThrownBy(
                        () ->
                                virtualOnlyPartitionComputer.getPartition(
                                        GenericRow.of((Object) null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Partition transform source value for 'event_time' shouldn't be null.");
    }

    @Test
    void testTimestampNtzDoesNotUseJvmDefaultTimeZone() {
        TableInfo tableInfo =
                tableInfo(
                        Schema.newBuilder()
                                .column("event_time", DataTypes.TIMESTAMP().copy(false))
                                .build(),
                        TableDescriptor.builder()
                                .partitionedByKeys(
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_day",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.DAY))))
                                .distributedBy(1));
        PartitionComputer partitionComputer =
                new PartitionComputer(tableInfo, tableInfo.getRowType());
        GenericRow row =
                GenericRow.of(TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 4, 1, 0, 30)));
        TimeZone originalTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            String utcPartition = partitionComputer.getPartition(row);
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            String shanghaiPartition = partitionComputer.getPartition(row);

            assertThat(shanghaiPartition).isEqualTo(utcPartition).isEqualTo("20240401");
        } finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }

    @Test
    void testComputeTimestampLtzWithResolvedTimeZone() {
        TableInfo tableInfo =
                tableInfo(
                        Schema.newBuilder()
                                .column("event_time", DataTypes.TIMESTAMP_LTZ().copy(false))
                                .build(),
                        TableDescriptor.builder()
                                .partitionedByKeys(
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_hour",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.HOUR,
                                                                ZoneId.of("Asia/Shanghai")))))
                                .distributedBy(1));

        PartitionComputer partitionComputer =
                new PartitionComputer(tableInfo, tableInfo.getRowType());

        assertThat(
                        partitionComputer.getPartition(
                                GenericRow.of(
                                        TimestampLtz.fromInstant(
                                                Instant.parse("2024-03-31T16:30:00Z")))))
                .isEqualTo("2024040100");
    }

    @Test
    void testTimestampLtzRequiresResolvedTimeZone() {
        TableInfo tableInfo =
                tableInfo(
                        Schema.newBuilder()
                                .column("event_time", DataTypes.TIMESTAMP_LTZ().copy(false))
                                .build(),
                        TableDescriptor.builder()
                                .partitionedByKeys(
                                        PartitionKey.expression(
                                                PartitionExpression.of(
                                                        "event_hour",
                                                        DateTruncPartitionTransform.of(
                                                                "event_time",
                                                                AutoPartitionTimeUnit.HOUR))))
                                .distributedBy(1));

        PartitionComputer partitionComputer =
                new PartitionComputer(tableInfo, tableInfo.getRowType());

        assertThatThrownBy(
                        () ->
                                partitionComputer.getPartition(
                                        GenericRow.of(TimestampLtz.fromEpochMillis(0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("DateTruncPartitionTransform must contain a resolved time zone.");
    }

    private static TableInfo tableInfo(Schema schema, TableDescriptor.Builder descriptorBuilder) {
        TableDescriptor descriptor = descriptorBuilder.schema(schema).build();
        return TableInfo.of(TablePath.of("db", "t"), 1, 0, descriptor, null, 0, 0);
    }
}
