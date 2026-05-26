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

package org.apache.fluss.flink.tiering.source.watermark;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SimpleWatermarkExtractor}. */
class SimpleWatermarkExtractorTest {

    @Test
    void testCreateWithNoWatermarkConfig() {
        TableInfo tableInfo = createTableInfoWithoutWatermark();
        assertThat(SimpleWatermarkExtractor.create(tableInfo)).isNull();
    }

    @Test
    void testCreateWithSimpleColumn() {
        TableInfo tableInfo = createTableInfoWithWatermark("`ts`", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row = GenericRow.of(1, TimestampNtz.fromMillis(1000L));
        assertThat(extractor.currentWatermark(row)).isEqualTo(1000L);
    }

    @Test
    void testCreateWithSimpleColumnWithoutBackticks() {
        TableInfo tableInfo = createTableInfoWithWatermark("ts", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row = GenericRow.of(1, TimestampNtz.fromMillis(2000L));
        assertThat(extractor.currentWatermark(row)).isEqualTo(2000L);
    }

    @Test
    void testCreateWithColumnMinusInterval() {
        TableInfo tableInfo =
                createTableInfoWithWatermark("`ts` - INTERVAL '5' SECOND", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row = GenericRow.of(1, TimestampNtz.fromMillis(10000L));
        assertThat(extractor.currentWatermark(row)).isEqualTo(5000L);
    }

    @Test
    void testCreateWithDecimalInterval() {
        TableInfo tableInfo =
                createTableInfoWithWatermark("`ts` - INTERVAL '0.001' SECOND", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row = GenericRow.of(1, TimestampNtz.fromMillis(1000L));
        assertThat(extractor.currentWatermark(row)).isEqualTo(999L);
    }

    @Test
    void testCreateWithMinuteInterval() {
        TableInfo tableInfo =
                createTableInfoWithWatermark("`ts` - INTERVAL '2' MINUTE", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row = GenericRow.of(1, TimestampNtz.fromMillis(200000L));
        assertThat(extractor.currentWatermark(row)).isEqualTo(80000L);
    }

    @Test
    void testCreateWithHourInterval() {
        TableInfo tableInfo =
                createTableInfoWithWatermark("`ts` - INTERVAL '1' HOUR", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row = GenericRow.of(1, TimestampNtz.fromMillis(7200000L));
        assertThat(extractor.currentWatermark(row)).isEqualTo(3600000L);
    }

    @Test
    void testCreateWithDayInterval() {
        TableInfo tableInfo =
                createTableInfoWithWatermark("`ts` - INTERVAL '1' DAY", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row = GenericRow.of(1, TimestampNtz.fromMillis(172800000L));
        assertThat(extractor.currentWatermark(row)).isEqualTo(86400000L);
    }

    @Test
    void testCreateWithComputedColumn() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .build();
        Map<String, String> customProps = new HashMap<>();
        customProps.put("schema.watermark.0.rowtime", "computed_ts");
        customProps.put("schema.watermark.0.strategy.expr", "`computed_ts`");
        customProps.put("schema.watermark.0.strategy.data-type", "TIMESTAMP(3)");
        TableInfo tableInfo = createTableInfoFromSchema(schema, customProps);
        assertThat(SimpleWatermarkExtractor.create(tableInfo)).isNull();
    }

    @Test
    void testCreateWithComplexExpression() {
        TableInfo tableInfo = createTableInfoWithWatermark("func(`ts`)", "TIMESTAMP(3)");
        assertThat(SimpleWatermarkExtractor.create(tableInfo)).isNull();
    }

    @Test
    void testCreateWithTimestampLtz() {
        TableInfo tableInfo =
                createTableInfoWithWatermark("`ts` - INTERVAL '3' SECOND", "TIMESTAMP_LTZ(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row = GenericRow.of(1, TimestampLtz.fromEpochMillis(10000L));
        assertThat(extractor.currentWatermark(row)).isEqualTo(7000L);
    }

    @Test
    void testCreateWithNoDataType() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .build();
        Map<String, String> customProps = new HashMap<>();
        customProps.put("schema.watermark.0.rowtime", "ts");
        customProps.put("schema.watermark.0.strategy.expr", "`ts`");
        TableInfo tableInfo = createTableInfoFromSchema(schema, customProps);
        assertThat(SimpleWatermarkExtractor.create(tableInfo)).isNull();
    }

    @Test
    void testCurrentWatermarkWithNullField() {
        TableInfo tableInfo = createTableInfoWithWatermark("`ts`", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row = GenericRow.of(1, null);
        assertThat(extractor.currentWatermark(row)).isNull();
    }

    @Test
    void testCurrentWatermarkWithTimestampLtzSimpleColumn() {
        TableInfo tableInfo = createTableInfoWithWatermark("`ts`", "TIMESTAMP_LTZ(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row = GenericRow.of(1, TimestampLtz.fromEpochMillis(5000L));
        assertThat(extractor.currentWatermark(row)).isEqualTo(5000L);
    }

    @ParameterizedTest
    @CsvSource({
        "TIMESTAMP, 0",
        "TIMESTAMP, 1",
        "TIMESTAMP, 2",
        "TIMESTAMP_LTZ, 0",
        "TIMESTAMP_LTZ, 1",
        "TIMESTAMP_LTZ, 2",
    })
    void testCreateWithTimestampPrecision(String dataTypeName, int precision) {
        boolean isTimestampLtz = dataTypeName.equals("TIMESTAMP_LTZ");
        String dataType = dataTypeName + "(" + precision + ")";
        TableInfo tableInfo = createTableInfoWithWatermark("`ts` - INTERVAL '1' SECOND", dataType);
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row =
                isTimestampLtz
                        ? GenericRow.of(1, TimestampLtz.fromEpochMillis(5000L))
                        : GenericRow.of(1, TimestampNtz.fromMillis(5000L));
        assertThat(extractor.currentWatermark(row)).isEqualTo(4000L);
    }

    private static TableInfo createTableInfoWithWatermark(String expr, String dataType) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("ts", DataTypes.parse(dataType))
                        .build();
        Map<String, String> customProps = new HashMap<>();
        customProps.put("schema.watermark.0.rowtime", "ts");
        customProps.put("schema.watermark.0.strategy.expr", expr);
        customProps.put("schema.watermark.0.strategy.data-type", dataType);
        return createTableInfoFromSchema(schema, customProps);
    }

    private static TableInfo createTableInfoWithoutWatermark() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .build();
        return createTableInfoFromSchema(schema, new HashMap<>());
    }

    private static TableInfo createTableInfoFromSchema(
            Schema schema, Map<String, String> customProps) {
        return new TableInfo(
                TablePath.of("test_db", "test_table"),
                1L,
                0,
                schema,
                Collections.emptyList(),
                Collections.emptyList(),
                1,
                new Configuration(),
                Configuration.fromMap(customProps),
                null,
                null,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }
}
