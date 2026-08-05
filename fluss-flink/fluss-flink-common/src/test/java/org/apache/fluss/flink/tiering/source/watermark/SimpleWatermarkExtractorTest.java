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
import org.apache.fluss.lake.batch.ArrowRecordBatch;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SimpleWatermarkExtractor}. */
class SimpleWatermarkExtractorTest {

    @Test
    void testCreateWithNoWatermarkConfig() {
        TableInfo tableInfo = createTableInfoWithoutWatermark();
        assertThat(SimpleWatermarkExtractor.create(tableInfo)).isNull();
    }

    @ParameterizedTest
    @CsvSource({
        "`ts`,                           1000,      1000",
        "ts,                             2000,      2000",
        "`ts` - INTERVAL '5' SECOND,     10000,     5000",
        "`ts` - INTERVAL '0.001' SECOND, 1000,      999",
        "`ts` - INTERVAL '2' MINUTE,     200000,    80000",
        "`ts` - INTERVAL '1' HOUR,       7200000,   3600000",
        "`ts` - INTERVAL '1' DAY,        172800000, 86400000",
    })
    void testNtzWatermarkExtraction(String expr, long inputMillis, long expectedMillis) {
        TableInfo tableInfo = createTableInfoWithWatermark(expr, "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        GenericRow row = GenericRow.of(1, TimestampNtz.fromMillis(inputMillis));
        assertThat(extractor.currentWatermark(row)).isEqualTo(expectedMillis);
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

    @Test
    void testCurrentWatermarkWithArrowRecordBatchUsesMaximumTimestamp() throws Exception {
        TableInfo tableInfo =
                createTableInfoWithWatermark("`ts` - INTERVAL '5' SECOND", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        try (ArrowRecordBatch recordBatch =
                createArrowRecordBatch(
                        new Object[] {1, TimestampNtz.fromMillis(10_000L)},
                        new Object[] {2, TimestampNtz.fromMillis(30_000L)},
                        new Object[] {3, TimestampNtz.fromMillis(20_000L)})) {
            assertThat(extractor.currentWatermark(recordBatch)).isEqualTo(25_000L);
        }
    }

    @Test
    void testCurrentWatermarkWithArrowRecordBatchSkipsNullRowtime() throws Exception {
        TableInfo tableInfo = createTableInfoWithWatermark("`ts`", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        try (ArrowRecordBatch recordBatch =
                createArrowRecordBatch(
                        new Object[] {1, null},
                        new Object[] {2, TimestampNtz.fromMillis(8_000L)},
                        new Object[] {3, null})) {
            assertThat(extractor.currentWatermark(recordBatch)).isEqualTo(8_000L);
        }
    }

    @Test
    void testCurrentWatermarkWithArrowRecordBatchReturnsNullForAllNullRowtime() throws Exception {
        TableInfo tableInfo = createTableInfoWithWatermark("`ts`", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        try (ArrowRecordBatch recordBatch =
                createArrowRecordBatch(new Object[] {1, null}, new Object[] {2, null})) {
            assertThat(extractor.currentWatermark(recordBatch)).isNull();
        }
    }

    @Test
    void testCurrentWatermarkWithArrowRecordBatchSupportsNegativeWatermark() throws Exception {
        TableInfo tableInfo =
                createTableInfoWithWatermark("`ts` - INTERVAL '5' SECOND", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        try (ArrowRecordBatch recordBatch =
                createArrowRecordBatch(
                        new Object[] {1, TimestampNtz.fromMillis(-10_000L)},
                        new Object[] {2, TimestampNtz.fromMillis(-3_000L)})) {
            assertThat(extractor.currentWatermark(recordBatch)).isEqualTo(-8_000L);
        }
    }

    @Test
    void testCurrentWatermarkWithEmptyArrowRecordBatchReturnsNull() throws Exception {
        TableInfo tableInfo = createTableInfoWithWatermark("`ts`", "TIMESTAMP(3)");
        SimpleWatermarkExtractor extractor = SimpleWatermarkExtractor.create(tableInfo);
        assertThat(extractor).isNotNull();

        try (ArrowRecordBatch recordBatch = createArrowRecordBatch()) {
            assertThat(extractor.currentWatermark(recordBatch)).isNull();
        }
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
        Configuration tableConfig = new Configuration();
        return new TableInfo(
                TablePath.of("test_db", "test_table"),
                1L,
                0,
                schema,
                Collections.emptyList(),
                Collections.emptyList(),
                1,
                tableConfig,
                Configuration.fromMap(customProps),
                null,
                null,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    private static ArrowRecordBatch createArrowRecordBatch(Object[]... rows) {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        IntVector idVector = new IntVector("id", allocator);
        TimeStampMilliVector timestampVector = new TimeStampMilliVector("ts", allocator);
        for (int rowId = 0; rowId < rows.length; rowId++) {
            idVector.setSafe(rowId, (Integer) rows[rowId][0]);
            Object timestamp = rows[rowId][1];
            if (timestamp == null) {
                timestampVector.setNull(rowId);
            } else {
                timestampVector.setSafe(rowId, ((TimestampNtz) timestamp).getMillisecond());
            }
        }
        idVector.setValueCount(rows.length);
        timestampVector.setValueCount(rows.length);
        List<FieldVector> vectors = Arrays.asList(idVector, timestampVector);
        VectorSchemaRoot vectorSchemaRoot =
                new VectorSchemaRoot(
                        Arrays.asList(idVector.getField(), timestampVector.getField()),
                        vectors,
                        rows.length);
        return new TestingArrowRecordBatch(
                new ArrowBatchData(vectorSchemaRoot, 0L, 0L, rows.length), allocator);
    }

    private static class TestingArrowRecordBatch extends ArrowRecordBatch {
        private final BufferAllocator allocator;

        private TestingArrowRecordBatch(ArrowBatchData arrowBatchData, BufferAllocator allocator) {
            super(arrowBatchData);
            this.allocator = allocator;
        }

        @Override
        public void close() {
            super.close();
            allocator.close();
        }
    }
}
