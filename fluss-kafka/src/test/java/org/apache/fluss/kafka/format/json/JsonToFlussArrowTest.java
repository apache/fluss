/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.format.json;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.arrow.ArrowReader;
import org.apache.fluss.row.arrow.writers.ArrowFieldWriter;
import org.apache.fluss.row.columnar.ColumnVector;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies converted JSON values through the Fluss Arrow writers and readers. */
class JsonToFlussArrowTest {
    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testBinaryPadsAndTruncatesWithoutCorruptingAdjacentValues(boolean handleSafe)
            throws Exception {
        RowType schema = DataTypes.ROW(DataTypes.BINARY(2));
        JsonToFlussConverter converter = JsonToFlussConverters.create(DataTypes.BINARY(2));
        String[] json = {"\"AQID\"", "\"AQ==\"", "\"\"", "null", "\"AwQ=\""};
        byte[][] expected = {
            new byte[] {1, 2}, new byte[] {1, 0}, new byte[] {0, 0}, null, new byte[] {3, 4}
        };
        try (RootAllocator allocator = new RootAllocator(1024 * 1024);
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(schema), allocator)) {
            root.allocateNew();
            ArrowFieldWriter writer =
                    ArrowUtils.createArrowFieldWriter(root.getVector(0), schema.getTypeAt(0));
            for (int i = 0; i < json.length; i++) {
                Object value = converter.convert(MAPPER.readTree(json[i]), "$[\"binary\"]");
                writer.write(i, GenericRow.of(value), 0, handleSafe);
            }
            root.setRowCount(json.length);
            ArrowReader reader = reader(root, schema);
            for (int i = 0; i < expected.length; i++) {
                ColumnarRow row = reader.read(i);
                if (expected[i] == null) {
                    assertThat(row.isNullAt(0)).isTrue();
                } else {
                    assertThat(row.getBinary(0, 2)).containsExactly(expected[i]);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNestedContainersNullsAndNormalizedScalarsRoundTrip(boolean handleSafe)
            throws Exception {
        RowType valueType =
                DataTypes.ROW(
                        DataTypes.FIELD("payload", DataTypes.BINARY(2)),
                        DataTypes.FIELD("amounts", DataTypes.ARRAY(DataTypes.DECIMAL(5, 2))),
                        DataTypes.FIELD("time", DataTypes.TIME(3)),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(6)));
        RowType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.CHAR(1)),
                        DataTypes.FIELD(
                                "groups",
                                DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), valueType))));
        String[] json = {
            "{\"name\":\"full value\",\"groups\":[{\"a\":{\"payload\":\"AQ==\","
                    + "\"amounts\":[12.345,null],\"time\":\"12:34:56.123456\","
                    + "\"ts\":\"1969-12-31 23:59:59.999999999\"},\"empty\":null},null,{}]}",
            "{\"name\":\"second\",\"groups\":null}",
            "{\"name\":\"third\",\"groups\":[]}"
        };
        JsonToFlussConverter converter = JsonToFlussConverters.create(schema);
        try (RootAllocator allocator = new RootAllocator(8 * 1024 * 1024);
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(schema), allocator)) {
            root.allocateNew();
            ArrowFieldWriter[] writers = new ArrowFieldWriter[schema.getFieldCount()];
            for (int field = 0; field < writers.length; field++) {
                writers[field] =
                        ArrowUtils.createArrowFieldWriter(
                                root.getVector(field), schema.getTypeAt(field));
            }
            for (int i = 0; i < json.length; i++) {
                GenericRow row = (GenericRow) converter.convert(MAPPER.readTree(json[i]), "$");
                for (int field = 0; field < writers.length; field++) {
                    writers[field].write(i, row, field, handleSafe);
                }
            }
            root.setRowCount(json.length);
            ArrowReader reader = reader(root, schema);
            ColumnarRow first = reader.read(0);
            assertThat(first.getChar(0, 1).toString()).isEqualTo("full value");
            InternalArray groups = first.getArray(1);
            assertThat(groups.size()).isEqualTo(3);
            assertThat(groups.isNullAt(1)).isTrue();
            assertThat(groups.getMap(2).size()).isZero();
            InternalMap entries = groups.getMap(0);
            assertThat(entries.size()).isEqualTo(2);
            assertThat(entries.keyArray().getString(0).toString()).isEqualTo("a");
            assertThat(entries.keyArray().getString(1).toString()).isEqualTo("empty");
            assertThat(entries.valueArray().isNullAt(1)).isTrue();
            InternalRow value = entries.valueArray().getRow(0, 4);
            assertThat(value.getBinary(0, 2)).containsExactly(1, 0);
            assertThat(value.getArray(1).getDecimal(0, 5, 2).toBigDecimal())
                    .isEqualTo(new BigDecimal("12.35"));
            assertThat(value.getArray(1).isNullAt(1)).isTrue();
            assertThat(value.getInt(2)).isEqualTo(45_296_123);
            assertThat(value.getTimestampNtz(3, 6))
                    .isEqualTo(
                            TimestampNtz.fromLocalDateTime(
                                    LocalDateTime.parse("1969-12-31T23:59:59.999999")));
            assertThat(reader.read(1).isNullAt(1)).isTrue();
            assertThat(reader.read(2).getArray(1).size()).isZero();
        }
    }

    private static ArrowReader reader(VectorSchemaRoot root, RowType schema) {
        ColumnVector[] columns = new ColumnVector[schema.getFieldCount()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = ArrowUtils.createArrowColumnVector(root.getVector(i), schema.getTypeAt(i));
        }
        return new ArrowReader(columns, root.getRowCount());
    }
}
