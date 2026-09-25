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

package org.apache.fluss.trino;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies actual values written into Trino blocks. */
final class FlussRowDecoderTest {
    @Test
    void testPrimitiveValuesAndOwnedBinaryCopy() {
        Schema schema =
                Schema.newBuilder()
                        .column("b", DataTypes.BOOLEAN())
                        .column("tiny", DataTypes.TINYINT())
                        .column("small", DataTypes.SMALLINT())
                        .column("i", DataTypes.INT())
                        .column("l", DataTypes.BIGINT())
                        .column("f", DataTypes.FLOAT())
                        .column("d", DataTypes.DOUBLE())
                        .column("bytes", DataTypes.BYTES())
                        .column("binary", DataTypes.BINARY(2))
                        .build();
        List<FlussColumnHandle> columns = new ArrayList<>();
        for (int i = 0; i < schema.getColumns().size(); i++) {
            columns.add(new FlussColumnHandle(schema.getColumns().get(i).getName(), i));
        }
        FlussRowDecoder decoder = new FlussRowDecoder(schema, columns);
        PageBuilder builder = new PageBuilder(decoder.getTypes());
        byte[] bytes = new byte[] {0, (byte) 255};
        decoder.append(
                GenericRow.of(
                        true,
                        Byte.MIN_VALUE,
                        Short.MAX_VALUE,
                        Integer.MIN_VALUE,
                        Long.MAX_VALUE,
                        -0.0f,
                        Double.NEGATIVE_INFINITY,
                        bytes,
                        bytes),
                builder);
        bytes[0] = 99;
        Page page = builder.build();
        assertThat(BOOLEAN.getBoolean(page.getBlock(0), 0)).isTrue();
        assertThat(TINYINT.getLong(page.getBlock(1), 0)).isEqualTo(Byte.MIN_VALUE);
        assertThat(SMALLINT.getLong(page.getBlock(2), 0)).isEqualTo(Short.MAX_VALUE);
        assertThat(INTEGER.getLong(page.getBlock(3), 0)).isEqualTo(Integer.MIN_VALUE);
        assertThat(BIGINT.getLong(page.getBlock(4), 0)).isEqualTo(Long.MAX_VALUE);
        assertThat(REAL.getLong(page.getBlock(5), 0)).isEqualTo(Float.floatToRawIntBits(-0.0f));
        assertThat(DOUBLE.getDouble(page.getBlock(6), 0)).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(VARBINARY.getSlice(page.getBlock(7), 0).getBytes())
                .containsExactly(0, (byte) 255);
        assertThat(VARBINARY.getSlice(page.getBlock(8), 0).getBytes())
                .containsExactly(0, (byte) 255);
    }

    @Test
    void testFloatingPointSpecialValues() {
        Schema schema =
                Schema.newBuilder()
                        .column("f", DataTypes.FLOAT())
                        .column("d", DataTypes.DOUBLE())
                        .build();
        FlussRowDecoder decoder =
                new FlussRowDecoder(
                        schema,
                        Arrays.asList(
                                new FlussColumnHandle("f", 0), new FlussColumnHandle("d", 1)));
        PageBuilder builder = new PageBuilder(decoder.getTypes());
        decoder.append(GenericRow.of(Float.NaN, Double.NaN), builder);
        decoder.append(GenericRow.of(Float.POSITIVE_INFINITY, -0.0d), builder);
        Page page = builder.build();
        assertThat(Float.intBitsToFloat((int) REAL.getLong(page.getBlock(0), 0))).isNaN();
        assertThat(DOUBLE.getDouble(page.getBlock(1), 0)).isNaN();
        assertThat(Float.intBitsToFloat((int) REAL.getLong(page.getBlock(0), 1)))
                .isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(Double.doubleToRawLongBits(DOUBLE.getDouble(page.getBlock(1), 1)))
                .isEqualTo(Double.doubleToRawLongBits(-0.0d));
    }

    @Test
    void testReorderedColumnsAndNull() {
        Schema schema =
                Schema.newBuilder()
                        .column("ID", DataTypes.BIGINT())
                        .column("Name", DataTypes.STRING())
                        .build();

        FlussRowDecoder decoder =
                new FlussRowDecoder(
                        schema,
                        Arrays.asList(
                                new FlussColumnHandle("Name", 1), new FlussColumnHandle("ID", 0)));

        PageBuilder builder = new PageBuilder(decoder.getTypes());

        decoder.append(GenericRow.of(42L, BinaryString.fromString("世界")), builder);
        decoder.append(GenericRow.of(null, null), builder);

        Page page = builder.build();

        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8()).isEqualTo("世界");
        assertThat(BIGINT.getLong(page.getBlock(1), 0)).isEqualTo(42);
        assertThat(page.getBlock(0).isNull(1)).isTrue();
        assertThat(page.getBlock(1).isNull(1)).isTrue();
    }

    @Test
    void testCharDecimalAndTemporalValues() {
        assertThat(value(DataTypes.CHAR(5), BinaryString.fromString("世界   ")))
                .isEqualTo(utf8Slice("世界"));
        assertThat(
                        value(
                                DataTypes.DECIMAL(18, 2),
                                Decimal.fromBigDecimal(
                                        new BigDecimal("-9999999999999999.99"), 18, 2)))
                .isEqualTo(-999999999999999999L);
        assertThat(
                        value(
                                DataTypes.DECIMAL(38, 9),
                                Decimal.fromBigDecimal(
                                        new BigDecimal("12345678901234567890123456789.123456789"),
                                        38,
                                        9)))
                .isEqualTo(
                        Int128.valueOf(new BigInteger("12345678901234567890123456789123456789")));
        assertThat(value(DataTypes.DATE(), -1)).isEqualTo(-1L);
        assertThat(value(DataTypes.TIME(3), 86399999)).isEqualTo(86399999000000000L);
        assertThat(value(DataTypes.TIMESTAMP(6), TimestampNtz.fromMillis(-1, 999000)))
                .isEqualTo(-1L);
        assertThat(value(DataTypes.TIMESTAMP(9), TimestampNtz.fromMillis(-1, 999999)))
                .isEqualTo(new LongTimestamp(-1, 999000));
        assertThat(value(DataTypes.TIMESTAMP_LTZ(3), TimestampLtz.fromEpochMillis(-1)))
                .isEqualTo(packDateTimeWithZone(-1, UTC_KEY));
        assertThat(value(DataTypes.TIMESTAMP_LTZ(9), TimestampLtz.fromEpochMillis(-1, 999999)))
                .isEqualTo(
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                                -1, 999999000, UTC_KEY));
    }

    @Test
    void testDecimalAndTemporalBoundaries() {
        assertThat(
                        value(
                                DataTypes.DECIMAL(38, 0),
                                Decimal.fromBigDecimal(
                                        new BigDecimal("-99999999999999999999999999999999999999"),
                                        38,
                                        0)))
                .isEqualTo(
                        Int128.valueOf(new BigInteger("-99999999999999999999999999999999999999")));
        assertThat(value(DataTypes.DECIMAL(18, 18), Decimal.fromUnscaledLong(1, 18, 18)))
                .isEqualTo(1L);
        assertThat(value(DataTypes.TIME(0), 0)).isEqualTo(0L);
        assertThat(value(DataTypes.TIME(9), 12345)).isEqualTo(12345000000000L);
        assertThat(value(DataTypes.TIMESTAMP(3), TimestampNtz.fromMillis(-1001)))
                .isEqualTo(-1001000L);
        assertThat(value(DataTypes.TIMESTAMP(9), TimestampNtz.fromMillis(1, 123456)))
                .isEqualTo(new LongTimestamp(1123, 456000));
        assertThat(value(DataTypes.TIMESTAMP_LTZ(6), TimestampLtz.fromEpochMillis(1, 123000)))
                .isEqualTo(
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                                1, 123000000, UTC_KEY));
        assertThat(value(DataTypes.CHAR(3), BinaryString.fromString("   "))).isEqualTo(EMPTY_SLICE);
        assertThat(value(DataTypes.CHAR(CharType.MAX_LENGTH + 1), BinaryString.fromString("x  ")))
                .isEqualTo(utf8Slice("x  "));
    }

    @Test
    void testNullsForAllAdditionalTypes() {
        for (DataType dataType :
                Arrays.asList(
                        DataTypes.CHAR(5),
                        DataTypes.DECIMAL(10, 2),
                        DataTypes.DECIMAL(38, 9),
                        DataTypes.DATE(),
                        DataTypes.TIME(3),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP(9),
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.TIMESTAMP_LTZ(9),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.ROW(DataTypes.FIELD("v", DataTypes.INT())))) {
            assertThat(value(dataType, null)).isNull();
        }
    }

    @Test
    void testEncodedRows() throws Exception {
        DataType[] dataTypes = {
            DataTypes.CHAR(8),
            DataTypes.BINARY(3),
            DataTypes.DECIMAL(38, 9),
            DataTypes.TIMESTAMP(9),
            DataTypes.TIMESTAMP_LTZ(9),
            DataTypes.ARRAY(DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT())))
        };
        Object[] values = {
            BinaryString.fromString("世界"),
            new byte[] {1, 2, 3},
            Decimal.fromBigDecimal(new BigDecimal("12345678901234567890.123456789"), 38, 9),
            TimestampNtz.fromMillis(-1, 999999),
            TimestampLtz.fromEpochMillis(-1, 999999),
            GenericArray.of(GenericRow.of(17), null)
        };
        Schema.Builder schema = Schema.newBuilder();
        List<FlussColumnHandle> columns = new ArrayList<>();
        for (int i = 0; i < dataTypes.length; i++) {
            schema.column("v" + i, dataTypes[i]);
            columns.add(new FlussColumnHandle("v" + i, i));
        }
        FlussRowDecoder decoder = new FlussRowDecoder(schema.build(), columns);
        for (KvFormat format : Arrays.asList(KvFormat.INDEXED, KvFormat.COMPACTED)) {
            try (RowEncoder encoder = RowEncoder.create(format, dataTypes)) {
                encoder.startNewRow();
                for (int i = 0; i < values.length; i++) {
                    encoder.encodeField(i, values[i]);
                }
                PageBuilder builder = new PageBuilder(decoder.getTypes());
                decoder.append(encoder.finishRow(), builder);
                Page page = builder.build();
                for (int i = 0; i < 5; i++) {
                    assertThat(readNativeValue(decoder.getTypes().get(i), page.getBlock(i), 0))
                            .isEqualTo(value(dataTypes[i], values[i]));
                }
                ArrayType arrayType = (ArrayType) decoder.getTypes().get(5);
                Block array = arrayType.getObject(page.getBlock(5), 0);
                SqlRow row = ((RowType) arrayType.getElementType()).getObject(array, 0);
                assertThat(INTEGER.getLong(row.getRawFieldBlock(0), row.getRawIndex()))
                        .isEqualTo(17L);
                assertThat(array.isNull(1)).isTrue();
            }
        }
    }

    @Test
    void testNestedValuesAndEmptyContainers() {
        DataType nested =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "items",
                                DataTypes.ARRAY(
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))),
                        DataTypes.FIELD("missing", DataTypes.STRING()));
        SqlRow row =
                (SqlRow)
                        value(
                                nested,
                                GenericRow.of(
                                        GenericArray.of(
                                                GenericMap.of(
                                                        BinaryString.fromString("世界"),
                                                        7,
                                                        BinaryString.fromString("null"),
                                                        null),
                                                null,
                                                new GenericMap(Collections.emptyMap())),
                                        null));
        RowType rowType = (RowType) FlussTypeConverter.toTrinoType(nested);
        ArrayType arrayType = (ArrayType) rowType.getTypeParameters().get(0);
        Block array = arrayType.getObject(row.getRawFieldBlock(0), row.getRawIndex());
        MapType mapType = (MapType) arrayType.getElementType();
        SqlMap map = mapType.getObject(array, 0);
        assertThat(map.getSize()).isEqualTo(2);
        Map<String, Long> actual = new HashMap<>();
        for (int i = 0; i < map.getSize(); i++) {
            int position = map.getRawOffset() + i;
            actual.put(
                    VARCHAR.getSlice(map.getRawKeyBlock(), position).toStringUtf8(),
                    map.getRawValueBlock().isNull(position)
                            ? null
                            : INTEGER.getLong(map.getRawValueBlock(), position));
        }
        assertThat(actual).containsEntry("世界", 7L).containsEntry("null", null);
        assertThat(array.isNull(1)).isTrue();
        assertThat(mapType.getObject(array, 2).getSize()).isZero();
        assertThat(row.getRawFieldBlock(1).isNull(row.getRawIndex())).isTrue();
        assertThat(
                        ((Block) value(DataTypes.ARRAY(DataTypes.INT()), GenericArray.of()))
                                .getPositionCount())
                .isZero();
        assertThat(value(nested, null)).isNull();
    }

    @Test
    void testRejectsNullMapKeys() {
        assertThatThrownBy(
                        () ->
                                value(
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                        new GenericMap(Collections.singletonMap(null, 1))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("null map key");
    }

    private Object value(DataType dataType, Object value) {
        FlussRowDecoder decoder =
                new FlussRowDecoder(
                        Schema.newBuilder().column("v", dataType).build(),
                        Collections.singletonList(new FlussColumnHandle("v", 0)));
        PageBuilder builder = new PageBuilder(decoder.getTypes());
        decoder.append(GenericRow.of(value), builder);
        Block block = builder.build().getBlock(0);
        return readNativeValue(decoder.getTypes().get(0), block, 0);
    }

    @Test
    void testRejectsStaleColumn() {
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        assertThatThrownBy(
                        () ->
                                new FlussRowDecoder(
                                        schema,
                                        Collections.singletonList(
                                                new FlussColumnHandle("other", 0))))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
