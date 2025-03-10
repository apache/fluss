/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.spark;

import com.alibaba.fluss.connector.spark.utils.SparkTypeUtils;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.DateTimeUtils;

import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.stream.Collectors;

import scala.Function1;
import scala.collection.JavaConverters;

import static com.alibaba.fluss.row.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SparkInternalRow}. */
public class SparkInternalRowTest {
    public static final RowType ALL_TYPES =
            RowType.builder(true) // posX and posY have field id 0 and 1, here we start from 2
                    .field("id", DataTypes.INT().copy(false))
                    .field("name", DataTypes.STRING()) /* optional by default */
                    .field("char", DataTypes.CHAR(10))
                    .field("varchar", DataTypes.STRING())
                    .field("salary", DataTypes.DOUBLE().copy(false))
                    .field("boolean", DataTypes.BOOLEAN().copy(false))
                    .field("tinyint", DataTypes.TINYINT())
                    .field("smallint", DataTypes.SMALLINT())
                    .field("bigint", DataTypes.BIGINT())
                    .field("bytes", DataTypes.BYTES())
                    .field("timestamp", DataTypes.TIMESTAMP_LTZ())
                    .field("timestamp_ntz", DataTypes.TIMESTAMP())
                    .field("date", DataTypes.DATE())
                    .field("decimal", DataTypes.DECIMAL(2, 2))
                    .field("decimal2", DataTypes.DECIMAL(38, 2))
                    .field("decimal3", DataTypes.DECIMAL(10, 1))
                    .build();

    @Test
    public void test() {
        TimeZone tz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        InternalRow rowData =
                GenericRow.of(
                        1,
                        fromString("hzy"),
                        fromString("alibaba"),
                        fromString("fluss"),
                        22.2,
                        true,
                        (byte) 22,
                        (short) 356,
                        23567222L,
                        "varbinary_v".getBytes(StandardCharsets.UTF_8),
                        TimestampLtz.fromInstant(
                                LocalDateTime.parse("2007-12-03T10:15:30")
                                        .toInstant(ZoneOffset.UTC)),
                        TimestampNtz.fromLocalDateTime(LocalDateTime.parse("2007-12-03T10:15:30")),
                        DateTimeUtils.toInternal(LocalDate.parse("2022-05-02")),
                        Decimal.fromBigDecimal(BigDecimal.valueOf(0.21), 2, 2),
                        Decimal.fromBigDecimal(BigDecimal.valueOf(65782123123.01), 38, 2),
                        Decimal.fromBigDecimal(BigDecimal.valueOf(62123123.5), 10, 1));

        // CatalystTypeConverters does not support char and varchar, we need to replace char and
        // varchar with string
        Function1<Object, Object> sparkConverter =
                CatalystTypeConverters.createToScalaConverter(
                        CharVarcharUtils.replaceCharVarcharWithString(
                                SparkTypeUtils.fromFlussType(ALL_TYPES)));
        org.apache.spark.sql.Row sparkRow =
                (org.apache.spark.sql.Row)
                        sparkConverter.apply(new SparkInternalRow(ALL_TYPES).replace(rowData));

        String expected =
                "1,"
                        + "hzy,"
                        + "alibaba,"
                        + "fluss,"
                        + "22.2,"
                        + "true,"
                        + "22,"
                        + "356,"
                        + "23567222,"
                        + "[B@,"
                        + "2007-12-03 18:15:30.0,"
                        + "2007-12-03T10:15:30,"
                        + "2022-05-02,"
                        + "0.21,"
                        + "65782123123.01,"
                        + "62123123.5";
        assertThat(sparkRowToString(sparkRow)).isEqualTo(expected);

        SparkRow sparkRowData = new SparkRow(ALL_TYPES, sparkRow);
        sparkRow =
                (org.apache.spark.sql.Row)
                        sparkConverter.apply(new SparkInternalRow(ALL_TYPES).replace(sparkRowData));
        assertThat(sparkRowToString(sparkRow)).isEqualTo(expected);
        TimeZone.setDefault(tz);
    }

    private String sparkRowToString(org.apache.spark.sql.Row row) {
        return JavaConverters.seqAsJavaList(row.toSeq()).stream()
                .map(
                        x ->
                                (x instanceof scala.collection.Seq)
                                        ? JavaConverters.seqAsJavaList(
                                                (scala.collection.Seq<Object>) x)
                                        : x)
                .map(Object::toString)
                // Since the toString result of Spark's binary col is unstable, replace it
                .map(x -> x.startsWith("[B@") ? "[B@" : x)
                .collect(Collectors.joining(","));
    }
}
