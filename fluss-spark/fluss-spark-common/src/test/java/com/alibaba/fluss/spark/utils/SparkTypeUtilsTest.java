/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.spark.utils;

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.types.DataTypes;

import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkTypeUtilsTest {

    @Test
    void testTypeConversion() {
        // fluss columns
        List<Schema.Column> flussColumns =
                Arrays.asList(
                        new Schema.Column("a", DataTypes.BOOLEAN().copy(false), null),
                        new Schema.Column("b", DataTypes.TINYINT().copy(false), null),
                        new Schema.Column("c", DataTypes.SMALLINT(), "comment1"),
                        new Schema.Column("d", DataTypes.INT(), "comment2"),
                        new Schema.Column("e", DataTypes.BIGINT(), null),
                        new Schema.Column("f", DataTypes.FLOAT(), null),
                        new Schema.Column("g", DataTypes.DOUBLE(), null),
                        new Schema.Column("h", DataTypes.CHAR(1), null),
                        new Schema.Column("i", DataTypes.STRING(), null),
                        new Schema.Column("j", DataTypes.DECIMAL(10, 2), null),
                        new Schema.Column("k", DataTypes.BYTES(), null),
                        new Schema.Column("l", DataTypes.DATE(), null),
                        new Schema.Column("m", DataTypes.TIMESTAMP_LTZ(6), null),
                        new Schema.Column("n", DataTypes.TIMESTAMP(6), null));

        // spark columns
        List<StructField> sparkColumns =
                Arrays.asList(
                        new StructField(
                                "a",
                                org.apache.spark.sql.types.DataTypes.BooleanType,
                                false,
                                Metadata.empty()),
                        new StructField(
                                "b",
                                org.apache.spark.sql.types.DataTypes.ByteType,
                                false,
                                Metadata.empty()),
                        new StructField(
                                        "c",
                                        org.apache.spark.sql.types.DataTypes.ShortType,
                                        true,
                                        Metadata.empty())
                                .withComment("comment1"),
                        new StructField(
                                        "d",
                                        org.apache.spark.sql.types.DataTypes.IntegerType,
                                        true,
                                        Metadata.empty())
                                .withComment("comment2"),
                        new StructField(
                                "e",
                                org.apache.spark.sql.types.DataTypes.LongType,
                                true,
                                Metadata.empty()),
                        new StructField(
                                "f",
                                org.apache.spark.sql.types.DataTypes.FloatType,
                                true,
                                Metadata.empty()),
                        new StructField(
                                "g",
                                org.apache.spark.sql.types.DataTypes.DoubleType,
                                true,
                                Metadata.empty()),
                        new StructField(
                                "h",
                                new org.apache.spark.sql.types.CharType(1),
                                true,
                                Metadata.empty()),
                        new StructField(
                                "i",
                                org.apache.spark.sql.types.DataTypes.StringType,
                                true,
                                Metadata.empty()),
                        new StructField(
                                "j",
                                org.apache.spark.sql.types.DataTypes.createDecimalType(10, 2),
                                true,
                                Metadata.empty()),
                        new StructField(
                                "k",
                                org.apache.spark.sql.types.DataTypes.BinaryType,
                                true,
                                Metadata.empty()),
                        new StructField(
                                "l",
                                org.apache.spark.sql.types.DataTypes.DateType,
                                true,
                                Metadata.empty()),
                        new StructField(
                                "m",
                                org.apache.spark.sql.types.DataTypes.TimestampType,
                                true,
                                Metadata.empty()),
                        new StructField(
                                "n",
                                org.apache.spark.sql.types.DataTypes.TimestampNTZType,
                                true,
                                Metadata.empty()));

        // test from fluss columns to spark columns
        List<StructField> actualSparkColumns = new ArrayList<>();
        for (Schema.Column flussColumn : flussColumns) {
            StructField field =
                    new StructField(
                            flussColumn.getName(),
                            SparkTypeUtils.fromFlussType(flussColumn.getDataType()),
                            flussColumn.getDataType().isNullable(),
                            Metadata.empty());
            if (flussColumn.getComment().isPresent()) {
                field.withComment(flussColumn.getComment().get());
            }
            actualSparkColumns.add(field);
        }
        assertThat(actualSparkColumns.toString()).isEqualTo(sparkColumns.toString());

        // test from spark columns to fluss columns
        List<Schema.Column> actualFlussColumns = new ArrayList<>();
        for (StructField sparkColumn : sparkColumns) {
            actualFlussColumns.add(
                    new Schema.Column(
                            sparkColumn.name(),
                            SparkTypeUtils.toFlussType(sparkColumn.dataType())
                                    .copy(sparkColumn.nullable()),
                            sparkColumn.getComment().getOrElse(() -> null)));
        }
        assertThat(actualFlussColumns).isEqualTo(flussColumns);

        // test TIME type
        assertThat(SparkTypeUtils.fromFlussType(DataTypes.TIME()))
                .isEqualTo(org.apache.spark.sql.types.DataTypes.IntegerType);
    }
}
