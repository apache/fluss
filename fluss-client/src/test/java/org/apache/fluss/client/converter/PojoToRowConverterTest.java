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

package org.apache.fluss.client.converter;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PojoToRowConverter}. */
public class PojoToRowConverterTest {

    @Test
    public void testNullHandlingToRow() {
        RowType table = ConvertersTestFixtures.fullSchema();
        RowType projection = table;
        PojoToRowConverter<ConvertersTestFixtures.TestPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.TestPojo.class, table, projection);
        assertThat(writer.toRow(null)).isNull();
    }

    @Test
    public void testProjectionSubsetWrites() {
        RowType table = ConvertersTestFixtures.fullSchema();
        RowType projection =
                RowType.builder()
                        .field("booleanField", DataTypes.BOOLEAN())
                        .field("intField", DataTypes.INT())
                        .field("stringField", DataTypes.STRING())
                        .build();

        PojoToRowConverter<ConvertersTestFixtures.TestPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.TestPojo.class, table, projection);
        ConvertersTestFixtures.TestPojo pojo = ConvertersTestFixtures.TestPojo.sample();
        GenericRow row = writer.toRow(pojo);
        assertThat(row.getFieldCount()).isEqualTo(3);
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getInt(1)).isEqualTo(123456);
        assertThat(row.getString(2).toString()).isEqualTo("Hello, World!");
    }

    @Test
    public void testPojoMustExactlyMatchTableSchema() {
        RowType table =
                RowType.builder()
                        .field("booleanField", DataTypes.BOOLEAN())
                        .field("intField", DataTypes.INT())
                        .field("stringField", DataTypes.STRING())
                        .field("extraField", DataTypes.DOUBLE())
                        .build();
        RowType projection = table;
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.PartialTestPojo.class,
                                        table,
                                        projection))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must exactly match table schema");
    }

    @Test
    public void testPojoNoDefaultCtorFails() {
        RowType table = RowType.builder().field("intField", DataTypes.INT()).build();
        RowType proj = table;
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.NoDefaultConstructorPojo.class,
                                        table,
                                        proj))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("public default constructor");
    }

    @Test
    public void testDecimalTypeValidationAtCreation() {
        RowType table = RowType.builder().field("decimalField", DataTypes.DECIMAL(10, 2)).build();
        RowType proj = table;
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.DecimalWrongTypePojo.class,
                                        table,
                                        proj))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type")
                .hasMessageContaining("decimalField");
    }

    @Test
    public void testDateTimeTypeValidationAtCreation() {
        RowType dateSchema = RowType.builder().field("dateField", DataTypes.DATE()).build();
        RowType timeSchema = RowType.builder().field("timeField", DataTypes.TIME()).build();
        RowType tsSchema = RowType.builder().field("timestampField", DataTypes.TIMESTAMP()).build();
        RowType ltzSchema =
                RowType.builder().field("timestampLtzField", DataTypes.TIMESTAMP_LTZ()).build();
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.DateWrongTypePojo.class,
                                        dateSchema,
                                        dateSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type")
                .hasMessageContaining("dateField");
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.TimeWrongTypePojo.class,
                                        timeSchema,
                                        timeSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type")
                .hasMessageContaining("timeField");
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.TimestampWrongTypePojo.class,
                                        tsSchema,
                                        tsSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type")
                .hasMessageContaining("timestampField");
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.TimestampLtzWrongTypePojo.class,
                                        ltzSchema,
                                        ltzSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type")
                .hasMessageContaining("timestampLtzField");
    }

    @Test
    public void testUnsupportedSchemaFieldTypeThrows() {
        RowType table =
                RowType.builder()
                        .field("mapField", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();
        RowType proj = table;
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.MapPojo.class, table, proj))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported field type")
                .hasMessageContaining("MAP")
                .hasMessageContaining("mapField");
    }
}
