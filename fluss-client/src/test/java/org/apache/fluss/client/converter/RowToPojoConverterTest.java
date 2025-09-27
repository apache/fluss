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

/** Tests for {@link RowToPojoConverter}. */
public class RowToPojoConverterTest {

    @Test
    public void testRoundTripFullSchema() {
        RowType table = ConvertersTestFixtures.fullSchema();
        RowType projection = table;

        PojoToRowConverter<ConvertersTestFixtures.TestPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.TestPojo.class, table, projection);
        RowToPojoConverter<ConvertersTestFixtures.TestPojo> scanner =
                RowToPojoConverter.of(ConvertersTestFixtures.TestPojo.class, table, projection);

        ConvertersTestFixtures.TestPojo pojo = ConvertersTestFixtures.TestPojo.sample();
        GenericRow row = writer.toRow(pojo);
        assertThat(row.getFieldCount()).isEqualTo(15);

        ConvertersTestFixtures.TestPojo back = scanner.fromRow(row);
        assertThat(back).isEqualTo(pojo);
    }

    @Test
    public void testNullHandlingFromRow() {
        RowType table = ConvertersTestFixtures.fullSchema();
        RowType projection = table;
        RowToPojoConverter<ConvertersTestFixtures.TestPojo> scanner =
                RowToPojoConverter.of(ConvertersTestFixtures.TestPojo.class, table, projection);
        assertThat(scanner.fromRow(null)).isNull();
    }

    @Test
    public void testProjectionSubsetReads() {
        RowType table = ConvertersTestFixtures.fullSchema();
        RowType projection =
                RowType.builder()
                        .field("booleanField", DataTypes.BOOLEAN())
                        .field("intField", DataTypes.INT())
                        .field("stringField", DataTypes.STRING())
                        .build();

        PojoToRowConverter<ConvertersTestFixtures.TestPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.TestPojo.class, table, projection);
        RowToPojoConverter<ConvertersTestFixtures.TestPojo> scanner =
                RowToPojoConverter.of(ConvertersTestFixtures.TestPojo.class, table, projection);

        ConvertersTestFixtures.TestPojo pojo = ConvertersTestFixtures.TestPojo.sample();
        GenericRow row = writer.toRow(pojo);
        assertThat(row.getFieldCount()).isEqualTo(3);
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getInt(1)).isEqualTo(123456);
        assertThat(row.getString(2).toString()).isEqualTo("Hello, World!");

        ConvertersTestFixtures.TestPojo back = scanner.fromRow(row);
        assertThat(back.booleanField).isTrue();
        assertThat(back.intField).isEqualTo(123456);
        assertThat(back.stringField).isEqualTo("Hello, World!");
        // non-projected remain null
        assertThat(back.byteField).isNull();
        assertThat(back.shortField).isNull();
        assertThat(back.longField).isNull();
        assertThat(back.floatField).isNull();
        assertThat(back.doubleField).isNull();
        assertThat(back.bytesField).isNull();
        assertThat(back.decimalField).isNull();
        assertThat(back.dateField).isNull();
        assertThat(back.timeField).isNull();
        assertThat(back.timestampField).isNull();
        assertThat(back.timestampLtzField).isNull();
        assertThat(back.offsetDateTimeField).isNull();
    }

    @Test
    public void testFromRowThrowsWhenDefaultConstructorThrows() {
        RowType table = RowType.builder().field("intField", DataTypes.INT()).build();
        RowType proj = table;
        RowToPojoConverter<ConvertersTestFixtures.ThrowingCtorPojo> scanner =
                RowToPojoConverter.of(ConvertersTestFixtures.ThrowingCtorPojo.class, table, proj);
        GenericRow row = new GenericRow(1);
        row.setField(0, 1);
        assertThatThrownBy(() -> scanner.fromRow(row))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to instantiate POJO class")
                .hasMessageContaining(ConvertersTestFixtures.ThrowingCtorPojo.class.getName());
    }

    @Test
    public void testCharacterFieldRoundTrip() {
        RowType table = RowType.builder().field("charField", DataTypes.STRING()).build();
        RowType proj = table;
        PojoToRowConverter<ConvertersTestFixtures.CharacterFieldPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.CharacterFieldPojo.class, table, proj);
        RowToPojoConverter<ConvertersTestFixtures.CharacterFieldPojo> scanner =
                RowToPojoConverter.of(ConvertersTestFixtures.CharacterFieldPojo.class, table, proj);
        ConvertersTestFixtures.CharacterFieldPojo pojo =
                new ConvertersTestFixtures.CharacterFieldPojo('A');
        GenericRow row = writer.toRow(pojo);
        assertThat(row.getString(0).toString()).isEqualTo("A");
        ConvertersTestFixtures.CharacterFieldPojo back = scanner.fromRow(row);
        assertThat(back.charField).isEqualTo('A');
    }
}
