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

import java.util.HashMap;
import java.util.Map;

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
        assertThat(row.getFieldCount()).isEqualTo(17);

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

    @Test
    public void testMapType() {
        RowType table =
                RowType.builder()
                        .field("mapField", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();

        PojoToRowConverter<MapPojo> writer = PojoToRowConverter.of(MapPojo.class, table, table);
        RowToPojoConverter<MapPojo> reader = RowToPojoConverter.of(MapPojo.class, table, table);

        MapPojo pojo = MapPojo.sample();
        GenericRow row = writer.toRow(pojo);
        MapPojo back = reader.fromRow(row);

        // Verify map field
        Map<Object, Object> mapField = back.mapField;
        assertThat(mapField.size()).isEqualTo(2);
        assertThat(mapField).containsEntry("test_1", 1);
        assertThat(mapField).containsEntry("test_2", 2);
    }

    @Test
    public void testNullMapField() {
        RowType table =
                RowType.builder()
                        .field("mapField", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();

        PojoToRowConverter<MapPojo> writer = PojoToRowConverter.of(MapPojo.class, table, table);
        RowToPojoConverter<MapPojo> reader = RowToPojoConverter.of(MapPojo.class, table, table);

        MapPojo pojo = new MapPojo();
        pojo.mapField = null;

        GenericRow row = writer.toRow(pojo);
        assertThat(row.isNullAt(0)).isTrue();

        MapPojo back = reader.fromRow(row);
        assertThat(back.mapField).isNull();
    }

    @Test
    public void testMapWithNullValues() {
        RowType table =
                RowType.builder()
                        .field("mapField", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();

        PojoToRowConverter<MapPojo> writer = PojoToRowConverter.of(MapPojo.class, table, table);
        RowToPojoConverter<MapPojo> reader = RowToPojoConverter.of(MapPojo.class, table, table);

        MapPojo pojo = new MapPojo();
        pojo.mapField = new HashMap<>();
        pojo.mapField.put("a", 1);
        pojo.mapField.put("b", null);

        GenericRow row = writer.toRow(pojo);
        MapPojo back = reader.fromRow(row);

        assertThat(back.mapField).containsEntry("a", 1);
        assertThat(back.mapField).containsKey("b");
        assertThat(back.mapField.get("b")).isNull();
    }

    @Test
    public void testEmptyMap() {
        RowType table =
                RowType.builder()
                        .field("mapField", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();

        PojoToRowConverter<MapPojo> writer = PojoToRowConverter.of(MapPojo.class, table, table);
        RowToPojoConverter<MapPojo> reader = RowToPojoConverter.of(MapPojo.class, table, table);

        MapPojo pojo = new MapPojo();
        pojo.mapField = new HashMap<>();

        GenericRow row = writer.toRow(pojo);
        MapPojo back = reader.fromRow(row);

        assertThat(back.mapField).isEmpty();
    }

    /** POJO for testing map type. */
    public static class MapPojo {
        public Map<Object, Object> mapField;

        public MapPojo() {}

        public static MapPojo sample() {
            MapPojo pojo = new MapPojo();
            pojo.mapField =
                    new HashMap<Object, Object>() {
                        {
                            put("test_1", 1);
                            put("test_2", 2);
                        }
                    };
            return pojo;
        }
    }

    // ==================== Nested ROW Round-Trip Tests ====================

    @Test
    public void testSimpleNestedRowRoundTrip() {
        RowType table =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field(
                                "address",
                                DataTypes.ROW(
                                        DataTypes.FIELD("city", DataTypes.STRING()),
                                        DataTypes.FIELD("zipCode", DataTypes.INT())))
                        .build();

        PojoToRowConverter<ConvertersTestFixtures.PersonPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.PersonPojo.class, table, table);
        RowToPojoConverter<ConvertersTestFixtures.PersonPojo> reader =
                RowToPojoConverter.of(ConvertersTestFixtures.PersonPojo.class, table, table);

        ConvertersTestFixtures.PersonPojo pojo = new ConvertersTestFixtures.PersonPojo();
        pojo.id = 1;
        pojo.address = new ConvertersTestFixtures.AddressPojo();
        pojo.address.city = "Beijing";
        pojo.address.zipCode = 100000;

        GenericRow row = writer.toRow(pojo);
        ConvertersTestFixtures.PersonPojo back = reader.fromRow(row);

        assertThat(back.id).isEqualTo(1);
        assertThat(back.address).isNotNull();
        assertThat(back.address.city).isEqualTo("Beijing");
        assertThat(back.address.zipCode).isEqualTo(100000);
    }

    @Test
    public void testNullNestedRowRoundTrip() {
        RowType table =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field(
                                "address",
                                DataTypes.ROW(
                                        DataTypes.FIELD("city", DataTypes.STRING()),
                                        DataTypes.FIELD("zipCode", DataTypes.INT())))
                        .build();

        PojoToRowConverter<ConvertersTestFixtures.PersonPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.PersonPojo.class, table, table);
        RowToPojoConverter<ConvertersTestFixtures.PersonPojo> reader =
                RowToPojoConverter.of(ConvertersTestFixtures.PersonPojo.class, table, table);

        ConvertersTestFixtures.PersonPojo pojo = new ConvertersTestFixtures.PersonPojo();
        pojo.id = 2;
        pojo.address = null;

        GenericRow row = writer.toRow(pojo);
        ConvertersTestFixtures.PersonPojo back = reader.fromRow(row);

        assertThat(back.id).isEqualTo(2);
        assertThat(back.address).isNull();
    }

    @Test
    public void testDeeplyNestedRowRoundTrip() {
        RowType innerRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("val", DataTypes.DOUBLE()),
                        DataTypes.FIELD("flag", DataTypes.BOOLEAN()));
        RowType middleRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("inner", innerRowType));
        RowType table =
                RowType.builder()
                        .field("name", DataTypes.STRING())
                        .field("nested", middleRowType)
                        .build();

        PojoToRowConverter<ConvertersTestFixtures.DeepNestOuterPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.DeepNestOuterPojo.class, table, table);
        RowToPojoConverter<ConvertersTestFixtures.DeepNestOuterPojo> reader =
                RowToPojoConverter.of(ConvertersTestFixtures.DeepNestOuterPojo.class, table, table);

        ConvertersTestFixtures.DeepNestOuterPojo pojo =
                new ConvertersTestFixtures.DeepNestOuterPojo();
        pojo.name = "test";
        pojo.nested = new ConvertersTestFixtures.MiddlePojo();
        pojo.nested.id = 42;
        pojo.nested.inner = new ConvertersTestFixtures.InnerPojo();
        pojo.nested.inner.val = 3.14;
        pojo.nested.inner.flag = true;

        GenericRow row = writer.toRow(pojo);
        ConvertersTestFixtures.DeepNestOuterPojo back = reader.fromRow(row);

        assertThat(back.name).isEqualTo("test");
        assertThat(back.nested).isNotNull();
        assertThat(back.nested.id).isEqualTo(42);
        assertThat(back.nested.inner).isNotNull();
        assertThat(back.nested.inner.val).isEqualTo(3.14);
        assertThat(back.nested.inner.flag).isTrue();
    }

    @Test
    public void testNestedRowWithArrayFieldRoundTrip() {
        RowType nestedRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()),
                        DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.INT())));
        RowType table =
                RowType.builder().field("id", DataTypes.INT()).field("data", nestedRowType).build();

        PojoToRowConverter<ConvertersTestFixtures.RowWithArrayOuterPojo> writer =
                PojoToRowConverter.of(
                        ConvertersTestFixtures.RowWithArrayOuterPojo.class, table, table);
        RowToPojoConverter<ConvertersTestFixtures.RowWithArrayOuterPojo> reader =
                RowToPojoConverter.of(
                        ConvertersTestFixtures.RowWithArrayOuterPojo.class, table, table);

        ConvertersTestFixtures.RowWithArrayOuterPojo pojo =
                new ConvertersTestFixtures.RowWithArrayOuterPojo();
        pojo.id = 10;
        pojo.data = new ConvertersTestFixtures.RowWithArrayPojo();
        pojo.data.label = "scores";
        pojo.data.values = new Integer[] {90, 85, 100};

        GenericRow row = writer.toRow(pojo);
        ConvertersTestFixtures.RowWithArrayOuterPojo back = reader.fromRow(row);

        assertThat(back.id).isEqualTo(10);
        assertThat(back.data).isNotNull();
        assertThat(back.data.label).isEqualTo("scores");
        assertThat(back.data.values).containsExactly(90, 85, 100);
    }

    @Test
    public void testNestedRowWithMapFieldRoundTrip() {
        RowType nestedRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "attrs", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        RowType table =
                RowType.builder().field("id", DataTypes.INT()).field("info", nestedRowType).build();

        PojoToRowConverter<ConvertersTestFixtures.RowWithMapOuterPojo> writer =
                PojoToRowConverter.of(
                        ConvertersTestFixtures.RowWithMapOuterPojo.class, table, table);
        RowToPojoConverter<ConvertersTestFixtures.RowWithMapOuterPojo> reader =
                RowToPojoConverter.of(
                        ConvertersTestFixtures.RowWithMapOuterPojo.class, table, table);

        ConvertersTestFixtures.RowWithMapOuterPojo pojo =
                new ConvertersTestFixtures.RowWithMapOuterPojo();
        pojo.id = 5;
        pojo.info = new ConvertersTestFixtures.RowWithMapPojo();
        pojo.info.name = "config";
        pojo.info.attrs = new HashMap<>();
        pojo.info.attrs.put("timeout", 30);
        pojo.info.attrs.put("retries", 3);

        GenericRow row = writer.toRow(pojo);
        ConvertersTestFixtures.RowWithMapOuterPojo back = reader.fromRow(row);

        assertThat(back.id).isEqualTo(5);
        assertThat(back.info).isNotNull();
        assertThat(back.info.name).isEqualTo("config");
        assertThat(back.info.attrs).containsEntry("timeout", 30);
        assertThat(back.info.attrs).containsEntry("retries", 3);
    }

    @Test
    public void testArrayOfNestedRowRoundTrip() {
        RowType elementRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("city", DataTypes.STRING()),
                        DataTypes.FIELD("zipCode", DataTypes.INT()));
        RowType table =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("addresses", DataTypes.ARRAY(elementRowType))
                        .build();

        PojoToRowConverter<ConvertersTestFixtures.ArrayOfRowPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.ArrayOfRowPojo.class, table, table);
        RowToPojoConverter<ConvertersTestFixtures.ArrayOfRowPojo> reader =
                RowToPojoConverter.of(ConvertersTestFixtures.ArrayOfRowPojo.class, table, table);

        ConvertersTestFixtures.ArrayOfRowPojo pojo = new ConvertersTestFixtures.ArrayOfRowPojo();
        pojo.id = 1;
        ConvertersTestFixtures.AddressPojo addr1 = new ConvertersTestFixtures.AddressPojo();
        addr1.city = "Beijing";
        addr1.zipCode = 100000;
        ConvertersTestFixtures.AddressPojo addr2 = new ConvertersTestFixtures.AddressPojo();
        addr2.city = "Shanghai";
        addr2.zipCode = 200000;
        pojo.addresses = new ConvertersTestFixtures.AddressPojo[] {addr1, addr2};

        GenericRow row = writer.toRow(pojo);
        ConvertersTestFixtures.ArrayOfRowPojo back = reader.fromRow(row);

        assertThat(back.id).isEqualTo(1);
        assertThat(back.addresses).hasSize(2);
        assertThat(back.addresses[0].city).isEqualTo("Beijing");
        assertThat(back.addresses[0].zipCode).isEqualTo(100000);
        assertThat(back.addresses[1].city).isEqualTo("Shanghai");
        assertThat(back.addresses[1].zipCode).isEqualTo(200000);
    }

    @Test
    public void testMapWithRowValuesRoundTrip() {
        RowType valueRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("city", DataTypes.STRING()),
                        DataTypes.FIELD("zipCode", DataTypes.INT()));
        RowType table =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("addressMap", DataTypes.MAP(DataTypes.STRING(), valueRowType))
                        .build();

        PojoToRowConverter<ConvertersTestFixtures.MapOfRowPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.MapOfRowPojo.class, table, table);
        RowToPojoConverter<ConvertersTestFixtures.MapOfRowPojo> reader =
                RowToPojoConverter.of(ConvertersTestFixtures.MapOfRowPojo.class, table, table);

        ConvertersTestFixtures.MapOfRowPojo pojo = new ConvertersTestFixtures.MapOfRowPojo();
        pojo.id = 1;
        pojo.addressMap = new HashMap<>();
        ConvertersTestFixtures.AddressPojo addr = new ConvertersTestFixtures.AddressPojo();
        addr.city = "Beijing";
        addr.zipCode = 100000;
        pojo.addressMap.put("home", addr);

        GenericRow row = writer.toRow(pojo);
        ConvertersTestFixtures.MapOfRowPojo back = reader.fromRow(row);

        assertThat(back.id).isEqualTo(1);
        assertThat(back.addressMap).containsKey("home");
        assertThat(back.addressMap.get("home")).isEqualTo(addr);
    }

    @Test
    public void testListOfNestedRowRoundTrip() {
        RowType elementRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("city", DataTypes.STRING()),
                        DataTypes.FIELD("zipCode", DataTypes.INT()));
        RowType table =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("addresses", DataTypes.ARRAY(elementRowType))
                        .build();

        PojoToRowConverter<ConvertersTestFixtures.ListOfRowPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.ListOfRowPojo.class, table, table);
        RowToPojoConverter<ConvertersTestFixtures.ListOfRowPojo> reader =
                RowToPojoConverter.of(ConvertersTestFixtures.ListOfRowPojo.class, table, table);

        ConvertersTestFixtures.AddressPojo addr1 = new ConvertersTestFixtures.AddressPojo();
        addr1.city = "Beijing";
        addr1.zipCode = 100000;
        ConvertersTestFixtures.AddressPojo addr2 = new ConvertersTestFixtures.AddressPojo();
        addr2.city = "Shanghai";
        addr2.zipCode = 200000;

        ConvertersTestFixtures.ListOfRowPojo pojo = new ConvertersTestFixtures.ListOfRowPojo();
        pojo.id = 7;
        pojo.addresses = java.util.Arrays.asList(addr1, addr2);

        GenericRow row = writer.toRow(pojo);
        ConvertersTestFixtures.ListOfRowPojo back = reader.fromRow(row);

        assertThat(back.id).isEqualTo(7);
        assertThat(back.addresses).hasSize(2);
        assertThat(back.addresses.get(0)).isEqualTo(addr1);
        assertThat(back.addresses.get(1)).isEqualTo(addr2);
    }
}
