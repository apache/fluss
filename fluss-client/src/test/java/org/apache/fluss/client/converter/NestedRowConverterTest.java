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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for NestedRow (ROW type) support in PojoToRowConverter and RowToPojoConverter. */
public class NestedRowConverterTest {

    // ==================== Simple Nested Row ====================

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

        PojoToRowConverter<PersonPojo> writer =
                PojoToRowConverter.of(PersonPojo.class, table, table);
        RowToPojoConverter<PersonPojo> reader =
                RowToPojoConverter.of(PersonPojo.class, table, table);

        PersonPojo pojo = new PersonPojo();
        pojo.id = 1;
        pojo.address = new AddressPojo();
        pojo.address.city = "Beijing";
        pojo.address.zipCode = 100000;

        GenericRow row = writer.toRow(pojo);
        assertThat(row.getInt(0)).isEqualTo(1);
        // The nested Row should be a GenericRow
        InternalRow nestedRow = row.getRow(1, 2);
        assertThat(nestedRow).isNotNull();
        assertThat(nestedRow.getString(0).toString()).isEqualTo("Beijing");
        assertThat(nestedRow.getInt(1)).isEqualTo(100000);

        PersonPojo back = reader.fromRow(row);
        assertThat(back.id).isEqualTo(1);
        assertThat(back.address).isNotNull();
        assertThat(back.address.city).isEqualTo("Beijing");
        assertThat(back.address.zipCode).isEqualTo(100000);
    }

    @Test
    public void testNullNestedRow() {
        RowType table =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field(
                                "address",
                                DataTypes.ROW(
                                        DataTypes.FIELD("city", DataTypes.STRING()),
                                        DataTypes.FIELD("zipCode", DataTypes.INT())))
                        .build();

        PojoToRowConverter<PersonPojo> writer =
                PojoToRowConverter.of(PersonPojo.class, table, table);
        RowToPojoConverter<PersonPojo> reader =
                RowToPojoConverter.of(PersonPojo.class, table, table);

        PersonPojo pojo = new PersonPojo();
        pojo.id = 2;
        pojo.address = null;

        GenericRow row = writer.toRow(pojo);
        assertThat(row.getInt(0)).isEqualTo(2);
        assertThat(row.isNullAt(1)).isTrue();

        PersonPojo back = reader.fromRow(row);
        assertThat(back.id).isEqualTo(2);
        assertThat(back.address).isNull();
    }

    // ==================== Deeply Nested Row ====================

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

        PojoToRowConverter<DeepNestOuterPojo> writer =
                PojoToRowConverter.of(DeepNestOuterPojo.class, table, table);
        RowToPojoConverter<DeepNestOuterPojo> reader =
                RowToPojoConverter.of(DeepNestOuterPojo.class, table, table);

        DeepNestOuterPojo pojo = new DeepNestOuterPojo();
        pojo.name = "test";
        pojo.nested = new MiddlePojo();
        pojo.nested.id = 42;
        pojo.nested.inner = new InnerPojo();
        pojo.nested.inner.val = 3.14;
        pojo.nested.inner.flag = true;

        GenericRow row = writer.toRow(pojo);
        DeepNestOuterPojo back = reader.fromRow(row);

        assertThat(back.name).isEqualTo("test");
        assertThat(back.nested).isNotNull();
        assertThat(back.nested.id).isEqualTo(42);
        assertThat(back.nested.inner).isNotNull();
        assertThat(back.nested.inner.val).isEqualTo(3.14);
        assertThat(back.nested.inner.flag).isTrue();
    }

    // ==================== Nested Row with Array Field ====================

    @Test
    public void testNestedRowWithArrayFieldRoundTrip() {
        RowType nestedRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()),
                        DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.INT())));
        RowType table =
                RowType.builder().field("id", DataTypes.INT()).field("data", nestedRowType).build();

        PojoToRowConverter<RowWithArrayOuterPojo> writer =
                PojoToRowConverter.of(RowWithArrayOuterPojo.class, table, table);
        RowToPojoConverter<RowWithArrayOuterPojo> reader =
                RowToPojoConverter.of(RowWithArrayOuterPojo.class, table, table);

        RowWithArrayOuterPojo pojo = new RowWithArrayOuterPojo();
        pojo.id = 10;
        pojo.data = new RowWithArrayPojo();
        pojo.data.label = "scores";
        pojo.data.values = new Integer[] {90, 85, 100};

        GenericRow row = writer.toRow(pojo);
        RowWithArrayOuterPojo back = reader.fromRow(row);

        assertThat(back.id).isEqualTo(10);
        assertThat(back.data).isNotNull();
        assertThat(back.data.label).isEqualTo("scores");
        assertThat(back.data.values).containsExactly(90, 85, 100);
    }

    // ==================== Nested Row with Map Field ====================

    @Test
    public void testNestedRowWithMapFieldRoundTrip() {
        RowType nestedRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "attrs", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        RowType table =
                RowType.builder().field("id", DataTypes.INT()).field("info", nestedRowType).build();

        PojoToRowConverter<RowWithMapOuterPojo> writer =
                PojoToRowConverter.of(RowWithMapOuterPojo.class, table, table);
        RowToPojoConverter<RowWithMapOuterPojo> reader =
                RowToPojoConverter.of(RowWithMapOuterPojo.class, table, table);

        RowWithMapOuterPojo pojo = new RowWithMapOuterPojo();
        pojo.id = 5;
        pojo.info = new RowWithMapPojo();
        pojo.info.name = "config";
        pojo.info.attrs = new HashMap<>();
        pojo.info.attrs.put("timeout", 30);
        pojo.info.attrs.put("retries", 3);

        GenericRow row = writer.toRow(pojo);
        RowWithMapOuterPojo back = reader.fromRow(row);

        assertThat(back.id).isEqualTo(5);
        assertThat(back.info).isNotNull();
        assertThat(back.info.name).isEqualTo("config");
        assertThat(back.info.attrs).containsEntry("timeout", 30);
        assertThat(back.info.attrs).containsEntry("retries", 3);
    }

    // ==================== Array of Nested Row ====================

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

        PojoToRowConverter<ArrayOfRowPojo> writer =
                PojoToRowConverter.of(ArrayOfRowPojo.class, table, table);
        RowToPojoConverter<ArrayOfRowPojo> reader =
                RowToPojoConverter.of(ArrayOfRowPojo.class, table, table);

        ArrayOfRowPojo pojo = new ArrayOfRowPojo();
        pojo.id = 1;
        AddressPojo addr1 = new AddressPojo();
        addr1.city = "Beijing";
        addr1.zipCode = 100000;
        AddressPojo addr2 = new AddressPojo();
        addr2.city = "Shanghai";
        addr2.zipCode = 200000;
        pojo.addresses = new AddressPojo[] {addr1, addr2};

        GenericRow row = writer.toRow(pojo);
        ArrayOfRowPojo back = reader.fromRow(row);

        assertThat(back.id).isEqualTo(1);
        assertThat(back.addresses).hasSize(2);
        assertThat(back.addresses[0].city).isEqualTo("Beijing");
        assertThat(back.addresses[0].zipCode).isEqualTo(100000);
        assertThat(back.addresses[1].city).isEqualTo("Shanghai");
        assertThat(back.addresses[1].zipCode).isEqualTo(200000);
    }

    // ==================== Map with Row Values ====================

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

        PojoToRowConverter<MapOfRowPojo> writer =
                PojoToRowConverter.of(MapOfRowPojo.class, table, table);
        RowToPojoConverter<MapOfRowPojo> reader =
                RowToPojoConverter.of(MapOfRowPojo.class, table, table);

        MapOfRowPojo pojo = new MapOfRowPojo();
        pojo.id = 1;
        pojo.addressMap = new HashMap<>();
        AddressPojo addr = new AddressPojo();
        addr.city = "Beijing";
        addr.zipCode = 100000;
        pojo.addressMap.put("home", addr);

        GenericRow row = writer.toRow(pojo);
        MapOfRowPojo back = reader.fromRow(row);

        assertThat(back.id).isEqualTo(1);
        assertThat(back.addressMap).containsKey("home");
        // Due to Java type erasure, the Map value type is treated as Object during
        // deserialization, so ROW values are deserialized as InternalRow instead of POJO
        Object rawValue = back.addressMap.get("home");
        assertThat(rawValue).isInstanceOf(InternalRow.class);
        InternalRow nestedRow = (InternalRow) rawValue;
        assertThat(nestedRow.getString(0).toString()).isEqualTo("Beijing");
        assertThat(nestedRow.getInt(1)).isEqualTo(100000);
    }

    // ==================== Validation Tests ====================

    @Test
    public void testRowFieldWithIncompatibleType() {
        RowType table =
                RowType.builder()
                        .field("badField", DataTypes.ROW(DataTypes.FIELD("x", DataTypes.INT())))
                        .build();

        // int[] is not a valid POJO type for ROW mapping
        assertThatThrownBy(() -> PojoToRowConverter.of(PrimitiveArrayFieldPojo.class, table, table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a POJO class for ROW type");
    }

    @Test
    public void testRowFieldWithMapType() {
        RowType table =
                RowType.builder()
                        .field("badField", DataTypes.ROW(DataTypes.FIELD("x", DataTypes.INT())))
                        .build();

        assertThatThrownBy(() -> PojoToRowConverter.of(MapFieldPojo.class, table, table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a POJO class for ROW type");
    }

    // ==================== Helper POJOs ====================

    /** Address POJO used as nested ROW type in tests. */
    public static class AddressPojo {
        public String city;
        public Integer zipCode;

        public AddressPojo() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AddressPojo that = (AddressPojo) o;
            return Objects.equals(city, that.city) && Objects.equals(zipCode, that.zipCode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(city, zipCode);
        }
    }

    /** Person POJO with nested Address. */
    public static class PersonPojo {
        public Integer id;
        public AddressPojo address;

        public PersonPojo() {}
    }

    /** Inner POJO for deeply nested row tests. */
    public static class InnerPojo {
        public Double val;
        public Boolean flag;

        public InnerPojo() {}
    }

    /** Middle POJO containing inner nested POJO. */
    public static class MiddlePojo {
        public Integer id;
        public InnerPojo inner;

        public MiddlePojo() {}
    }

    /** Outer POJO for deeply nested row tests. */
    public static class DeepNestOuterPojo {
        public String name;
        public MiddlePojo nested;

        public DeepNestOuterPojo() {}
    }

    /** Nested POJO with array field. */
    public static class RowWithArrayPojo {
        public String label;
        public Integer[] values;

        public RowWithArrayPojo() {}
    }

    /** Outer POJO with nested row containing array. */
    public static class RowWithArrayOuterPojo {
        public Integer id;
        public RowWithArrayPojo data;

        public RowWithArrayOuterPojo() {}
    }

    /** Nested POJO with map field. */
    public static class RowWithMapPojo {
        public String name;
        public Map<String, Integer> attrs;

        public RowWithMapPojo() {}
    }

    /** Outer POJO with nested row containing map. */
    public static class RowWithMapOuterPojo {
        public Integer id;
        public RowWithMapPojo info;

        public RowWithMapOuterPojo() {}
    }

    /** POJO with array of nested row type. */
    public static class ArrayOfRowPojo {
        public Integer id;
        public AddressPojo[] addresses;

        public ArrayOfRowPojo() {}
    }

    /** POJO with map having nested row values. */
    public static class MapOfRowPojo {
        public Integer id;
        public Map<String, AddressPojo> addressMap;

        public MapOfRowPojo() {}
    }

    /** Negative test: array cannot be used as ROW type POJO field. */
    public static class PrimitiveArrayFieldPojo {
        public Integer[] badField;

        public PrimitiveArrayFieldPojo() {}
    }

    /** Negative test: Map cannot be used as ROW type POJO field. */
    public static class MapFieldPojo {
        public Map<String, Integer> badField;

        public MapFieldPojo() {}
    }
}
