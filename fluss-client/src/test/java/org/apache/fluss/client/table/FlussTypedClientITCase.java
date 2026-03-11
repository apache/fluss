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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.converter.RowToPojoConverter;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.lookup.TypedLookuper;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.TypedScanRecord;
import org.apache.fluss.client.table.scanner.log.TypedLogScanner;
import org.apache.fluss.client.table.scanner.log.TypedScanRecords;
import org.apache.fluss.client.table.writer.TypedAppendWriter;
import org.apache.fluss.client.table.writer.TypedUpsertWriter;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for writing and scanning POJOs via client API. */
public class FlussTypedClientITCase extends ClientToServerITCaseBase {

    /** Test POJO containing all supported field types used by converters. */
    public static class AllTypesPojo {
        // primary key
        public Integer a;
        // all supported converter fields
        public Boolean bool1;
        public Byte tiny;
        public Short small;
        public Integer intv;
        public Long big;
        public Float flt;
        public Double dbl;
        public Character ch;
        public String str;
        public byte[] bin;
        public byte[] bytes;
        public BigDecimal dec;
        public LocalDate dt;
        public LocalTime tm;
        public LocalDateTime tsNtz;
        public Instant tsLtz;

        public AllTypesPojo() {}

        public AllTypesPojo(
                Integer a,
                Boolean bool1,
                Byte tiny,
                Short small,
                Integer intv,
                Long big,
                Float flt,
                Double dbl,
                Character ch,
                String str,
                byte[] bin,
                byte[] bytes,
                BigDecimal dec,
                LocalDate dt,
                LocalTime tm,
                LocalDateTime tsNtz,
                Instant tsLtz) {
            this.a = a;
            this.bool1 = bool1;
            this.tiny = tiny;
            this.small = small;
            this.intv = intv;
            this.big = big;
            this.flt = flt;
            this.dbl = dbl;
            this.ch = ch;
            this.str = str;
            this.bin = bin;
            this.bytes = bytes;
            this.dec = dec;
            this.dt = dt;
            this.tm = tm;
            this.tsNtz = tsNtz;
            this.tsLtz = tsLtz;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AllTypesPojo that = (AllTypesPojo) o;
            return Objects.equals(a, that.a)
                    && Objects.equals(bool1, that.bool1)
                    && Objects.equals(tiny, that.tiny)
                    && Objects.equals(small, that.small)
                    && Objects.equals(intv, that.intv)
                    && Objects.equals(big, that.big)
                    && Objects.equals(flt, that.flt)
                    && Objects.equals(dbl, that.dbl)
                    && Objects.equals(ch, that.ch)
                    && Objects.equals(str, that.str)
                    && Arrays.equals(bin, that.bin)
                    && Arrays.equals(bytes, that.bytes)
                    && Objects.equals(dec, that.dec)
                    && Objects.equals(dt, that.dt)
                    && Objects.equals(tm, that.tm)
                    && Objects.equals(tsNtz, that.tsNtz)
                    && Objects.equals(tsLtz, that.tsLtz);
        }

        @Override
        public int hashCode() {
            int result =
                    Objects.hash(
                            a, bool1, tiny, small, intv, big, flt, dbl, ch, str, dec, dt, tm, tsNtz,
                            tsLtz);
            result = 31 * result + Arrays.hashCode(bin);
            result = 31 * result + Arrays.hashCode(bytes);
            return result;
        }

        @Override
        public String toString() {
            return "AllTypesPojo{"
                    + "a="
                    + a
                    + ", bool1="
                    + bool1
                    + ", tiny="
                    + tiny
                    + ", small="
                    + small
                    + ", intv="
                    + intv
                    + ", big="
                    + big
                    + ", flt="
                    + flt
                    + ", dbl="
                    + dbl
                    + ", ch="
                    + ch
                    + ", str='"
                    + str
                    + '\''
                    + ", bin="
                    + Arrays.toString(bin)
                    + ", bytes="
                    + Arrays.toString(bytes)
                    + ", dec="
                    + dec
                    + ", dt="
                    + dt
                    + ", tm="
                    + tm
                    + ", tsNtz="
                    + tsNtz
                    + ", tsLtz="
                    + tsLtz
                    + '}';
        }
    }

    /** Minimal POJO representing the primary key for {@link AllTypesPojo}. */
    public static class PLookupKey {
        public Integer a;

        public PLookupKey() {}

        public PLookupKey(Integer a) {
            this.a = a;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PLookupKey that = (PLookupKey) o;
            return Objects.equals(a, that.a);
        }

        @Override
        public int hashCode() {
            return Objects.hash(a);
        }

        @Override
        public String toString() {
            return "PLookupKey{" + "a=" + a + '}';
        }
    }

    private static Schema allTypesLogSchema() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("bool1", DataTypes.BOOLEAN())
                .column("tiny", DataTypes.TINYINT())
                .column("small", DataTypes.SMALLINT())
                .column("intv", DataTypes.INT())
                .column("big", DataTypes.BIGINT())
                .column("flt", DataTypes.FLOAT())
                .column("dbl", DataTypes.DOUBLE())
                .column("ch", DataTypes.CHAR(1))
                .column("str", DataTypes.STRING())
                .column("bin", DataTypes.BINARY(3))
                .column("bytes", DataTypes.BYTES())
                .column("dec", DataTypes.DECIMAL(10, 2))
                .column("dt", DataTypes.DATE())
                .column("tm", DataTypes.TIME())
                .column("tsNtz", DataTypes.TIMESTAMP(3))
                .column("tsLtz", DataTypes.TIMESTAMP_LTZ(3))
                .build();
    }

    private static Schema allTypesPkSchema() {
        // Same columns as log schema but with PK on 'a'
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("bool1", DataTypes.BOOLEAN())
                .column("tiny", DataTypes.TINYINT())
                .column("small", DataTypes.SMALLINT())
                .column("intv", DataTypes.INT())
                .column("big", DataTypes.BIGINT())
                .column("flt", DataTypes.FLOAT())
                .column("dbl", DataTypes.DOUBLE())
                .column("ch", DataTypes.CHAR(1))
                .column("str", DataTypes.STRING())
                .column("bin", DataTypes.BINARY(3))
                .column("bytes", DataTypes.BYTES())
                .column("dec", DataTypes.DECIMAL(10, 2))
                .column("dt", DataTypes.DATE())
                .column("tm", DataTypes.TIME())
                .column("tsNtz", DataTypes.TIMESTAMP(3))
                .column("tsLtz", DataTypes.TIMESTAMP_LTZ(3))
                .primaryKey("a")
                .build();
    }

    private static AllTypesPojo newAllTypesPojo(int i) {
        Integer a = i;
        Boolean bool1 = (i % 2) == 0;
        Byte tiny = (byte) (i - 5);
        Short small = (short) (100 + i);
        Integer intv = 1000 + i;
        Long big = 100000L + i;
        Float flt = 1.5f + i;
        Double dbl = 2.5 + i;
        Character ch = (char) ('a' + (i % 26));
        String str = "s" + i;
        byte[] bin = new byte[] {(byte) i, (byte) (i + 1), (byte) (i + 2)};
        byte[] bytes = new byte[] {(byte) (10 + i), (byte) (20 + i)};
        BigDecimal dec = new BigDecimal("12345." + (10 + i)).setScale(2, RoundingMode.HALF_UP);
        LocalDate dt = LocalDate.of(2024, 1, 1).plusDays(i);
        LocalTime tm = LocalTime.of(12, (i * 7) % 60, (i * 11) % 60);
        LocalDateTime tsNtz = LocalDateTime.of(2024, 1, 1, 0, 0).plusSeconds(i).withNano(0);
        Instant tsLtz = Instant.ofEpochMilli(1700000000000L + (i * 1000L));
        return new AllTypesPojo(
                a, bool1, tiny, small, intv, big, flt, dbl, ch, str, bin, bytes, dec, dt, tm, tsNtz,
                tsLtz);
    }

    @Test
    void testTypedAppendWriteAndScan() throws Exception {
        // Build all-types log table schema
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("bool1", DataTypes.BOOLEAN())
                        .column("tiny", DataTypes.TINYINT())
                        .column("small", DataTypes.SMALLINT())
                        .column("intv", DataTypes.INT())
                        .column("big", DataTypes.BIGINT())
                        .column("flt", DataTypes.FLOAT())
                        .column("dbl", DataTypes.DOUBLE())
                        .column("ch", DataTypes.CHAR(1))
                        .column("str", DataTypes.STRING())
                        .column("bin", DataTypes.BINARY(3))
                        .column("bytes", DataTypes.BYTES())
                        .column("dec", DataTypes.DECIMAL(10, 2))
                        .column("dt", DataTypes.DATE())
                        .column("tm", DataTypes.TIME())
                        .column("tsNtz", DataTypes.TIMESTAMP(3))
                        .column("tsLtz", DataTypes.TIMESTAMP_LTZ(3))
                        .build();
        TablePath path = TablePath.of("pojo_db", "all_types_log");
        TableDescriptor td = TableDescriptor.builder().schema(schema).distributedBy(2).build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            // write
            TypedAppendWriter<AllTypesPojo> writer =
                    table.newAppend().createTypedWriter(AllTypesPojo.class);
            List<AllTypesPojo> expected = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                AllTypesPojo u = newAllTypesPojo(i);
                expected.add(u);
                writer.append(u);
            }
            writer.flush();

            // read
            Scan scan = table.newScan();
            TypedLogScanner<AllTypesPojo> scanner = scan.createTypedLogScanner(AllTypesPojo.class);
            subscribeFromBeginning(scanner, table);

            List<AllTypesPojo> actual = new ArrayList<>();
            while (actual.size() < expected.size()) {
                TypedScanRecords<AllTypesPojo> recs = scanner.poll(Duration.ofSeconds(2));
                for (TypedScanRecord<AllTypesPojo> r : recs) {
                    assertThat(r.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    actual.add(r.getValue());
                }
            }
            assertThat(actual)
                    .usingRecursiveFieldByFieldElementComparator()
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    @Test
    void testTypedUpsertWriteAndScan() throws Exception {
        // Build all-types PK table schema (PK on 'a')
        Schema schema = allTypesPkSchema();
        TablePath path = TablePath.of("pojo_db", "all_types_pk");
        TableDescriptor td = TableDescriptor.builder().schema(schema).distributedBy(2, "a").build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            Upsert upsert = table.newUpsert();
            TypedUpsertWriter<AllTypesPojo> writer = upsert.createTypedWriter(AllTypesPojo.class);

            AllTypesPojo p1 = newAllTypesPojo(1);
            AllTypesPojo p2 = newAllTypesPojo(2);
            writer.upsert(p1).get();
            writer.upsert(p2).get();

            // update key 1: change a couple of fields
            AllTypesPojo p1Updated = newAllTypesPojo(1);
            p1Updated.str = "a1";
            p1Updated.dec = new java.math.BigDecimal("42.42");
            writer.upsert(p1Updated).get();
            writer.flush();

            // scan as POJOs and verify change types and values
            TypedLogScanner<AllTypesPojo> scanner =
                    table.newScan().createTypedLogScanner(AllTypesPojo.class);
            subscribeFromBeginning(scanner, table);

            List<ChangeType> changes = new ArrayList<>();
            List<AllTypesPojo> values = new ArrayList<>();
            while (values.size() < 4) { // INSERT 1, INSERT 2, UPDATE_BEFORE 1, UPDATE_AFTER 1
                TypedScanRecords<AllTypesPojo> recs = scanner.poll(Duration.ofSeconds(2));
                for (TypedScanRecord<AllTypesPojo> r : recs) {
                    changes.add(r.getChangeType());
                    values.add(r.getValue());
                }
            }

            assertThat(changes)
                    .containsExactlyInAnyOrder(
                            ChangeType.INSERT,
                            ChangeType.INSERT,
                            ChangeType.UPDATE_BEFORE,
                            ChangeType.UPDATE_AFTER);
            // ensure the last update_after reflects new value
            int lastIdx = changes.lastIndexOf(ChangeType.UPDATE_AFTER);
            assertThat(values.get(lastIdx)).isEqualTo(p1Updated);
        }
    }

    @Test
    void testTypedLookups() throws Exception {
        Schema schema = allTypesPkSchema();
        TablePath path = TablePath.of("pojo_db", "lookup_pk");
        TableDescriptor td = TableDescriptor.builder().schema(schema).distributedBy(2, "a").build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            TypedUpsertWriter<AllTypesPojo> writer =
                    table.newUpsert().createTypedWriter(AllTypesPojo.class);
            writer.upsert(newAllTypesPojo(1)).get();
            writer.upsert(newAllTypesPojo(2)).get();
            writer.flush();

            // primary key lookup using Lookuper API with POJO key
            TypedLookuper<PLookupKey> lookuper =
                    table.newLookup().createTypedLookuper(PLookupKey.class);
            RowType tableSchema = table.getTableInfo().getRowType();
            RowToPojoConverter<AllTypesPojo> rowConv =
                    RowToPojoConverter.of(AllTypesPojo.class, tableSchema, tableSchema);

            LookupResult lr = lookuper.lookup(new PLookupKey(1)).get();
            AllTypesPojo one = rowConv.fromRow(lr.getSingletonRow());
            assertThat(one).isEqualTo(newAllTypesPojo(1));
        }
    }

    @Test
    void testInternalRowLookup() throws Exception {
        Schema schema = allTypesPkSchema();
        TablePath path = TablePath.of("pojo_db", "lookup_internalrow");
        TableDescriptor td = TableDescriptor.builder().schema(schema).distributedBy(2, "a").build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            // write a couple of rows via POJO writer
            TypedUpsertWriter<AllTypesPojo> writer =
                    table.newUpsert().createTypedWriter(AllTypesPojo.class);
            writer.upsert(newAllTypesPojo(101)).get();
            writer.upsert(newAllTypesPojo(202)).get();
            writer.flush();

            // now perform lookup using the raw InternalRow path to ensure it's still supported
            Lookuper lookuper = table.newLookup().createLookuper();
            RowType tableSchema = table.getTableInfo().getRowType();
            RowType keyProjection = tableSchema.project(table.getTableInfo().getPrimaryKeys());

            // Build the key row directly using GenericRow to avoid any POJO conversion
            GenericRow keyRow = new GenericRow(keyProjection.getFieldCount());
            keyRow.setField(0, 101); // primary key field 'a'

            LookupResult lr = lookuper.lookup(keyRow).get();
            RowToPojoConverter<AllTypesPojo> rowConv =
                    RowToPojoConverter.of(AllTypesPojo.class, tableSchema, tableSchema);
            AllTypesPojo pojo = rowConv.fromRow(lr.getSingletonRow());
            assertThat(pojo).isEqualTo(newAllTypesPojo(101));
        }
    }

    @Test
    void testTypedProjections() throws Exception {
        TablePath path = TablePath.of("pojo_db", "proj_log");
        TableDescriptor td =
                TableDescriptor.builder().schema(allTypesLogSchema()).distributedBy(1).build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            TypedAppendWriter<AllTypesPojo> writer =
                    table.newAppend().createTypedWriter(AllTypesPojo.class);
            writer.append(newAllTypesPojo(10)).get();
            writer.append(newAllTypesPojo(11)).get();
            writer.flush();

            // Project only a subset of fields
            TypedLogScanner<AllTypesPojo> scanner =
                    table.newScan()
                            .project(Arrays.asList("a", "str"))
                            .createTypedLogScanner(AllTypesPojo.class);
            subscribeFromBeginning(scanner, table);
            TypedScanRecords<AllTypesPojo> recs = scanner.poll(Duration.ofSeconds(2));
            int i = 10;
            for (TypedScanRecord<AllTypesPojo> r : recs) {
                AllTypesPojo u = r.getValue();
                AllTypesPojo expectedPojo = new AllTypesPojo();
                expectedPojo.a = i;
                expectedPojo.str = "s" + i;
                assertThat(u).isEqualTo(expectedPojo);
                i++;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Complex-type (ARRAY / MAP) POJO and helpers
    // -------------------------------------------------------------------------

    /**
     * POJO covering the complex-type fields that are new in this PR.
     *
     * <ul>
     *   <li>ARRAY&lt;INT&gt; — Integer[]
     *   <li>ARRAY&lt;STRING&gt; — String[]
     *   <li>ARRAY&lt;ARRAY&lt;INT&gt;&gt; — Integer[][]
     *   <li>MAP&lt;STRING, INT&gt; — Map&lt;String, Integer&gt;
     *   <li>MAP&lt;STRING, ARRAY&lt;INT&gt;&gt; — Map&lt;String, Object[]&gt; (generic type is
     *       erased at runtime; inner arrays always come back as Object[])
     * </ul>
     */
    public static class ComplexTypesPojo {
        public Integer id;
        public Integer[] intArray;
        public String[] strArray;
        public Integer[][] nestedArray;
        public Map<String, Integer> simpleMap;
        // Map values that are arrays are always deserialized as Object[] (type erasure)
        public Map<String, Object[]> mapOfArrays;

        public ComplexTypesPojo() {}

        /** Constructor that sets only the id; all array/map fields default to null. */
        public ComplexTypesPojo(Integer id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ComplexTypesPojo that = (ComplexTypesPojo) o;
            return Objects.equals(id, that.id)
                    && Arrays.equals(intArray, that.intArray)
                    && Arrays.equals(strArray, that.strArray)
                    && Arrays.deepEquals(nestedArray, that.nestedArray)
                    && Objects.equals(simpleMap, that.simpleMap)
                    && mapsOfArraysEqual(mapOfArrays, that.mapOfArrays);
        }

        private static boolean mapsOfArraysEqual(Map<String, Object[]> a, Map<String, Object[]> b) {
            if (a == b) {
                return true;
            }
            if (a == null || b == null || a.size() != b.size()) {
                return false;
            }
            for (Map.Entry<String, Object[]> e : a.entrySet()) {
                if (!Arrays.equals(e.getValue(), b.get(e.getKey()))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(id, simpleMap);
            result = 31 * result + Arrays.hashCode(intArray);
            result = 31 * result + Arrays.hashCode(strArray);
            result = 31 * result + Arrays.deepHashCode(nestedArray);
            return result;
        }

        @Override
        public String toString() {
            return "ComplexTypesPojo{"
                    + "id="
                    + id
                    + ", intArray="
                    + Arrays.toString(intArray)
                    + ", strArray="
                    + Arrays.toString(strArray)
                    + ", nestedArray="
                    + Arrays.deepToString(nestedArray)
                    + ", simpleMap="
                    + simpleMap
                    + ", mapOfArrays="
                    + mapOfArrays
                    + '}';
        }
    }

    /** Primary-key lookup POJO for {@link ComplexTypesPojo}. */
    public static class ComplexTypesLookupKey {
        public Integer id;

        public ComplexTypesLookupKey() {}

        public ComplexTypesLookupKey(Integer id) {
            this.id = id;
        }
    }

    private static Schema complexTypesLogSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("intArray", DataTypes.ARRAY(DataTypes.INT()))
                .column("strArray", DataTypes.ARRAY(DataTypes.STRING()))
                .column("nestedArray", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))
                .column("simpleMap", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                .column(
                        "mapOfArrays",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT())))
                .build();
    }

    private static Schema complexTypesPkSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("intArray", DataTypes.ARRAY(DataTypes.INT()))
                .column("strArray", DataTypes.ARRAY(DataTypes.STRING()))
                .column("nestedArray", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))
                .column("simpleMap", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                .column(
                        "mapOfArrays",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT())))
                .primaryKey("id")
                .build();
    }

    private static ComplexTypesPojo newComplexTypesPojo(int i) {
        ComplexTypesPojo p = new ComplexTypesPojo();
        p.id = i;
        p.intArray = new Integer[] {i, i + 1, i + 2};
        p.strArray = new String[] {"s" + i, "s" + (i + 1)};
        p.nestedArray = new Integer[][] {{i, i + 1}, {i + 2, i + 3}};
        p.simpleMap = new HashMap<>();
        p.simpleMap.put("k" + i, i * 10);
        p.simpleMap.put("k" + (i + 1), (i + 1) * 10);
        p.mapOfArrays = new HashMap<>();
        p.mapOfArrays.put("arr" + i, new Object[] {i * 100, i * 100 + 1});
        p.mapOfArrays.put("arr" + (i + 1), new Object[] {(i + 1) * 100, (i + 1) * 100 + 1});
        return p;
    }

    @Test
    void testComplexTypesAppendWriteAndScan() throws Exception {
        TablePath path = TablePath.of("pojo_db", "complex_types_log");
        TableDescriptor td =
                TableDescriptor.builder().schema(complexTypesLogSchema()).distributedBy(1).build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            TypedAppendWriter<ComplexTypesPojo> writer =
                    table.newAppend().createTypedWriter(ComplexTypesPojo.class);

            List<ComplexTypesPojo> expected = new ArrayList<>();
            for (int i = 1; i <= 3; i++) {
                ComplexTypesPojo p = newComplexTypesPojo(i);
                expected.add(p);
                writer.append(p);
            }

            // also write a row with null array / map fields to verify null propagation
            ComplexTypesPojo nullFieldPojo = new ComplexTypesPojo(99);
            expected.add(nullFieldPojo);
            writer.append(nullFieldPojo);

            writer.flush();

            TypedLogScanner<ComplexTypesPojo> scanner =
                    table.newScan().createTypedLogScanner(ComplexTypesPojo.class);
            subscribeFromBeginning(scanner, table);

            List<ComplexTypesPojo> actual = new ArrayList<>();
            while (actual.size() < expected.size()) {
                TypedScanRecords<ComplexTypesPojo> recs = scanner.poll(Duration.ofSeconds(2));
                for (TypedScanRecord<ComplexTypesPojo> r : recs) {
                    assertThat(r.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    actual.add(r.getValue());
                }
            }

            // verify all elements (custom equals handles deep array comparison)
            assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

            // spot-check the null-field row
            ComplexTypesPojo nullBack =
                    actual.stream().filter(p -> p.id == 99).findFirst().orElse(null);
            assertThat(nullBack).isNotNull();
            assertThat(nullBack.intArray).isNull();
            assertThat(nullBack.strArray).isNull();
            assertThat(nullBack.nestedArray).isNull();
            assertThat(nullBack.simpleMap).isNull();
            assertThat(nullBack.mapOfArrays).isNull();
        }
    }

    @Test
    void testComplexTypesUpsertAndLookup() throws Exception {
        TablePath path = TablePath.of("pojo_db", "complex_types_pk");
        TableDescriptor td =
                TableDescriptor.builder()
                        .schema(complexTypesPkSchema())
                        .distributedBy(1, "id")
                        .build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            TypedUpsertWriter<ComplexTypesPojo> writer =
                    table.newUpsert().createTypedWriter(ComplexTypesPojo.class);

            ComplexTypesPojo original = newComplexTypesPojo(10);
            writer.upsert(original).get();

            // overwrite with updated arrays/maps
            ComplexTypesPojo updated = new ComplexTypesPojo();
            updated.id = 10;
            updated.intArray = new Integer[] {100, 200, 300};
            updated.strArray = new String[] {"updated"};
            updated.nestedArray = new Integer[][] {{9, 8}, {7}};
            updated.simpleMap = new HashMap<>();
            updated.simpleMap.put("new_key", 999);
            updated.mapOfArrays = new HashMap<>();
            updated.mapOfArrays.put("new_arr", new Object[] {-1, -2});
            writer.upsert(updated).get();
            writer.flush();

            // verify via typed lookup
            RowType tableSchema = table.getTableInfo().getRowType();
            RowToPojoConverter<ComplexTypesPojo> rowConv =
                    RowToPojoConverter.of(ComplexTypesPojo.class, tableSchema, tableSchema);

            TypedLookuper<ComplexTypesLookupKey> lookuper =
                    table.newLookup().createTypedLookuper(ComplexTypesLookupKey.class);
            ComplexTypesPojo lookedUp =
                    rowConv.fromRow(
                            lookuper.lookup(new ComplexTypesLookupKey(10)).get().getSingletonRow());

            assertThat(lookedUp.id).isEqualTo(10);
            assertThat(lookedUp.intArray).isEqualTo(new Integer[] {100, 200, 300});
            assertThat(lookedUp.strArray).isEqualTo(new String[] {"updated"});
            assertThat(Arrays.deepEquals(lookedUp.nestedArray, new Integer[][] {{9, 8}, {7}}))
                    .isTrue();
            assertThat(lookedUp.simpleMap).containsEntry("new_key", 999);
            assertThat(lookedUp.mapOfArrays).containsKey("new_arr");
            // inner arrays are deserialized as Object[] due to type erasure
            assertThat(lookedUp.mapOfArrays.get("new_arr")).isEqualTo(new Object[] {-1, -2});

            // verify non-existent key returns null row
            assertThat(lookuper.lookup(new ComplexTypesLookupKey(999)).get().getSingletonRow())
                    .isNull();
        }
    }

    // -------------------------------------------------------------------------
    // List-field POJO — verifies that java.util.List fields work on both paths
    // -------------------------------------------------------------------------

    /**
     * POJO where ARRAY columns are mapped to {@link List} fields instead of Java arrays. Both the
     * write path ({@link PojoArrayToFlussArray}) and the read path ({@link FlussArrayToPojoArray})
     * support {@link java.util.Collection} types.
     */
    public static class ListTypesPojo {
        public Integer id;
        public List<Integer> intList;
        public List<String> strList;
        public List<Integer> nullableIntList;

        public ListTypesPojo() {}
    }

    @Test
    void testListFieldsRoundTrip() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("intList", DataTypes.ARRAY(DataTypes.INT()))
                        .column("strList", DataTypes.ARRAY(DataTypes.STRING()))
                        .column("nullableIntList", DataTypes.ARRAY(DataTypes.INT()))
                        .build();

        TablePath path = TablePath.of("pojo_db", "list_fields_log");
        TableDescriptor td = TableDescriptor.builder().schema(schema).distributedBy(1).build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            TypedAppendWriter<ListTypesPojo> writer =
                    table.newAppend().createTypedWriter(ListTypesPojo.class);

            ListTypesPojo p1 = new ListTypesPojo();
            p1.id = 1;
            p1.intList = new ArrayList<>(Arrays.asList(10, 20, 30));
            p1.strList = Arrays.asList("alpha", "beta");
            p1.nullableIntList = Arrays.asList(1, null, 3);

            ListTypesPojo p2 = new ListTypesPojo();
            p2.id = 2;
            p2.intList = new ArrayList<>(); // empty list
            p2.strList = Arrays.asList("only");
            p2.nullableIntList = null; // null list field

            writer.append(p1);
            writer.append(p2);
            writer.flush();

            TypedLogScanner<ListTypesPojo> scanner =
                    table.newScan().createTypedLogScanner(ListTypesPojo.class);
            subscribeFromBeginning(scanner, table);

            Map<Integer, ListTypesPojo> actual = new HashMap<>();
            while (actual.size() < 2) {
                TypedScanRecords<ListTypesPojo> recs = scanner.poll(Duration.ofSeconds(2));
                for (TypedScanRecord<ListTypesPojo> r : recs) {
                    actual.put(r.getValue().id, r.getValue());
                }
            }

            // verify row 1
            ListTypesPojo back1 = actual.get(1);
            assertThat(back1.intList).isInstanceOf(List.class);
            assertThat(back1.intList).containsExactly(10, 20, 30);
            assertThat(back1.strList).containsExactly("alpha", "beta");
            assertThat(back1.nullableIntList).containsExactly(1, null, 3);

            // verify row 2
            ListTypesPojo back2 = actual.get(2);
            assertThat(back2.intList).isInstanceOf(List.class);
            assertThat(back2.intList).isEmpty();
            assertThat(back2.strList).containsExactly("only");
            assertThat(back2.nullableIntList).isNull();
        }
    }

    @Test
    void testTypedPartialUpdates() throws Exception {
        // Use full PK schema and update a subset of fields
        Schema schema = allTypesPkSchema();
        TablePath path = TablePath.of("pojo_db", "pk_partial");
        TableDescriptor td = TableDescriptor.builder().schema(schema).distributedBy(1, "a").build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            // 1. initial full row
            TypedUpsertWriter<AllTypesPojo> fullWriter =
                    table.newUpsert().createTypedWriter(AllTypesPojo.class);
            fullWriter.upsert(newAllTypesPojo(1)).get();
            fullWriter.flush();

            // 2. partial update: only PK + subset fields
            Upsert upsert = table.newUpsert().partialUpdate("a", "str", "dec");
            TypedUpsertWriter<AllTypesPojo> writer = upsert.createTypedWriter(AllTypesPojo.class);

            AllTypesPojo patch = new AllTypesPojo();
            patch.a = 1;
            patch.str = "second";
            patch.dec = new BigDecimal("99.99");
            writer.upsert(patch).get();
            writer.flush();

            // verify via lookup and scan using Lookuper + POJO key
            TypedLookuper<PLookupKey> lookuper =
                    table.newLookup().createTypedLookuper(PLookupKey.class);
            RowType tableSchema = table.getTableInfo().getRowType();
            RowToPojoConverter<AllTypesPojo> rowConv =
                    RowToPojoConverter.of(AllTypesPojo.class, tableSchema, tableSchema);
            AllTypesPojo lookedUp =
                    rowConv.fromRow(lookuper.lookup(new PLookupKey(1)).get().getSingletonRow());
            AllTypesPojo expected = newAllTypesPojo(1);
            expected.str = "second";
            expected.dec = new BigDecimal("99.99");
            assertThat(lookedUp).isEqualTo(expected);

            TypedLogScanner<AllTypesPojo> scanner =
                    table.newScan().createTypedLogScanner(AllTypesPojo.class);
            subscribeFromBeginning(scanner, table);
            boolean sawUpdateAfter = false;
            while (!sawUpdateAfter) {
                TypedScanRecords<AllTypesPojo> recs = scanner.poll(Duration.ofSeconds(2));
                for (TypedScanRecord<AllTypesPojo> r : recs) {
                    if (r.getChangeType() == ChangeType.UPDATE_AFTER) {
                        assertThat(r.getValue()).isEqualTo(expected);
                        sawUpdateAfter = true;
                    }
                }
            }
        }
    }
}
