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

package org.apache.fluss.rpc.util;

import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.predicate.GreaterThan;
import org.apache.fluss.predicate.In;
import org.apache.fluss.predicate.IsNull;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.LessThan;
import org.apache.fluss.predicate.Or;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.rpc.messages.PbPredicate;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** PredicateMessageUtilsTest. */
public class PredicateMessageUtilsTest {

    /**
     * Builds a RowType that covers all field indices used by the given predicates. Positions not
     * covered by any predicate are filled with a nullable IntType placeholder. Each field gets a
     * stable, non-ordinal field id so tests validate field-id based serialization.
     */
    private static RowType buildRowType(List<LeafPredicate> predicates) {
        int maxIndex = 0;
        for (LeafPredicate p : predicates) {
            if (p.index() > maxIndex) {
                maxIndex = p.index();
            }
        }
        DataField[] fields = new DataField[maxIndex + 1];
        for (int i = 0; i <= maxIndex; i++) {
            fields[i] = new DataField("_placeholder_" + i, new IntType(true), 100 + i * 10);
        }
        for (LeafPredicate p : predicates) {
            fields[p.index()] = new DataField(p.fieldName(), p.type(), 100 + p.index() * 10);
        }
        return new RowType(Arrays.asList(fields));
    }

    private static RowType buildRowType(LeafPredicate... predicates) {
        return buildRowType(Arrays.asList(predicates));
    }

    @Test
    public void testLeafPredicateIntEqual() {
        DataType type = new IntType(false);
        LeafPredicate predicate =
                new LeafPredicate(Equal.INSTANCE, type, 0, "id", Collections.singletonList(123));
        RowType rowType = buildRowType(predicate);
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate, rowType);
        assertThat(pb.totalSize()).isGreaterThan(0);
        assertThat(pb.getLeaf().getFieldId()).isEqualTo(rowType.getFields().get(0).getFieldId());
        Predicate result = PredicateMessageUtils.toPredicate(pb, rowType);
        assertThat(result).isInstanceOf(LeafPredicate.class);
        LeafPredicate lp = (LeafPredicate) result;
        assertThat(lp.function()).isEqualTo(Equal.INSTANCE);
        assertThat(lp.fieldName()).isEqualTo("id");
        assertThat(lp.literals().get(0)).isEqualTo(123);
    }

    @Test
    public void testLeafPredicateStringIn() {
        DataType type = new StringType(true);
        List<Object> values =
                Arrays.asList(
                        BinaryString.fromString("foo"),
                        BinaryString.fromString("bar"),
                        BinaryString.fromString("baz"));
        LeafPredicate predicate = new LeafPredicate(In.INSTANCE, type, 1, "name", values);
        RowType rowType = buildRowType(predicate);
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate, rowType);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb, rowType);
        assertThat(result).isInstanceOf(LeafPredicate.class);
        LeafPredicate lp = (LeafPredicate) result;
        assertThat(lp.function()).isEqualTo(In.INSTANCE);
        assertThat(lp.fieldName()).isEqualTo("name");
        assertThat(lp.literals()).isEqualTo(values);
    }

    @Test
    public void testLeafPredicateIsNull() {
        DataType type = new IntType(true);
        LeafPredicate predicate =
                new LeafPredicate(IsNull.INSTANCE, type, 2, "age", Collections.emptyList());
        RowType rowType = buildRowType(predicate);
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate, rowType);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb, rowType);
        assertThat(result).isInstanceOf(LeafPredicate.class);
        LeafPredicate lp = (LeafPredicate) result;
        assertThat(lp.function()).isEqualTo(IsNull.INSTANCE);
        assertThat(lp.fieldName()).isEqualTo("age");
    }

    @Test
    public void testLeafPredicateDecimal() {
        DataType type = new DecimalType(false, 10, 2);
        Decimal decimal = Decimal.fromBigDecimal(new BigDecimal("1234.56"), 10, 2);
        LeafPredicate predicate =
                new LeafPredicate(
                        Equal.INSTANCE, type, 3, "amount", Collections.singletonList(decimal));
        RowType rowType = buildRowType(predicate);
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate, rowType);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb, rowType);
        assertThat(result).isInstanceOf(LeafPredicate.class);
        LeafPredicate lp = (LeafPredicate) result;
        assertThat(lp.function()).isEqualTo(Equal.INSTANCE);
        assertThat(lp.fieldName()).isEqualTo("amount");
        assertThat(lp.literals()).hasSize(1);
        assertThat(((Decimal) lp.literals().get(0)).toBigDecimal())
                .isEqualByComparingTo(decimal.toBigDecimal());
        assertThat(((Decimal) lp.literals().get(0)).precision()).isEqualTo(decimal.precision());
        assertThat(((Decimal) lp.literals().get(0)).scale()).isEqualTo(decimal.scale());
    }

    @Test
    public void testLeafPredicateTimestamp() {
        DataType type = new TimestampType(false, 3);
        TimestampNtz ts = TimestampNtz.fromMillis(1680000000000L, 3);
        LeafPredicate predicate =
                new LeafPredicate(
                        GreaterThan.INSTANCE, type, 4, "ts", Collections.singletonList(ts));
        RowType rowType = buildRowType(predicate);
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate, rowType);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb, rowType);
        assertThat(result).isInstanceOf(LeafPredicate.class);
        LeafPredicate lp = (LeafPredicate) result;
        assertThat(lp.function()).isEqualTo(GreaterThan.INSTANCE);
        assertThat(lp.fieldName()).isEqualTo("ts");
        assertThat(lp.literals()).hasSize(1);
        assertThat(((TimestampNtz) lp.literals().get(0)).getMillisecond())
                .isEqualTo(ts.getMillisecond());
        assertThat(((TimestampNtz) lp.literals().get(0)).getNanoOfMillisecond())
                .isEqualTo(ts.getNanoOfMillisecond());
    }

    @Test
    public void testLeafPredicateBoolean() {
        DataType type = new BooleanType(false);
        LeafPredicate predicate =
                new LeafPredicate(Equal.INSTANCE, type, 5, "flag", Collections.singletonList(true));
        RowType rowType = buildRowType(predicate);
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate, rowType);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb, rowType);
        assertThat(result).isInstanceOf(LeafPredicate.class);
        LeafPredicate lp = (LeafPredicate) result;
        assertThat(lp.function()).isEqualTo(Equal.INSTANCE);
        assertThat(lp.fieldName()).isEqualTo("flag");
        assertThat(lp.literals().get(0)).isEqualTo(true);
    }

    @Test
    public void testCompoundPredicateAnd() {
        DataType type = new IntType(false);
        LeafPredicate p1 =
                new LeafPredicate(
                        GreaterThan.INSTANCE, type, 0, "id", Collections.singletonList(10));
        LeafPredicate p2 =
                new LeafPredicate(LessThan.INSTANCE, type, 0, "id", Collections.singletonList(100));
        CompoundPredicate andPredicate = new CompoundPredicate(And.INSTANCE, Arrays.asList(p1, p2));
        RowType rowType = buildRowType(p1, p2);
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(andPredicate, rowType);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb, rowType);
        assertThat(result).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate cp = (CompoundPredicate) result;
        assertThat(cp.function()).isEqualTo(And.INSTANCE);
        assertThat(cp.children()).hasSize(2);
    }

    @Test
    public void testCompoundPredicateOrNested() {
        DataType type = new StringType(false);
        LeafPredicate p1 =
                new LeafPredicate(
                        Equal.INSTANCE, type, 0, "name", Collections.singletonList("foo"));
        LeafPredicate p2 =
                new LeafPredicate(
                        Equal.INSTANCE, type, 0, "name", Collections.singletonList("bar"));
        CompoundPredicate orPredicate = new CompoundPredicate(Or.INSTANCE, Arrays.asList(p1, p2));
        CompoundPredicate andPredicate =
                new CompoundPredicate(And.INSTANCE, Arrays.asList(orPredicate, p1));
        RowType rowType = buildRowType(p1, p2);
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(andPredicate, rowType);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb, rowType);
        assertThat(result).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate cp = (CompoundPredicate) result;
        assertThat(cp.function()).isEqualTo(And.INSTANCE);
        assertThat(cp.children()).hasSize(2);
        assertThat(cp.children().get(0)).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate orCp = (CompoundPredicate) cp.children().get(0);
        assertThat(orCp.function()).isEqualTo(Or.INSTANCE);
    }

    @Test
    public void testPbLiteralSerde() {
        // boolean
        DataType boolType = new BooleanType(false);
        LeafPredicate boolPredicate =
                new LeafPredicate(
                        Equal.INSTANCE, boolType, 0, "f_bool", Collections.singletonList(true));
        // int
        DataType intType = new IntType(false);
        LeafPredicate intPredicate =
                new LeafPredicate(
                        Equal.INSTANCE, intType, 1, "f_int", Collections.singletonList(123));
        // bigint
        DataType bigIntType = new BigIntType(false);
        LeafPredicate bigIntPredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        bigIntType,
                        2,
                        "f_bigint",
                        Collections.singletonList(1234567890123L));
        // float
        DataType floatType = new FloatType(false);
        LeafPredicate floatPredicate =
                new LeafPredicate(
                        Equal.INSTANCE, floatType, 3, "f_float", Collections.singletonList(1.23f));
        // double
        DataType doubleType = new DoubleType(false);
        LeafPredicate doublePredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        doubleType,
                        4,
                        "f_double",
                        Collections.singletonList(2.34d));
        // char
        DataType charType = new CharType(false, 5);
        LeafPredicate charPredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        charType,
                        5,
                        "f_char",
                        Collections.singletonList(BinaryString.fromString("abcde")));
        // string
        DataType stringType = new StringType(false);
        LeafPredicate stringPredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        stringType,
                        6,
                        "f_string",
                        Collections.singletonList(BinaryString.fromString("hello")));
        // decimal
        DataType decimalType = new DecimalType(false, 10, 2);
        Decimal decimal = Decimal.fromBigDecimal(new BigDecimal("1234.56"), 10, 2);
        LeafPredicate decimalPredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        decimalType,
                        7,
                        "f_decimal",
                        Collections.singletonList(decimal));
        // date
        DataType dateType = new DateType(false);
        LeafPredicate datePredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        dateType,
                        8,
                        "f_date",
                        Collections.singletonList(
                                LocalDate.ofEpochDay(19000L))); // days since epoch
        // time
        DataType timeType = new TimeType(false, 3);
        LocalTime time = LocalTime.of(12, 30);
        LeafPredicate timePredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        timeType,
                        9,
                        "f_time",
                        Collections.singletonList(time)); // millis of day
        // timestamp
        DataType tsType = new TimestampType(false, 3);
        TimestampNtz ts = TimestampNtz.fromMillis(1680000000000L, 3);
        LeafPredicate tsPredicate =
                new LeafPredicate(
                        Equal.INSTANCE, tsType, 10, "f_ts", Collections.singletonList(ts));
        // timestamp_ltz
        DataType ltzType = new LocalZonedTimestampType(false, 3);
        TimestampLtz ltz = TimestampLtz.fromEpochMillis(1680000000000L, 3);
        LeafPredicate ltzPredicate =
                new LeafPredicate(
                        Equal.INSTANCE, ltzType, 11, "f_ltz", Collections.singletonList(ltz));
        // binary
        DataType binaryType = new BinaryType(4);
        LeafPredicate binaryPredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        binaryType,
                        12,
                        "f_binary",
                        Collections.singletonList(new byte[] {1, 2, 3, 4}));
        // bytes
        DataType bytesType = new BytesType(false);
        LeafPredicate bytesPredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        bytesType,
                        13,
                        "f_bytes",
                        Collections.singletonList(new byte[] {5, 6, 7, 8}));
        // tinyint
        DataType tinyIntType = new TinyIntType(false);
        LeafPredicate tinyIntPredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        tinyIntType,
                        14,
                        "f_tinyint",
                        Collections.singletonList((byte) 7));
        // smallint
        DataType smallIntType = new SmallIntType(false);
        LeafPredicate smallIntPredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        smallIntType,
                        15,
                        "f_smallint",
                        Collections.singletonList((short) 1234));

        List<LeafPredicate> predicates =
                Arrays.asList(
                        boolPredicate,
                        intPredicate,
                        bigIntPredicate,
                        floatPredicate,
                        doublePredicate,
                        charPredicate,
                        stringPredicate,
                        decimalPredicate,
                        datePredicate,
                        timePredicate,
                        tsPredicate,
                        ltzPredicate,
                        binaryPredicate,
                        bytesPredicate,
                        tinyIntPredicate,
                        smallIntPredicate);
        RowType rowType = buildRowType(predicates);
        for (LeafPredicate predicate : predicates) {
            PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate, rowType);
            assertThat(pb.totalSize()).isGreaterThan(0);
            assertThat(pb.getLeaf().getFieldId())
                    .isEqualTo(rowType.getFields().get(predicate.index()).getFieldId());
            Predicate result = PredicateMessageUtils.toPredicate(pb, rowType);
            assertThat(result).isInstanceOf(LeafPredicate.class);
            LeafPredicate lp = (LeafPredicate) result;
            assertThat(lp.function()).isEqualTo(predicate.function());
            assertThat(lp.fieldName()).isEqualTo(predicate.fieldName());
            assertThat(lp.index()).isEqualTo(predicate.index());
            assertThat(lp.type().getClass()).isEqualTo(predicate.type().getClass());
            if (predicate.type() instanceof DecimalType) {
                assertThat(((Decimal) lp.literals().get(0)).precision())
                        .isEqualTo(((Decimal) predicate.literals().get(0)).precision());
                assertThat(((Decimal) lp.literals().get(0)).scale())
                        .isEqualTo(((Decimal) predicate.literals().get(0)).scale());
            } else if (predicate.type() instanceof TimestampType) {
                assertThat(((TimestampNtz) lp.literals().get(0)).getMillisecond())
                        .isEqualTo(((TimestampNtz) predicate.literals().get(0)).getMillisecond());
            } else if (predicate.type() instanceof LocalZonedTimestampType) {
                assertThat(((TimestampLtz) lp.literals().get(0)).getEpochMillisecond())
                        .isEqualTo(
                                ((TimestampLtz) predicate.literals().get(0)).getEpochMillisecond());
            } else if (predicate.type() instanceof BinaryType
                    || predicate.type() instanceof BytesType) {
                assertThat((byte[]) lp.literals().get(0))
                        .isEqualTo((byte[]) predicate.literals().get(0));
            } else {
                assertThat(lp.literals().get(0)).isEqualTo(predicate.literals().get(0));
            }
        }
    }

    @Test
    public void testAllDataTypesWithNullValues() {
        List<LeafPredicate> nullPredicates =
                Arrays.asList(
                        // Boolean null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new BooleanType(true),
                                0,
                                "f_bool_null",
                                Collections.singletonList(null)),
                        // Int null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new IntType(true),
                                1,
                                "f_int_null",
                                Collections.singletonList(null)),
                        // BigInt null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new BigIntType(true),
                                2,
                                "f_bigint_null",
                                Collections.singletonList(null)),
                        // Float null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new FloatType(true),
                                3,
                                "f_float_null",
                                Collections.singletonList(null)),
                        // Double null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new DoubleType(true),
                                4,
                                "f_double_null",
                                Collections.singletonList(null)),
                        // Char null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new CharType(true, 5),
                                5,
                                "f_char_null",
                                Collections.singletonList(null)),
                        // String null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new StringType(true),
                                6,
                                "f_string_null",
                                Collections.singletonList(null)),
                        // Decimal null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new DecimalType(true, 10, 2),
                                7,
                                "f_decimal_null",
                                Collections.singletonList(null)),
                        // Date null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new DateType(true),
                                8,
                                "f_date_null",
                                Collections.singletonList(null)),
                        // Time null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new TimeType(true, 3),
                                9,
                                "f_time_null",
                                Collections.singletonList(null)),
                        // Timestamp null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new TimestampType(true, 3),
                                10,
                                "f_ts_null",
                                Collections.singletonList(null)),
                        // TimestampLtz null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new LocalZonedTimestampType(true, 3),
                                11,
                                "f_ltz_null",
                                Collections.singletonList(null)),
                        // Binary null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new BinaryType(4),
                                12,
                                "f_binary_null",
                                Collections.singletonList(null)),
                        // Bytes null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new BytesType(true),
                                13,
                                "f_bytes_null",
                                Collections.singletonList(null)),
                        // TinyInt null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new TinyIntType(true),
                                14,
                                "f_tinyint_null",
                                Collections.singletonList(null)),
                        // SmallInt null
                        new LeafPredicate(
                                Equal.INSTANCE,
                                new SmallIntType(true),
                                15,
                                "f_smallint_null",
                                Collections.singletonList(null)));

        RowType rowType = buildRowType(nullPredicates);
        for (LeafPredicate predicate : nullPredicates) {
            PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate, rowType);
            assertThat(pb.totalSize()).isGreaterThan(0);
            Predicate result = PredicateMessageUtils.toPredicate(pb, rowType);
            assertThat(result).isInstanceOf(LeafPredicate.class);
            LeafPredicate lp = (LeafPredicate) result;
            assertThat(lp.function()).isEqualTo(predicate.function());
            assertThat(lp.fieldName()).isEqualTo(predicate.fieldName());
            assertThat(lp.index()).isEqualTo(predicate.index());
            assertThat(lp.type().getClass()).isEqualTo(predicate.type().getClass());
            assertThat(lp.literals().get(0)).isNull();
        }
    }

    @Test
    public void testSerializeUsesSchemaFieldIdInsteadOfIndex() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField("id", new IntType(false), 0),
                                new DataField("payload", new StringType(true), 6),
                                new DataField("ts", new TimestampType(false, 3), 9)));
        LeafPredicate predicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        rowType.getTypeAt(1),
                        1,
                        "payload",
                        Collections.singletonList(BinaryString.fromString("foo")));

        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate, rowType);

        assertThat(pb.getLeaf().getFieldId()).isEqualTo(6);
        Predicate result = PredicateMessageUtils.toPredicate(pb, rowType);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        assertThat(leafPredicate.index()).isEqualTo(1);
        assertThat(leafPredicate.fieldName()).isEqualTo("payload");
    }

    @Test
    public void testDeserializeResolvesByFieldIdAcrossSchemaEvolution() {
        RowType originalRowType =
                new RowType(
                        Arrays.asList(
                                new DataField("id", new IntType(false), 0),
                                new DataField("name", new StringType(true), 6),
                                new DataField("score", new BigIntType(true), 9)));
        LeafPredicate predicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        originalRowType.getTypeAt(1),
                        1,
                        "name",
                        Collections.singletonList(BinaryString.fromString("Alice")));

        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate, originalRowType);

        RowType evolvedRowType =
                new RowType(
                        Arrays.asList(
                                new DataField("id", new IntType(false), 0),
                                new DataField("age", new IntType(true), 3),
                                new DataField("full_name", new StringType(true), 6),
                                new DataField("score", new BigIntType(true), 9)));

        Predicate result = PredicateMessageUtils.toPredicate(pb, evolvedRowType);

        assertThat(result).isInstanceOf(LeafPredicate.class);
        LeafPredicate leafPredicate = (LeafPredicate) result;
        assertThat(leafPredicate.index()).isEqualTo(2);
        assertThat(leafPredicate.fieldName()).isEqualTo("full_name");
        assertThat(leafPredicate.type()).isEqualTo(evolvedRowType.getTypeAt(2));
        assertThat(leafPredicate.literals()).containsExactly(BinaryString.fromString("Alice"));
    }

    @Test
    public void testRoundTripAcrossSchemaEvolutionUsesFieldIdAsStableAnchor() {
        RowType sourceRowType =
                new RowType(
                        Arrays.asList(
                                new DataField("id", new IntType(false), 0),
                                new DataField("name", new StringType(true), 6),
                                new DataField("score", new BigIntType(true), 9)));
        LeafPredicate predicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        sourceRowType.getTypeAt(1),
                        1,
                        "name",
                        Collections.singletonList(BinaryString.fromString("Bob")));

        PbPredicate pbPredicate = PredicateMessageUtils.toPbPredicate(predicate, sourceRowType);

        RowType targetRowType =
                new RowType(
                        Arrays.asList(
                                new DataField("score", new BigIntType(true), 9),
                                new DataField("age", new IntType(true), 3),
                                new DataField("customer_name", new StringType(true), 6),
                                new DataField("id", new IntType(false), 0)));

        LeafPredicate restored =
                (LeafPredicate) PredicateMessageUtils.toPredicate(pbPredicate, targetRowType);

        assertThat(pbPredicate.getLeaf().getFieldId()).isEqualTo(6);
        assertThat(restored.index()).isEqualTo(2);
        assertThat(restored.fieldName()).isEqualTo("customer_name");
        assertThat(restored.type()).isEqualTo(targetRowType.getTypeAt(2));
        assertThat(restored.literals()).containsExactly(BinaryString.fromString("Bob"));
    }

    @Test
    public void testDeserializeRejectsIncompatibleEvolvedFieldTypeForSameFieldId() {
        RowType sourceRowType =
                new RowType(
                        Arrays.asList(
                                new DataField("id", new IntType(false), 0),
                                new DataField("name", new StringType(true), 6)));
        LeafPredicate predicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        sourceRowType.getTypeAt(1),
                        1,
                        "name",
                        Collections.singletonList(BinaryString.fromString("Alice")));

        PbPredicate pbPredicate = PredicateMessageUtils.toPbPredicate(predicate, sourceRowType);

        RowType targetRowType =
                new RowType(
                        Arrays.asList(
                                new DataField("id", new IntType(false), 0),
                                new DataField("name_as_bigint", new BigIntType(true), 6)));

        assertThatThrownBy(() -> PredicateMessageUtils.toPredicate(pbPredicate, targetRowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match target schema field type");
    }

    @Test
    public void testDeserializeRejectsMissingFieldIdInTargetSchema() {
        RowType sourceRowType =
                new RowType(
                        Arrays.asList(
                                new DataField("id", new IntType(false), 0),
                                new DataField("name", new StringType(true), 6)));
        LeafPredicate predicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        sourceRowType.getTypeAt(1),
                        1,
                        "name",
                        Collections.singletonList(BinaryString.fromString("Alice")));

        PbPredicate pbPredicate = PredicateMessageUtils.toPbPredicate(predicate, sourceRowType);

        RowType targetRowType =
                new RowType(
                        Arrays.asList(
                                new DataField("id", new IntType(false), 0),
                                new DataField("age", new IntType(true), 3)));

        assertThatThrownBy(() -> PredicateMessageUtils.toPredicate(pbPredicate, targetRowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot resolve field id 6");
    }

    @Test
    public void testSerializeRequiresSchemaFieldId() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField("id", new IntType(false))));
        LeafPredicate predicate =
                new LeafPredicate(
                        Equal.INSTANCE, new IntType(false), 0, "id", Collections.singletonList(1));

        assertThatThrownBy(() -> PredicateMessageUtils.toPbPredicate(predicate, rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not have a valid field id");
    }

    @Test
    public void testSerializeRejectsSchemaMismatch() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField("id", new IntType(false), 0),
                                new DataField("payload", new StringType(true), 6)));
        LeafPredicate wrongNamePredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        rowType.getTypeAt(1),
                        1,
                        "wrong_payload",
                        Collections.singletonList(BinaryString.fromString("foo")));
        LeafPredicate wrongTypePredicate =
                new LeafPredicate(
                        Equal.INSTANCE,
                        new IntType(true),
                        1,
                        "payload",
                        Collections.singletonList(1));

        assertThatThrownBy(() -> PredicateMessageUtils.toPbPredicate(wrongNamePredicate, rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match schema field");
        assertThatThrownBy(() -> PredicateMessageUtils.toPbPredicate(wrongTypePredicate, rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match schema field type");
    }

    @Test
    public void testTimestampLiteralFieldNumbersFollowReviewerLayout() throws Exception {
        assertThat(getPbLiteralValueFieldNumber("_TIMESTAMP_MILLIS_VALUE_FIELD_NUMBER"))
                .isEqualTo(11);
        assertThat(getPbLiteralValueFieldNumber("_TIMESTAMP_NANO_OF_MILLIS_VALUE_FIELD_NUMBER"))
                .isEqualTo(12);
        assertThat(getPbLiteralValueFieldNumber("_DECIMAL_BYTES_FIELD_NUMBER")).isEqualTo(13);
    }

    private static int getPbLiteralValueFieldNumber(String fieldName) throws Exception {
        Field field =
                org.apache.fluss.rpc.messages.PbLiteralValue.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getInt(null);
    }
}
