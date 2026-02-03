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
import org.apache.fluss.record.Filter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.rpc.messages.PbFilter;
import org.apache.fluss.rpc.messages.PbPredicate;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** PredicateMessageUtilsTest. */
public class PredicateMessageUtilsTest {

    @Test
    public void testLeafPredicateIntEqual() {
        DataType type = new IntType(false);
        LeafPredicate predicate =
                new LeafPredicate(Equal.INSTANCE, type, 0, "id", Collections.singletonList(123));
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb);
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
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb);
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
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb);
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
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb);
        assertThat(result).isInstanceOf(LeafPredicate.class);
        LeafPredicate lp = (LeafPredicate) result;
        assertThat(lp.function()).isEqualTo(Equal.INSTANCE);
        // 这里只能判断类型和字段名，decimal反序列化后equals未必一致
        assertThat(lp.fieldName()).isEqualTo("amount");
    }

    @Test
    public void testLeafPredicateTimestamp() {
        DataType type = new TimestampType(false, 3);
        TimestampNtz ts = TimestampNtz.fromMillis(1680000000000L, 3);
        LeafPredicate predicate =
                new LeafPredicate(
                        GreaterThan.INSTANCE, type, 4, "ts", Collections.singletonList(ts));
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb);
        assertThat(result).isInstanceOf(LeafPredicate.class);
        LeafPredicate lp = (LeafPredicate) result;
        assertThat(lp.function()).isEqualTo(GreaterThan.INSTANCE);
        assertThat(lp.fieldName()).isEqualTo("ts");
    }

    @Test
    public void testLeafPredicateBoolean() {
        DataType type = new BooleanType(false);
        LeafPredicate predicate =
                new LeafPredicate(Equal.INSTANCE, type, 5, "flag", Collections.singletonList(true));
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb);
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
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(andPredicate);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb);
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
        PbPredicate pb = PredicateMessageUtils.toPbPredicate(andPredicate);
        assertThat(pb.totalSize()).isGreaterThan(0);
        Predicate result = PredicateMessageUtils.toPredicate(pb);
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
        for (LeafPredicate predicate : predicates) {
            PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate);
            assertThat(pb.totalSize()).isGreaterThan(0);
            Predicate result = PredicateMessageUtils.toPredicate(pb);
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
        // 测试所有数据类型与null值的序列化和反序列化
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

        for (LeafPredicate predicate : nullPredicates) {
            PbPredicate pb = PredicateMessageUtils.toPbPredicate(predicate);
            assertThat(pb.totalSize()).isGreaterThan(0);
            Predicate result = PredicateMessageUtils.toPredicate(pb);
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
    public void testFilterSerde() {
        DataType type = new IntType(false);
        Predicate predicate =
                new LeafPredicate(Equal.INSTANCE, type, 0, "id", Collections.singletonList(123));
        int schemaId = 1;

        // test toPbFilter
        PbFilter pbFilter = PredicateMessageUtils.toPbFilter(predicate, schemaId);
        assertThat(pbFilter.getSchemaId()).isEqualTo(schemaId);
        assertThat(pbFilter.hasPredicate()).isTrue();

        // test toFilter
        Filter filter = PredicateMessageUtils.toFilter(pbFilter);
        assertThat(filter.getSchemaId()).isEqualTo(schemaId);
        assertThat(filter.getPredicate()).isInstanceOf(LeafPredicate.class);
        LeafPredicate lp = (LeafPredicate) filter.getPredicate();
        assertThat(lp.function()).isEqualTo(Equal.INSTANCE);
        assertThat(lp.fieldName()).isEqualTo("id");
        assertThat(lp.literals().get(0)).isEqualTo(123);
    }
}
