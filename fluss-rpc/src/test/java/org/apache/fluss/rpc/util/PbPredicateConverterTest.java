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
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.LessThan;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.rpc.messages.PbCompoundFunction;
import org.apache.fluss.rpc.messages.PbLeafFunction;
import org.apache.fluss.rpc.messages.PbPredicate;
import org.apache.fluss.rpc.messages.PbPredicateType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PbPredicateConverter}. */
public class PbPredicateConverterTest {

    @Test
    public void testVisitLeafPredicate() {
        DataType type = new IntType(false);
        LeafPredicate predicate =
                new LeafPredicate(Equal.INSTANCE, type, 0, "id", Collections.singletonList(123));
        PbPredicate pb = predicate.visit(PbPredicateConverter.INSTANCE);

        assertThat(pb.getType()).isEqualTo(PbPredicateType.LEAF);
        assertThat(pb.getLeaf().getFunction()).isEqualTo(PbLeafFunction.EQUAL);
        assertThat(pb.getLeaf().getFieldRef().getFieldName()).isEqualTo("id");
        assertThat(pb.getLeaf().getLiteralsList().get(0).getIntValue()).isEqualTo(123);
    }

    @Test
    public void testVisitCompoundPredicate() {
        DataType type = new IntType(false);
        LeafPredicate p1 =
                new LeafPredicate(
                        GreaterThan.INSTANCE, type, 0, "id", Collections.singletonList(10));
        LeafPredicate p2 =
                new LeafPredicate(LessThan.INSTANCE, type, 0, "id", Collections.singletonList(100));
        CompoundPredicate andPredicate = new CompoundPredicate(And.INSTANCE, Arrays.asList(p1, p2));

        PbPredicate pb = andPredicate.visit(PbPredicateConverter.INSTANCE);

        assertThat(pb.getType()).isEqualTo(PbPredicateType.COMPOUND);
        assertThat(pb.getCompound().getFunction()).isEqualTo(PbCompoundFunction.AND);
        assertThat(pb.getCompound().getChildrensCount()).isEqualTo(2);
    }

    @Test
    public void testToPbLiteralValue() {
        // Test various types conversion to PbLiteralValue via LeafPredicate visit
        // boolean
        verifyLiteral(new BooleanType(false), true, pb -> assertThat(pb.isBooleanValue()).isTrue());
        // int
        verifyLiteral(new IntType(false), 123, pb -> assertThat(pb.getIntValue()).isEqualTo(123));
        // bigint
        verifyLiteral(
                new BigIntType(false),
                1234567890123L,
                pb -> assertThat(pb.getBigintValue()).isEqualTo(1234567890123L));
        // float
        verifyLiteral(
                new FloatType(false), 1.23f, pb -> assertThat(pb.getFloatValue()).isEqualTo(1.23f));
        // double
        verifyLiteral(
                new DoubleType(false),
                2.34d,
                pb -> assertThat(pb.getDoubleValue()).isEqualTo(2.34d));
        // string
        verifyLiteral(
                new StringType(false),
                BinaryString.fromString("hello"),
                pb -> assertThat(pb.getStringValue()).isEqualTo("hello"));
        // decimal
        Decimal decimal = Decimal.fromBigDecimal(new BigDecimal("1234.56"), 10, 2);
        verifyLiteral(
                new DecimalType(false, 10, 2),
                decimal,
                pb -> {
                    if (decimal.isCompact()) {
                        assertThat(pb.getDecimalValue()).isEqualTo(decimal.toUnscaledLong());
                    } else {
                        assertThat(pb.getDecimalBytes()).isEqualTo(decimal.toUnscaledBytes());
                    }
                });
        // date
        LocalDate date = LocalDate.ofEpochDay(19000L);
        verifyLiteral(
                new DateType(false),
                date,
                pb -> assertThat(pb.getBigintValue()).isEqualTo(date.toEpochDay()));
        // time
        LocalTime time = LocalTime.of(12, 30);
        verifyLiteral(
                new TimeType(false, 3),
                time,
                pb ->
                        assertThat(pb.getIntValue())
                                .isEqualTo((int) (time.toNanoOfDay() / 1_000_000L)));
        // timestamp
        TimestampNtz ts = TimestampNtz.fromMillis(1680000000000L, 3);
        verifyLiteral(
                new TimestampType(false, 3),
                ts,
                pb -> {
                    assertThat(pb.getTimestampMillisValue()).isEqualTo(ts.getMillisecond());
                    assertThat(pb.getTimestampNanoOfMillisValue())
                            .isEqualTo(ts.getNanoOfMillisecond());
                });
        // timestamp_ltz
        TimestampLtz ltz = TimestampLtz.fromEpochMillis(1680000000000L, 3);
        verifyLiteral(
                new LocalZonedTimestampType(false, 3),
                ltz,
                pb -> {
                    assertThat(pb.getTimestampMillisValue()).isEqualTo(ltz.getEpochMillisecond());
                    assertThat(pb.getTimestampNanoOfMillisValue())
                            .isEqualTo(ltz.getNanoOfMillisecond());
                });
        // binary
        byte[] binary = new byte[] {1, 2, 3, 4};
        verifyLiteral(
                new BinaryType(4), binary, pb -> assertThat(pb.getBinaryValue()).isEqualTo(binary));
        // tinyint
        verifyLiteral(
                new TinyIntType(false), (byte) 7, pb -> assertThat(pb.getIntValue()).isEqualTo(7));
        // smallint
        verifyLiteral(
                new SmallIntType(false),
                (short) 1234,
                pb -> assertThat(pb.getIntValue()).isEqualTo(1234));
        // null
        verifyLiteral(new IntType(true), null, pb -> assertThat(pb.isIsNull()).isTrue());
    }

    private void verifyLiteral(
            DataType type,
            Object value,
            java.util.function.Consumer<org.apache.fluss.rpc.messages.PbLiteralValue> assertion) {
        LeafPredicate predicate =
                new LeafPredicate(Equal.INSTANCE, type, 0, "f", Collections.singletonList(value));
        PbPredicate pb = predicate.visit(PbPredicateConverter.INSTANCE);
        assertion.accept(pb.getLeaf().getLiteralsList().get(0));
    }
}
