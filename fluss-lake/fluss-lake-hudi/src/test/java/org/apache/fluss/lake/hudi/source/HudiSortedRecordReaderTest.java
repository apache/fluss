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

package org.apache.fluss.lake.hudi.source;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampNtz;

import org.apache.hudi.org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HudiSortedRecordReader}. */
class HudiSortedRecordReaderTest {

    @Test
    void testPrimaryKeyComparatorUsesProjectedPrimaryKeyOrder() {
        Comparator<InternalRow> comparator =
                HudiSortedRecordReader.createPrimaryKeyComparator(
                        hudiSchema(), new String[] {"name", "id"});

        GenericRow rowA1 = GenericRow.of(BinaryString.fromString("a"), 1L);
        GenericRow rowA2 = GenericRow.of(BinaryString.fromString("a"), 2L);
        GenericRow rowB1 = GenericRow.of(BinaryString.fromString("b"), 1L);

        assertThat(comparator.compare(rowA1, rowA2)).isLessThan(0);
        assertThat(comparator.compare(rowA2, rowA1)).isGreaterThan(0);
        assertThat(comparator.compare(rowA2, rowB1)).isLessThan(0);
        assertThat(comparator.compare(rowA2, GenericRow.of(BinaryString.fromString("a"), 2L)))
                .isZero();
    }

    @Test
    void testPrimaryKeyComparatorOrdersNullsFirst() {
        Comparator<InternalRow> comparator =
                HudiSortedRecordReader.createPrimaryKeyComparator(
                        hudiSchema(), new String[] {"name", "id"});

        GenericRow nullName = GenericRow.of(null, 1L);
        GenericRow nonNullName = GenericRow.of(BinaryString.fromString("a"), 1L);

        assertThat(comparator.compare(nullName, nonNullName)).isLessThan(0);
        assertThat(comparator.compare(nonNullName, nullName)).isGreaterThan(0);
        assertThat(comparator.compare(nullName, GenericRow.of(null, 1L))).isZero();
    }

    @Test
    void testPrimaryKeyComparatorSupportsDecimalAndTimestamp() {
        Comparator<InternalRow> comparator =
                HudiSortedRecordReader.createPrimaryKeyComparator(
                        hudiSchema(), new String[] {"amount", "event_time"});

        GenericRow smallAmount =
                GenericRow.of(
                        Decimal.fromBigDecimal(new BigDecimal("10.00"), 10, 2),
                        TimestampNtz.fromMillis(2000L));
        GenericRow largeAmount =
                GenericRow.of(
                        Decimal.fromBigDecimal(new BigDecimal("20.00"), 10, 2),
                        TimestampNtz.fromMillis(1000L));
        GenericRow earlierTime =
                GenericRow.of(
                        Decimal.fromBigDecimal(new BigDecimal("10.00"), 10, 2),
                        TimestampNtz.fromMillis(1000L));

        assertThat(comparator.compare(smallAmount, largeAmount)).isLessThan(0);
        assertThat(comparator.compare(earlierTime, smallAmount)).isLessThan(0);
        assertThat(
                        comparator.compare(
                                smallAmount,
                                GenericRow.of(smallAmount.getField(0), smallAmount.getField(1))))
                .isZero();
    }

    @Test
    void testPrimaryKeyComparatorFailsForMissingPrimaryKeyField() {
        assertThatThrownBy(
                        () ->
                                HudiSortedRecordReader.createPrimaryKeyComparator(
                                        hudiSchema(), new String[] {"missing"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing")
                .hasMessageContaining("does not exist");
    }

    @Test
    void testPrimaryKeyComparatorFailsForUnsupportedPrimaryKeyType() {
        assertThatThrownBy(
                        () ->
                                HudiSortedRecordReader.createPrimaryKeyComparator(
                                        hudiSchema(), new String[] {"tags"}))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("ARRAY");
    }

    private static Schema hudiSchema() {
        return new Schema.Parser()
                .parse(
                        "{"
                                + "\"type\":\"record\","
                                + "\"name\":\"hudi_record\","
                                + "\"fields\":["
                                + "{\"name\":\"id\",\"type\":\"long\"},"
                                + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],"
                                + "\"default\":null},"
                                + "{\"name\":\"amount\","
                                + "\"type\":{\"type\":\"bytes\","
                                + "\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}},"
                                + "{\"name\":\"event_time\","
                                + "\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
                                + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}"
                                + "]"
                                + "}");
    }
}
