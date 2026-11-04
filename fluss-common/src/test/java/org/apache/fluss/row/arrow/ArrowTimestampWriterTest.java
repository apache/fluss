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

package org.apache.fluss.row.arrow;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.arrow.writers.ArrowFieldWriter;
import org.apache.fluss.row.columnar.ColumnVector;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies timestamp range checks and round trips through Arrow vectors. */
class ArrowTimestampWriterTest {

    @ParameterizedTest
    @MethodSource("timestampWriters")
    void testRoundTripsTimestampBoundaries(int precision, boolean withZone, boolean handleSafe) {
        DataType type =
                withZone ? DataTypes.TIMESTAMP_LTZ(precision) : DataTypes.TIMESTAMP(precision);
        RowType rowType = DataTypes.ROW(type);
        long unitsPerMilli = precision == 6 ? 1_000 : 1_000_000;
        int nanosPerUnit = precision == 6 ? 1_000 : 1;
        try (RootAllocator allocator = new RootAllocator(1024 * 1024);
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator)) {
            root.allocateNew();
            TimeStampVector vector = (TimeStampVector) root.getVector(0);
            ArrowFieldWriter writer = ArrowUtils.createArrowFieldWriter(vector, type);
            long[] values = {
                Long.MIN_VALUE,
                -1_000_001,
                -unitsPerMilli,
                -unitsPerMilli + 1,
                -1,
                0,
                1,
                1_000_001,
                Long.MAX_VALUE
            };
            for (int i = 0; i < values.length; i++) {
                long millis = Math.floorDiv(values[i], unitsPerMilli);
                int nanos = (int) Math.floorMod(values[i], unitsPerMilli) * nanosPerUnit;
                writer.write(i, timestampRow(millis, nanos, withZone), 0, handleSafe);
                assertThat(vector.get(i)).isEqualTo(values[i]);
            }
            root.setRowCount(values.length);
            ArrowReader reader =
                    new ArrowReader(
                            new ColumnVector[] {ArrowUtils.createArrowColumnVector(vector, type)},
                            root.getRowCount());
            for (int i = 0; i < values.length; i++) {
                long millis = Math.floorDiv(values[i], unitsPerMilli);
                int nanos = (int) Math.floorMod(values[i], unitsPerMilli) * nanosPerUnit;
                ColumnarRow row = reader.read(i);
                if (withZone) {
                    assertThat(row.getTimestampLtz(0, precision))
                            .isEqualTo(TimestampLtz.fromEpochMillis(millis, nanos));
                } else {
                    assertThat(row.getTimestampNtz(0, precision))
                            .isEqualTo(TimestampNtz.fromMillis(millis, nanos));
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("timestampWriters")
    void testRejectsOverflowBeforeChangingVector(
            int precision, boolean withZone, boolean handleSafe) {
        DataType type =
                withZone ? DataTypes.TIMESTAMP_LTZ(precision) : DataTypes.TIMESTAMP(precision);
        RowType rowType = DataTypes.ROW(type);
        long unitsPerMilli = precision == 6 ? 1_000 : 1_000_000;
        int nanosPerUnit = precision == 6 ? 1_000 : 1;
        try (RootAllocator allocator = new RootAllocator(1024 * 1024);
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator)) {
            root.allocateNew();
            TimeStampVector vector = (TimeStampVector) root.getVector(0);
            ArrowFieldWriter writer = ArrowUtils.createArrowFieldWriter(vector, type);
            vector.set(0, 42);
            for (long limit : new long[] {Long.MIN_VALUE, Long.MAX_VALUE}) {
                long millis = Math.floorDiv(limit, unitsPerMilli);
                int nanos = (int) Math.floorMod(limit, unitsPerMilli) * nanosPerUnit;
                int outsideNanos = nanos + (limit < 0 ? -nanosPerUnit : nanosPerUnit);
                assertThatThrownBy(
                                () ->
                                        writer.write(
                                                0,
                                                timestampRow(millis, outsideNanos, withZone),
                                                0,
                                                handleSafe))
                        .isInstanceOf(ArithmeticException.class);
                long outsideMillis = millis + (limit < 0 ? -1 : 1);
                assertThatThrownBy(
                                () ->
                                        writer.write(
                                                0,
                                                timestampRow(outsideMillis, nanos, withZone),
                                                0,
                                                handleSafe))
                        .isInstanceOf(ArithmeticException.class);
                assertThat(vector.get(0)).isEqualTo(42);
            }
        }
    }

    private static GenericRow timestampRow(long millis, int nanos, boolean withZone) {
        return GenericRow.of(
                withZone
                        ? TimestampLtz.fromEpochMillis(millis, nanos)
                        : TimestampNtz.fromMillis(millis, nanos));
    }

    private static Stream<Arguments> timestampWriters() {
        return Stream.of(
                Arguments.of(6, false, false), Arguments.of(6, false, true),
                Arguments.of(6, true, false), Arguments.of(6, true, true),
                Arguments.of(9, false, false), Arguments.of(9, false, true),
                Arguments.of(9, true, false), Arguments.of(9, true, true));
    }
}
