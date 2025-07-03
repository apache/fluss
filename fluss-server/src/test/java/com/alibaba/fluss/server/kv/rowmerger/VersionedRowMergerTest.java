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

package com.alibaba.fluss.server.kv.rowmerger;

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link VersionedRowMerger}. */
class VersionedRowMergerTest {

    static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of(
                        DataTypes.INT(),
                        Arrays.asList(
                                TestSpec.of(1, 2, "new"),
                                TestSpec.of(2, 1, "old"),
                                TestSpec.of(1, 1, "new"))),
                Arguments.of(
                        DataTypes.BIGINT(),
                        Arrays.asList(
                                TestSpec.of(1L, 2L, "new"),
                                TestSpec.of(2L, 1L, "old"),
                                TestSpec.of(1L, 1L, "new"))),
                Arguments.of(
                        DataTypes.TIMESTAMP(),
                        Arrays.asList(
                                TestSpec.of(
                                        timestampNtz("2025-01-10T12:00:00.123"),
                                        timestampNtz("2025-01-10T12:00:10.123"),
                                        "new"),
                                TestSpec.of(
                                        timestampNtz("2025-01-10T12:00:00.123"),
                                        timestampNtz("2025-01-10T12:00:00.122"),
                                        "old"),
                                TestSpec.of(
                                        timestampNtz("2025-01-10T12:00:00.123"),
                                        timestampNtz("2025-01-10T12:00:00.123"),
                                        "new"))),
                Arguments.of(
                        DataTypes.TIMESTAMP(9),
                        Arrays.asList(
                                TestSpec.of(
                                        timestampNtz("2025-01-10T12:00:00.123456789"),
                                        timestampNtz("2025-01-10T12:00:10.123456789"),
                                        "new"),
                                TestSpec.of(
                                        timestampNtz("2025-01-10T12:00:00.123456789"),
                                        timestampNtz("2025-01-10T12:00:00.123456788"),
                                        "old"),
                                TestSpec.of(
                                        timestampNtz("2025-01-10T12:00:00.123456789"),
                                        timestampNtz("2025-01-10T12:00:00.123456789"),
                                        "new"))),
                Arguments.of(
                        DataTypes.TIMESTAMP_LTZ(),
                        Arrays.asList(
                                TestSpec.of(
                                        timestampLtz("2025-01-10T12:00:00.123"),
                                        timestampLtz("2025-01-10T12:00:10.123"),
                                        "new"),
                                TestSpec.of(
                                        timestampLtz("2025-01-10T12:00:00.123"),
                                        timestampLtz("2025-01-10T12:00:00.122"),
                                        "old"),
                                TestSpec.of(
                                        timestampLtz("2025-01-10T12:00:00.123"),
                                        timestampLtz("2025-01-10T12:00:00.123"),
                                        "new"))),
                Arguments.of(
                        DataTypes.TIMESTAMP_LTZ(9),
                        Arrays.asList(
                                TestSpec.of(
                                        timestampLtz("2025-01-10T12:00:00.123456789"),
                                        timestampLtz("2025-01-10T12:00:10.123456789"),
                                        "new"),
                                TestSpec.of(
                                        timestampLtz("2025-01-10T12:00:00.123456789"),
                                        timestampLtz("2025-01-10T12:00:00.123456788"),
                                        "old"),
                                TestSpec.of(
                                        timestampLtz("2025-01-10T12:00:00.123456789"),
                                        timestampLtz("2025-01-10T12:00:00.123456789"),
                                        "new"))));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testAllTypes(DataType type, List<TestSpec> testSpecs) {
        RowType schema =
                Schema.newBuilder()
                        .column("a", type)
                        .column("b", DataTypes.STRING())
                        .build()
                        .getRowType();
        VersionedRowMerger merger = new VersionedRowMerger(schema, "a");

        for (TestSpec testSpec : testSpecs) {
            BinaryRow oldRow = compactedRow(schema, new Object[] {testSpec.oldValue, "dummy"});
            BinaryRow newRow = compactedRow(schema, new Object[] {testSpec.newValue, "dummy"});
            BinaryRow mergedRow = merger.merge(oldRow, newRow);
            if (testSpec.expected.equals("old")) {
                assertThat(mergedRow).isSameAs(oldRow);
            } else if (testSpec.expected.equals("new")) {
                assertThat(mergedRow).isSameAs(newRow);
            } else {
                throw new IllegalArgumentException("Unknown expected value: " + testSpec.expected);
            }
        }
    }

    @Test
    void testNormal() {
        RowType schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build()
                        .getRowType();
        VersionedRowMerger merger = new VersionedRowMerger(schema, "a");

        assertThat(merger.supportsDelete()).isFalse();
        assertThat(merger.configureTargetColumns(null)).isSameAs(merger);
    }

    private static TimestampNtz timestampNtz(String timestamp) {
        return TimestampNtz.fromLocalDateTime(LocalDateTime.parse(timestamp));
    }

    private static TimestampLtz timestampLtz(String timestamp) {
        Instant instant = LocalDateTime.parse(timestamp).toInstant(ZoneOffset.UTC);
        return TimestampLtz.fromInstant(instant);
    }

    /** Test specification for {@link VersionedRowMerger}. */
    private static class TestSpec {
        private final Object oldValue;
        private final Object newValue;
        private final String expected;

        private TestSpec(Object oldValue, Object newValue, String expected) {
            this.oldValue = oldValue;
            this.newValue = newValue;
            this.expected = expected;
        }

        public static TestSpec of(Object oldValue, Object newValue, String expected) {
            return new TestSpec(oldValue, newValue, expected);
        }
    }
}
