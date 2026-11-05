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

package org.apache.fluss.server.kv;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.indexedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BytewiseRowEqualizer}. */
class BytewiseRowEqualizerTest {

    private static final short SCHEMA_ID = 1;

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testIdenticalSerializedRowsAndDifferentValues(KvFormat format) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        BinaryValue left = value(SCHEMA_ID, schema, format, 1, "same");
        BinaryValue same = value(SCHEMA_ID, schema, format, 1, "same");
        BinaryValue changed = value(SCHEMA_ID, schema, format, 1, "different");

        assertThat(BytewiseRowEqualizer.equals(left, same)).isTrue();
        assertThat(BytewiseRowEqualizer.equals(left, left)).isTrue();
        assertThat(BytewiseRowEqualizer.equals(same, left)).isTrue();
        assertThat(BytewiseRowEqualizer.equals(left, changed)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testSchemaIdIsPartOfEquality(KvFormat format) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        BinaryValue stored = value(SCHEMA_ID, schema, format, 1, "same");
        BinaryValue newSchema = value((short) 2, schema, format, 1, "same");

        assertThat(BytewiseRowEqualizer.equals(stored, newSchema)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testSignedZeroHasDifferentSerializedBytes(KvFormat format) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("float_value", DataTypes.FLOAT())
                        .column("double_value", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();
        BinaryValue negative = value(SCHEMA_ID, schema, format, 1, -0.0f, -0.0d);
        BinaryValue positive = value(SCHEMA_ID, schema, format, 1, +0.0f, +0.0d);

        assertThat(BytewiseRowEqualizer.equals(negative, positive)).isFalse();
        assertThat(
                        BytewiseRowEqualizer.equals(
                                negative, value(SCHEMA_ID, schema, format, 1, -0.0f, -0.0d)))
                .isTrue();
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testMapEntryOrderHasDifferentSerializedBytes(KvFormat format) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .primaryKey("id")
                        .build();
        BinaryValue stored =
                value(
                        SCHEMA_ID,
                        schema,
                        format,
                        1,
                        GenericMap.of(
                                BinaryString.fromString("a"), 1, BinaryString.fromString("b"), 2));
        BinaryValue reordered =
                value(
                        SCHEMA_ID,
                        schema,
                        format,
                        1,
                        GenericMap.of(
                                BinaryString.fromString("b"), 2, BinaryString.fromString("a"), 1));

        assertThat(BytewiseRowEqualizer.equals(stored, reordered)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testEqualRowsWithDifferentSegmentOffsets(KvFormat format) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        BinaryValue left = value(SCHEMA_ID, schema, format, 1, "same");
        int size = left.row.getSizeInBytes();
        byte[] shiftedBytes = new byte[size + 3];
        left.row.copyTo(shiftedBytes, 2);
        BinaryRow shiftedRow = left.row.copy();
        shiftedRow.pointTo(MemorySegment.wrap(shiftedBytes), 2, size);

        assertThat(BytewiseRowEqualizer.equals(left, new BinaryValue(SCHEMA_ID, shiftedRow)))
                .isTrue();
        shiftedBytes[2] ^= 1;
        assertThat(BytewiseRowEqualizer.equals(left, new BinaryValue(SCHEMA_ID, shiftedRow)))
                .isFalse();
    }

    private static BinaryValue value(
            short schemaId, Schema schema, KvFormat format, Object... fields) {
        BinaryRow row =
                format == KvFormat.COMPACTED
                        ? compactedRow(schema.getRowType(), fields)
                        : indexedRow(schema.getRowType(), fields);
        return new BinaryValue(schemaId, row);
    }
}
