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

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.indexedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LogicalRowEqualizer}. */
class LogicalRowEqualizerTest {

    private static final short SCHEMA_ID = 1;

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testFloatAndDoubleLogicalEquality(KvFormat format) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("float_value", DataTypes.FLOAT())
                        .column("double_value", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();
        LogicalRowEqualizer equalizer = equalizer(schema);

        BinaryValue left =
                value(
                        SCHEMA_ID,
                        schema,
                        format,
                        1,
                        -0.0f,
                        Double.longBitsToDouble(0x7ff8000000000001L));
        BinaryValue right = value(SCHEMA_ID, schema, format, 1, +0.0f, Double.NaN);
        assertThat(equalizer.equals(left, right)).isTrue();

        BinaryValue changed = value(SCHEMA_ID, schema, format, 1, 0.25f, Double.NaN);
        assertThat(equalizer.equals(left, changed)).isFalse();
        assertThat(
                        equalizer.equals(
                                value(SCHEMA_ID, schema, format, 1, Float.NaN, 0.0d),
                                value(SCHEMA_ID, schema, format, 1, Float.NaN, -0.0d)))
                .isTrue();
    }

    @Test
    void testNestedArraysRowsAndBinaryContent() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("array", DataTypes.ARRAY(DataTypes.BYTES()))
                        .column(
                                "nested",
                                DataTypes.ROW(
                                        DataTypes.FIELD("payload", DataTypes.BYTES()),
                                        DataTypes.FIELD(
                                                "numbers", DataTypes.ARRAY(DataTypes.FLOAT()))))
                        .primaryKey("id")
                        .build();
        LogicalRowEqualizer equalizer = equalizer(schema);

        BinaryValue left =
                value(
                        SCHEMA_ID,
                        schema,
                        KvFormat.COMPACTED,
                        1,
                        new GenericArray(new Object[] {new byte[] {1, 2}, null}),
                        GenericRow.of(
                                new byte[] {3, 4},
                                new GenericArray(new Object[] {-0.0f, Float.NaN})));
        BinaryValue right =
                value(
                        SCHEMA_ID,
                        schema,
                        KvFormat.COMPACTED,
                        1,
                        new GenericArray(new Object[] {new byte[] {1, 2}, null}),
                        GenericRow.of(
                                new byte[] {3, 4},
                                new GenericArray(new Object[] {+0.0f, Float.NaN})));
        assertThat(equalizer.equals(left, right)).isTrue();

        BinaryValue changed =
                value(
                        SCHEMA_ID,
                        schema,
                        KvFormat.COMPACTED,
                        1,
                        new GenericArray(new Object[] {new byte[] {1, 2}, null}),
                        GenericRow.of(
                                new byte[] {3, 5},
                                new GenericArray(new Object[] {+0.0f, Float.NaN})));
        assertThat(equalizer.equals(left, changed)).isFalse();

        BinaryValue shorterArray =
                value(
                        SCHEMA_ID,
                        schema,
                        KvFormat.COMPACTED,
                        1,
                        new GenericArray(new Object[] {new byte[] {1, 2}}),
                        GenericRow.of(
                                new byte[] {3, 4},
                                new GenericArray(new Object[] {+0.0f, Float.NaN})));
        assertThat(equalizer.equals(left, shorterArray)).isFalse();
    }

    @Test
    void testMapOrderAndNestedValues() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column(
                                "attributes",
                                DataTypes.MAP(
                                        DataTypes.STRING(), DataTypes.ARRAY(DataTypes.BYTES())))
                        .primaryKey("id")
                        .build();
        LogicalRowEqualizer equalizer = equalizer(schema);

        BinaryValue left =
                value(
                        SCHEMA_ID,
                        schema,
                        KvFormat.COMPACTED,
                        1,
                        GenericMap.of(
                                key("a"),
                                new GenericArray(new Object[] {new byte[] {1}, null}),
                                key("b"),
                                new GenericArray(new Object[] {new byte[] {2}})));
        BinaryValue reordered =
                value(
                        SCHEMA_ID,
                        schema,
                        KvFormat.COMPACTED,
                        1,
                        GenericMap.of(
                                key("b"),
                                new GenericArray(new Object[] {new byte[] {2}}),
                                key("a"),
                                new GenericArray(new Object[] {new byte[] {1}, null})));
        assertThat(equalizer.equals(left, reordered)).isTrue();

        BinaryValue changed =
                value(
                        SCHEMA_ID,
                        schema,
                        KvFormat.COMPACTED,
                        1,
                        GenericMap.of(
                                key("b"),
                                new GenericArray(new Object[] {new byte[] {3}}),
                                key("a"),
                                new GenericArray(new Object[] {new byte[] {1}, null})));
        assertThat(equalizer.equals(left, changed)).isFalse();

        BinaryValue differentKey =
                value(
                        SCHEMA_ID,
                        schema,
                        KvFormat.COMPACTED,
                        1,
                        GenericMap.of(
                                key("c"),
                                new GenericArray(new Object[] {new byte[] {2}}),
                                key("a"),
                                new GenericArray(new Object[] {new byte[] {1}, null})));
        assertThat(equalizer.equals(left, differentKey)).isFalse();
    }

    @Test
    void testMapFloatKeysUseLogicalEquality() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("attributes", DataTypes.MAP(DataTypes.FLOAT(), DataTypes.INT()))
                        .primaryKey("id")
                        .build();
        LogicalRowEqualizer equalizer = equalizer(schema);

        BinaryValue left = value(SCHEMA_ID, schema, KvFormat.COMPACTED, 1, GenericMap.of(-0.0f, 7));
        BinaryValue same = value(SCHEMA_ID, schema, KvFormat.COMPACTED, 1, GenericMap.of(+0.0f, 7));
        BinaryValue changed =
                value(SCHEMA_ID, schema, KvFormat.COMPACTED, 1, GenericMap.of(1.0f, 7));
        assertThat(equalizer.equals(left, same)).isTrue();
        assertThat(equalizer.equals(left, changed)).isFalse();
    }

    @Test
    void testAddedAndDroppedColumnsAlignById() {
        Schema oldSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("removed", DataTypes.STRING())
                        .column("payload", DataTypes.BYTES())
                        .primaryKey("id")
                        .build();
        Schema latestSchema =
                Schema.newBuilder()
                        .fromColumns(
                                Arrays.asList(
                                        oldSchema.getColumn("id"),
                                        oldSchema.getColumn("payload"),
                                        new Schema.Column(
                                                "added", DataTypes.INT(), null, (short) 3)))
                        .primaryKey("id")
                        .build();
        short latestSchemaId = 2;
        TestingSchemaGetter getter = new TestingSchemaGetter(new SchemaInfo(oldSchema, SCHEMA_ID));
        getter.updateLatestSchemaInfo(new SchemaInfo(latestSchema, latestSchemaId));
        LogicalRowEqualizer equalizer = new LogicalRowEqualizer(getter, latestSchema);

        BinaryValue old =
                value(SCHEMA_ID, oldSchema, KvFormat.COMPACTED, 1, "ignored", new byte[] {7});
        BinaryValue same =
                value(latestSchemaId, latestSchema, KvFormat.COMPACTED, 1, new byte[] {7}, null);
        assertThat(equalizer.equals(old, same)).isTrue();
        assertThat(equalizer.equals(same, old)).isTrue();

        BinaryValue changed =
                value(latestSchemaId, latestSchema, KvFormat.COMPACTED, 1, new byte[] {7}, 2);
        assertThat(equalizer.equals(old, changed)).isFalse();
    }

    private static LogicalRowEqualizer equalizer(Schema schema) {
        return new LogicalRowEqualizer(
                new TestingSchemaGetter(new SchemaInfo(schema, SCHEMA_ID)), schema);
    }

    private static BinaryString key(String string) {
        return BinaryString.fromString(string);
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
