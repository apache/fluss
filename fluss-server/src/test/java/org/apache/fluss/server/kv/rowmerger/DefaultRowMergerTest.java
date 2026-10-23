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

package org.apache.fluss.server.kv.rowmerger;

import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.indexedRow;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultRowMerger} delete behavior functionality. */
class DefaultRowMergerTest {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final Schema SCHEMA_2 =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("id", DataTypes.INT(), null, (short) 0),
                                    new Schema.Column("name", DataTypes.STRING(), null, (short) 1),
                                    // add new column at end
                                    new Schema.Column("age", DataTypes.STRING(), null, (short) 2)))
                    .primaryKey("id")
                    .build();

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testIgnoreNullsPreservesColumnsAndDeleteSemantics(KvFormat format) {
        RowMerger root = ignoreNullsMerger(format, SCHEMA_2);
        RowMerger full = root.configureTargetColumns(null, (short) 2, SCHEMA_2);
        BinaryValue first = value(format, SCHEMA_2, 1, "Alice", null);
        BinaryValue second = value(format, SCHEMA_2, 1, null, "20");
        BinaryValue merged = full.merge(full.merge(null, first), second);
        assertThat(merged).isEqualTo(value(format, SCHEMA_2, 1, "Alice", "20"));
        assertThat(full.merge(merged, value(format, SCHEMA_2, 1, null, null))).isEqualTo(merged);
        assertThat(full.delete(merged)).isNull();
        assertThat(full.merge(null, value(format, SCHEMA_2, 1, null, null)))
                .isEqualTo(value(format, SCHEMA_2, 1, null, null));

        RowMerger explicitFull =
                root.configureTargetColumns(new int[] {2, 0, 1}, (short) 2, SCHEMA_2);
        assertThat(explicitFull.merge(first, second)).isEqualTo(merged);
        assertThat(explicitFull.delete(merged)).isNull();

        RowMerger partial = root.configureTargetColumns(new int[] {0, 2}, (short) 2, SCHEMA_2);
        assertThat(partial.merge(merged, value(format, SCHEMA_2, 1, "ignored", null)))
                .isEqualTo(merged);
        assertThat(partial.merge(merged, value(format, SCHEMA_2, 1, "ignored", "21")))
                .isEqualTo(value(format, SCHEMA_2, 1, "Alice", "21"));
        assertThat(partial.delete(merged)).isEqualTo(first);
        assertThat(partial.delete(second)).isNull();
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testIgnoreNullsAcrossSchemaEvolution(KvFormat format) {
        RowMerger root = ignoreNullsMerger(format, SCHEMA_2);
        RowMerger full = root.configureTargetColumns(null, (short) 2, SCHEMA_2);
        BinaryValue old = value(format, SCHEMA, 1, "Alice");
        assertThat(full.merge(old, value(format, SCHEMA_2, 1, null, "20")))
                .isEqualTo(value(format, SCHEMA_2, 1, "Alice", "20"));
        assertThat(
                        full.merge(
                                value(format, SCHEMA_2, 1, "Alice", "20"),
                                value(format, SCHEMA, 1, "Bob")))
                .isEqualTo(value(format, SCHEMA_2, 1, "Bob", "20"));
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testIgnoreNullsReplacesWholeComplexColumns(KvFormat format) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("profile", DataTypes.ROW(DataTypes.STRING(), DataTypes.STRING()))
                        .column("attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .column("items", DataTypes.ARRAY(DataTypes.STRING()))
                        .column("active", DataTypes.BOOLEAN())
                        .column("count", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        RowMerger merger =
                ignoreNullsMerger(format, schema).configureTargetColumns(null, (short) 2, schema);
        BinaryValue old =
                value(
                        format,
                        schema,
                        1,
                        new Object[] {"Alice", "Hangzhou"},
                        new Object[] {"a", "old"},
                        new Object[] {"old"},
                        true,
                        1,
                        "name");
        BinaryValue replacement =
                value(
                        format,
                        schema,
                        1,
                        new Object[] {null, "Shanghai"},
                        new Object[] {},
                        new Object[] {},
                        false,
                        0,
                        "");
        assertThat(merger.merge(old, replacement)).isEqualTo(replacement);
        assertThat(
                        merger.merge(
                                replacement,
                                value(format, schema, 1, null, null, null, null, null, null)))
                .isEqualTo(replacement);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testIgnoreNullsFullWritesAllowNotNullColumns(KvFormat format) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("required", DataTypes.STRING().copy(false))
                        .column("optional", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        RowMerger merger =
                ignoreNullsMerger(format, schema).configureTargetColumns(null, (short) 2, schema);
        assertThat(
                        merger.merge(
                                value(format, schema, 1, "before", "retained"),
                                value(format, schema, 1, "after", null)))
                .isEqualTo(value(format, schema, 1, "after", "retained"));
    }

    private RowMerger ignoreNullsMerger(KvFormat format, Schema schema) {
        return new DefaultRowMerger(format, null, true);
    }

    private BinaryValue value(KvFormat format, Schema schema, Object... fields) {
        GenericRow internal = row(schema.getRowType(), fields);
        for (int i = 0; i < fields.length; i++) {
            fields[i] = internal.getField(i);
        }
        return new BinaryValue(
                (short) (schema == SCHEMA ? 1 : 2),
                format == KvFormat.COMPACTED
                        ? compactedRow(schema.getRowType(), fields)
                        : indexedRow(schema.getRowType(), fields));
    }

    private BinaryValue createBinaryValue(int id, String name) {
        return new BinaryValue(
                (short) 1, compactedRow(SCHEMA.getRowType(), new Object[] {id, name}));
    }

    private BinaryValue createBinaryValue(int id, String name, String age) {
        return new BinaryValue(
                (short) 2, compactedRow(SCHEMA_2.getRowType(), new Object[] {id, name, age}));
    }

    @ParameterizedTest
    @EnumSource(DeleteBehavior.class)
    void testDefaultRowMerger(DeleteBehavior deleteBehavior) {
        DefaultRowMerger merger = new DefaultRowMerger(KvFormat.COMPACTED, deleteBehavior);
        merger.configureTargetColumns(null, (byte) 1, SCHEMA);

        BinaryValue oldValue = createBinaryValue(1, "old");
        BinaryValue newValue = createBinaryValue(1, "new");

        // Test merge operation - should return new row
        BinaryValue mergedValue = merger.merge(oldValue, newValue);
        assertThat(mergedValue).isSameAs(newValue);

        // Test delete operation - should return null (deleted)
        BinaryValue deletedValue = merger.delete(oldValue);
        assertThat(deletedValue).isNull();

        // Test supportsDelete - should return true
        assertThat(merger.deleteBehavior()).isEqualTo(deleteBehavior);

        // Test schema change.
        merger.configureTargetColumns(null, (byte) 2, SCHEMA_2);
        newValue = createBinaryValue(1, "new2", "20");
        assertThat(merger.merge(oldValue, newValue)).isSameAs(newValue);
        assertThat(merger.delete(newValue)).isNull();
    }

    @ParameterizedTest
    @EnumSource(DeleteBehavior.class)
    void testPartialUpdateRowMergerDeleteBehavior(DeleteBehavior deleteBehavior) {
        DefaultRowMerger merger = new DefaultRowMerger(KvFormat.COMPACTED, deleteBehavior);

        // Explicit full schema ({id, name}) matches plain merger behavior (same as null targets).
        RowMerger partialMerger =
                merger.configureTargetColumns(new int[] {0, 1}, (byte) 1, SCHEMA); // id + name
        assertThat(partialMerger).isSameAs(merger);

        BinaryValue oldValue = createBinaryValue(1, "old");

        BinaryValue ignoredValue = partialMerger.delete(oldValue);
        assertThat(ignoredValue).isNull();
        assertThat(partialMerger.deleteBehavior()).isEqualTo(deleteBehavior);

        assertThat(partialMerger.merge(null, oldValue)).isEqualTo(oldValue);

        // schema change then partial update (only id + age; omit name).
        partialMerger = merger.configureTargetColumns(new int[] {0, 2}, (byte) 2, SCHEMA_2);
        BinaryValue newValue = createBinaryValue(1, null, "20");
        BinaryValue mergeValue = createBinaryValue(1, "old", "20");
        assertThat(partialMerger.merge(oldValue, newValue)).isEqualTo(mergeValue);
        assertThat(partialMerger.delete(mergeValue)).isEqualTo(createBinaryValue(1, "old", null));
    }
}
