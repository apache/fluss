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
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.indexedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link UpdateIfChangedRowMerger}. */
class UpdateIfChangedRowMergerTest {

    private static final short SCHEMA_ID = 1;
    private static final short SCHEMA_2_ID = 2;
    private static final short SCHEMA_AFTER_DROP_ID = 3;

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("data", DataTypes.BYTES())
                    .primaryKey("id")
                    .build();

    private static final Schema SCHEMA_2 =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("id", DataTypes.INT(), null, (short) 0),
                                    new Schema.Column("name", DataTypes.STRING(), null, (short) 1),
                                    new Schema.Column("data", DataTypes.BYTES(), null, (short) 2),
                                    // add new column at end
                                    new Schema.Column("age", DataTypes.INT(), null, (short) 3)))
                    .primaryKey("id")
                    .build();

    private static final Schema SCHEMA_AFTER_DROP =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("id", DataTypes.INT(), null, (short) 0),
                                    new Schema.Column("data", DataTypes.BYTES(), null, (short) 2)))
                    .primaryKey("id")
                    .build();

    private BinaryValue value(int id, String name, byte[] data) {
        return value(KvFormat.COMPACTED, id, name, data);
    }

    private BinaryValue value(KvFormat kvFormat, int id, String name, byte[] data) {
        return binaryValue(SCHEMA_ID, SCHEMA, kvFormat, new Object[] {id, name, data});
    }

    private BinaryValue value2(int id, String name, byte[] data, Integer age) {
        return binaryValue(
                SCHEMA_2_ID, SCHEMA_2, KvFormat.COMPACTED, new Object[] {id, name, data, age});
    }

    private BinaryValue valueAfterDrop(int id, byte[] data) {
        return binaryValue(
                SCHEMA_AFTER_DROP_ID,
                SCHEMA_AFTER_DROP,
                KvFormat.COMPACTED,
                new Object[] {id, data});
    }

    private static BinaryValue binaryValue(
            short schemaId, Schema schema, KvFormat kvFormat, Object[] fields) {
        BinaryRow row =
                kvFormat == KvFormat.COMPACTED
                        ? compactedRow(schema.getRowType(), fields)
                        : indexedRow(schema.getRowType(), fields);
        return new BinaryValue(schemaId, row);
    }

    private static UpdateIfChangedRowMerger createMerger(
            KvFormat kvFormat, SchemaInfo... schemaInfos) {
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(schemaInfos[0]);
        for (int i = 1; i < schemaInfos.length; i++) {
            schemaGetter.updateLatestSchemaInfo(schemaInfos[i]);
        }
        return new UpdateIfChangedRowMerger(kvFormat, schemaGetter, DeleteBehavior.ALLOW);
    }

    private static UpdateIfChangedRowMerger createMerger() {
        return createMerger(KvFormat.COMPACTED, new SchemaInfo(SCHEMA, SCHEMA_ID));
    }

    @Test
    void testInsertWhenNoOldValue() {
        UpdateIfChangedRowMerger merger = createMerger();
        merger.configureTargetColumns(null, SCHEMA_ID, SCHEMA);

        BinaryValue newValue = value(1, "a", new byte[] {1, 2});
        assertThat(merger.merge(null, newValue)).isSameAs(newValue);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testNoOpWhenLogicallyEqual(KvFormat kvFormat) {
        UpdateIfChangedRowMerger merger = createMerger(kvFormat, new SchemaInfo(SCHEMA, SCHEMA_ID));
        merger.configureTargetColumns(null, SCHEMA_ID, SCHEMA);

        BinaryValue oldValue = value(kvFormat, 1, "a", new byte[] {1, 2});
        // different instances, same logical content (including binary content)
        BinaryValue newValue = value(kvFormat, 1, "a", new byte[] {1, 2});

        // returns the old value instance so the write path treats it as a no-op
        assertThat(merger.merge(oldValue, newValue)).isSameAs(oldValue);
    }

    @Test
    void testUpdateWhenFieldDiffers() {
        UpdateIfChangedRowMerger merger = createMerger();
        merger.configureTargetColumns(null, SCHEMA_ID, SCHEMA);

        BinaryValue oldValue = value(1, "a", new byte[] {1, 2});
        BinaryValue newValue = value(1, "b", new byte[] {1, 2});

        assertThat(merger.merge(oldValue, newValue)).isSameAs(newValue);
    }

    @Test
    void testUpdateWhenBinaryContentDiffers() {
        UpdateIfChangedRowMerger merger = createMerger();
        merger.configureTargetColumns(null, SCHEMA_ID, SCHEMA);

        BinaryValue oldValue = value(1, "a", new byte[] {1, 2});
        BinaryValue newValue = value(1, "a", new byte[] {1, 3});

        assertThat(merger.merge(oldValue, newValue)).isSameAs(newValue);
    }

    @Test
    void testNullFieldEquality() {
        UpdateIfChangedRowMerger merger = createMerger();
        merger.configureTargetColumns(null, SCHEMA_ID, SCHEMA);

        // both name null -> equal
        BinaryValue oldValue = value(1, null, new byte[] {1});
        BinaryValue newValue = value(1, null, new byte[] {1});
        assertThat(merger.merge(oldValue, newValue)).isSameAs(oldValue);

        // one null, one non-null -> differs
        BinaryValue newValue2 = value(1, "a", new byte[] {1});
        assertThat(merger.merge(oldValue, newValue2)).isSameAs(newValue2);
    }

    @Test
    void testSchemaEvolutionAlignment() {
        UpdateIfChangedRowMerger merger =
                createMerger(
                        KvFormat.COMPACTED,
                        new SchemaInfo(SCHEMA, SCHEMA_ID),
                        new SchemaInfo(SCHEMA_2, SCHEMA_2_ID));
        // latest schema has the extra nullable "age" column
        merger.configureTargetColumns(null, SCHEMA_2_ID, SCHEMA_2);

        // old row uses the older schema (no age field), incoming row uses latest schema with age
        // null; missing trailing fields are aligned to null -> logically equal
        BinaryValue oldValue = value(1, "a", new byte[] {1, 2});
        BinaryValue newValue = value2(1, "a", new byte[] {1, 2}, null);
        assertThat(merger.merge(oldValue, newValue)).isSameAs(oldValue);

        // incoming row sets age -> differs
        BinaryValue newValue2 = value2(1, "a", new byte[] {1, 2}, 20);
        assertThat(merger.merge(oldValue, newValue2)).isSameAs(newValue2);

        // A newly arrived request may still use the old schema. It is also aligned by column ID.
        BinaryValue latestValue = value2(1, "a", new byte[] {1, 2}, null);
        BinaryValue oldSchemaRequest = value(1, "a", new byte[] {1, 2});
        assertThat(merger.merge(latestValue, oldSchemaRequest)).isSameAs(latestValue);
    }

    @Test
    void testSchemaEvolutionAlignmentAfterDroppingMiddleColumn() {
        UpdateIfChangedRowMerger merger =
                createMerger(
                        KvFormat.COMPACTED,
                        new SchemaInfo(SCHEMA, SCHEMA_ID),
                        new SchemaInfo(SCHEMA_AFTER_DROP, SCHEMA_AFTER_DROP_ID));
        merger.configureTargetColumns(null, SCHEMA_AFTER_DROP_ID, SCHEMA_AFTER_DROP);

        BinaryValue oldValue = value(1, "removed", new byte[] {1, 2});
        BinaryValue sameValue = valueAfterDrop(1, new byte[] {1, 2});
        assertThat(merger.merge(oldValue, sameValue)).isSameAs(oldValue);

        BinaryValue changedValue = valueAfterDrop(1, new byte[] {1, 3});
        assertThat(merger.merge(oldValue, changedValue)).isSameAs(changedValue);
    }

    @Test
    void testDeleteReturnsNull() {
        UpdateIfChangedRowMerger merger = createMerger();
        merger.configureTargetColumns(null, SCHEMA_ID, SCHEMA);

        BinaryValue oldValue = value(1, "a", new byte[] {1});
        assertThat(merger.delete(oldValue)).isNull();
        assertThat(merger.deleteBehavior()).isEqualTo(DeleteBehavior.ALLOW);
    }

    @Test
    void testDefaultDeleteBehaviorIsAllow() {
        UpdateIfChangedRowMerger merger =
                new UpdateIfChangedRowMerger(
                        KvFormat.COMPACTED,
                        new TestingSchemaGetter(new SchemaInfo(SCHEMA, SCHEMA_ID)),
                        null);
        assertThat(merger.deleteBehavior()).isEqualTo(DeleteBehavior.ALLOW);
    }

    @Test
    void testPartialUpdateNoOpWhenUnchanged() {
        UpdateIfChangedRowMerger merger = createMerger();
        // partial update on id + name (omit data)
        RowMerger partial = merger.configureTargetColumns(new int[] {0, 1}, SCHEMA_ID, SCHEMA);

        BinaryValue oldValue = value(1, "a", new byte[] {9});
        // partial row only carries id + name equal to stored, data absent -> candidate keeps old
        // data, so candidate equals old -> no-op
        BinaryValue partialRow = value(1, "a", null);
        assertThat(partial.merge(oldValue, partialRow)).isSameAs(oldValue);
    }

    @Test
    void testPartialUpdateAppliesWhenChanged() {
        UpdateIfChangedRowMerger merger = createMerger();
        RowMerger partial = merger.configureTargetColumns(new int[] {0, 1}, SCHEMA_ID, SCHEMA);

        BinaryValue oldValue = value(1, "a", new byte[] {9});
        BinaryValue partialRow = value(1, "b", null);
        BinaryValue expected = value(1, "b", new byte[] {9});
        assertThat(partial.merge(oldValue, partialRow)).isEqualTo(expected);
    }

    @Test
    void testPartialDeleteNoOpWhenUnchanged() {
        UpdateIfChangedRowMerger merger = createMerger();
        RowMerger partial = merger.configureTargetColumns(new int[] {0, 1}, SCHEMA_ID, SCHEMA);

        BinaryValue oldValue = value(1, null, new byte[] {9});
        assertThat(partial.delete(oldValue)).isSameAs(oldValue);
    }

    @Test
    void testPartialDeleteAppliesWhenChanged() {
        UpdateIfChangedRowMerger merger = createMerger();
        RowMerger partial = merger.configureTargetColumns(new int[] {0, 1}, SCHEMA_ID, SCHEMA);

        BinaryValue oldValue = value(1, "a", new byte[] {9});
        BinaryValue expected = value(1, null, new byte[] {9});
        assertThat(partial.delete(oldValue)).isEqualTo(expected);
    }
}
