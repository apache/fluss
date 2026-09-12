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
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultRowMerger} delete behavior and sequence group arbitration. */
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

    /** {@code name} is ordered by {@code ts}, while {@code note} takes part in no group. */
    private static final Schema SEQUENCE_GROUP_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("ts", DataTypes.INT())
                    .column("note", DataTypes.STRING())
                    .sequenceGroup(
                            java.util.Collections.singletonList("ts"),
                            java.util.Collections.singletonList("name"))
                    .primaryKey("id")
                    .build();

    private static BinaryValue sequenceGroupValue(String name, Integer ts, String note) {
        return new BinaryValue(
                (short) 1,
                compactedRow(SEQUENCE_GROUP_SCHEMA.getRowType(), new Object[] {1, name, ts, note}));
    }

    @Test
    void testSequenceGroupRowMergerOnFullRow() {
        RowMerger merger =
                new DefaultRowMerger(KvFormat.COMPACTED, DeleteBehavior.ALLOW)
                        .configureTargetColumns(null, (short) 1, SEQUENCE_GROUP_SCHEMA);

        // a first row without a sequence skips the group, while an ungrouped field is accepted
        assertThat(merger.merge(null, sequenceGroupValue("skipped", null, "n0")))
                .isEqualTo(sequenceGroupValue(null, null, "n0"));

        // a first row carrying a sequence initializes the group without being re-encoded
        BinaryValue first = sequenceGroupValue("first", 100, "n1");
        assertThat(merger.merge(null, first)).isSameAs(first);

        // every group advances, so the whole incoming row wins without being re-encoded
        BinaryValue newer = sequenceGroupValue("newer", 101, "n2");
        assertThat(merger.merge(first, newer)).isSameAs(newer);

        // the group falls behind, so its columns keep the stored values while the column outside
        // any group still takes the incoming one
        BinaryValue stale = sequenceGroupValue("stale", 99, "n3");
        assertThat(merger.merge(newer, stale)).isEqualTo(sequenceGroupValue("newer", 101, "n3"));

        // a delete carries no sequence values, so it keeps removing the whole row
        assertThat(merger.delete(newer)).isNull();
    }

    @Test
    void testSequenceGroupRowMergerReadsAShorterStoredRow() {
        RowMerger merger =
                new DefaultRowMerger(KvFormat.COMPACTED, DeleteBehavior.ALLOW)
                        .configureTargetColumns(null, (short) 1, SEQUENCE_GROUP_SCHEMA);

        // the stored row was written before 'ts' and 'note' were added, so it carries fewer fields
        // and its absent sequence orders before everything
        BinaryValue shortRow =
                new BinaryValue(
                        (short) 1, compactedRow(SCHEMA.getRowType(), new Object[] {1, "stored"}));
        BinaryValue incoming = sequenceGroupValue("incoming", 1, "n1");
        assertThat(merger.merge(shortRow, incoming)).isSameAs(incoming);

        // without any sequence the incoming group is dropped, and the missing fields of the stored
        // row are read as null
        BinaryValue withoutSequence = sequenceGroupValue("dropped", null, "n2");
        assertThat(merger.merge(shortRow, withoutSequence))
                .isEqualTo(sequenceGroupValue("stored", null, "n2"));
    }

    @Test
    void testSequenceGroupRowMergerOnPartialColumns() {
        DefaultRowMerger merger = new DefaultRowMerger(KvFormat.COMPACTED, DeleteBehavior.ALLOW);
        // only 'name' and its sequence column are written, leaving 'note' out
        RowMerger partialMerger =
                merger.configureTargetColumns(
                        new int[] {0, 1, 2}, (short) 1, SEQUENCE_GROUP_SCHEMA);

        // the partial path applies the same first-write arbitration as the full-row path
        assertThat(partialMerger.merge(null, sequenceGroupValue("skipped", null, null)))
                .isEqualTo(sequenceGroupValue(null, null, null));

        BinaryValue stored = sequenceGroupValue("stored", 100, "kept");
        // the group advances, so the written columns take the incoming values and 'note' is kept
        assertThat(partialMerger.merge(stored, sequenceGroupValue("newer", 101, null)))
                .isEqualTo(sequenceGroupValue("newer", 101, "kept"));
        // the group falls behind, so the write is a no-op and the stored value is returned as is
        assertThat(partialMerger.merge(stored, sequenceGroupValue("stale", 99, null)))
                .isSameAs(stored);
    }

    @Test
    void testBlindOverwriteRestoresAnAlreadyDecidedValue() {
        // recovering by undo writes back the value stored at the checkpoint, which is older than
        // what is in the store, so arbitrating it would discard the very row being recovered
        BinaryValue checkpointed = sequenceGroupValue("checkpointed", 100, "n1");
        BinaryValue newer = sequenceGroupValue("newer", 200, "n2");

        DefaultRowMerger blind = DefaultRowMerger.forBlindOverwrite(KvFormat.COMPACTED);
        assertThat(blind.configureTargetColumns(null, (short) 1, SEQUENCE_GROUP_SCHEMA))
                // staying a DefaultRowMerger also keeps the fast path KvTablet takes for a write
                // that may skip reading the stored row
                .isSameAs(blind);
        assertThat(blind.merge(newer, checkpointed)).isSameAs(checkpointed);

        RowMerger blindPartial =
                DefaultRowMerger.forBlindOverwrite(KvFormat.COMPACTED)
                        .configureTargetColumns(
                                new int[] {0, 1, 2}, (short) 1, SEQUENCE_GROUP_SCHEMA);
        assertThat(blindPartial.merge(newer, checkpointed))
                .isEqualTo(sequenceGroupValue("checkpointed", 100, "n2"));
    }

    @Test
    void testArbitratingMergerReplacesThePlainOne() {
        DefaultRowMerger merger = new DefaultRowMerger(KvFormat.COMPACTED, DeleteBehavior.ALLOW);

        // KvTablet skips reading the stored row while the merger is a DefaultRowMerger, which would
        // leave every group unarbitrated, so a schema with sequence groups must replace it
        assertThat(merger.configureTargetColumns(null, (short) 1, SEQUENCE_GROUP_SCHEMA))
                .isNotInstanceOf(DefaultRowMerger.class);
        // a schema without sequence groups keeps the plain merger and so keeps the fast path
        assertThat(merger.configureTargetColumns(null, (short) 2, SCHEMA)).isSameAs(merger);
        // the merger is rebuilt on a schema change and reused within one schema
        RowMerger arbitrating =
                merger.configureTargetColumns(null, (short) 3, SEQUENCE_GROUP_SCHEMA);
        assertThat(merger.configureTargetColumns(null, (short) 3, SEQUENCE_GROUP_SCHEMA))
                .isSameAs(arbitrating);

        assertThatThrownBy(
                        () ->
                                arbitrating.configureTargetColumns(
                                        null, (short) 3, SEQUENCE_GROUP_SCHEMA))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not support reconfigure");
    }
}
