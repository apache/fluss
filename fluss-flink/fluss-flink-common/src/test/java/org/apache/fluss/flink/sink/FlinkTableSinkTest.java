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

package org.apache.fluss.flink.sink;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.flink.sink.undo.RecoveryAction;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.fluss.metadata.AggFunctions.MAX;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests recovery-action plumbing in {@link FlinkTableSink}. */
class FlinkTableSinkTest {

    private static final Schema AGG_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .column("max_value", DataTypes.BIGINT(), MAX())
                    .primaryKey("id")
                    .build();
    private static final RowType FLINK_ROW_TYPE =
            (RowType)
                    org.apache.flink.table.api.DataTypes.ROW(
                                    org.apache.flink.table.api.DataTypes.FIELD(
                                            "id", org.apache.flink.table.api.DataTypes.BIGINT()),
                                    org.apache.flink.table.api.DataTypes.FIELD(
                                            "max_value",
                                            org.apache.flink.table.api.DataTypes.BIGINT()))
                            .getLogicalType();

    @Test
    void testInsertOnlyRecoveryProofSurvivesCopy() {
        FlinkTableSink sink = createSink(AGG_SCHEMA, false, DeleteBehavior.ALLOW);
        assertThat(sink.getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.insertOnly());

        assertThat(sink.getRecoveryAction(new int[] {0, 1})).isEqualTo(RecoveryAction.NO_OP);

        FlinkTableSink copied = (FlinkTableSink) sink.copy();
        assertThat(copied.getRecoveryAction(new int[] {0, 1})).isEqualTo(RecoveryAction.NO_OP);
    }

    @Test
    void testEveryObservedChangelogModeMustProveUpsertOnlyInput() {
        FlinkTableSink insertThenAll = createSink(AGG_SCHEMA, false, DeleteBehavior.ALLOW);
        insertThenAll.getChangelogMode(ChangelogMode.insertOnly());
        insertThenAll.getChangelogMode(ChangelogMode.all());

        assertThat(insertThenAll.getRecoveryAction(new int[] {0, 1}))
                .isEqualTo(RecoveryAction.UNDO);
        FlinkTableSink copied = (FlinkTableSink) insertThenAll.copy();
        copied.getChangelogMode(ChangelogMode.insertOnly());
        assertThat(copied.getRecoveryAction(new int[] {0, 1})).isEqualTo(RecoveryAction.UNDO);

        FlinkTableSink allThenInsert = createSink(AGG_SCHEMA, false, DeleteBehavior.ALLOW);
        allThenInsert.getChangelogMode(ChangelogMode.all());
        allThenInsert.getChangelogMode(ChangelogMode.insertOnly());

        assertThat(allThenInsert.getRecoveryAction(new int[] {0, 1}))
                .isEqualTo(RecoveryAction.UNDO);
    }

    @Test
    void testAcceptedAndIgnoredDeletesSelectRecoveryAction() {
        FlinkTableSink deleting = createSink(AGG_SCHEMA, false, DeleteBehavior.ALLOW);
        ChangelogMode acceptedMode = deleting.getChangelogMode(ChangelogMode.all());
        assertThat(acceptedMode.getContainedKinds())
                .containsExactlyInAnyOrder(RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE);
        assertThat(deleting.getRecoveryAction(new int[] {0, 1})).isEqualTo(RecoveryAction.UNDO);
        assertThat(((FlinkTableSink) deleting.copy()).getRecoveryAction(new int[] {0, 1}))
                .isEqualTo(RecoveryAction.UNDO);

        FlinkTableSink ignoring = createSink(AGG_SCHEMA, true, DeleteBehavior.ALLOW);
        ignoring.getChangelogMode(ChangelogMode.all());
        assertThat(ignoring.getRecoveryAction(new int[] {0, 1})).isEqualTo(RecoveryAction.NO_OP);
    }

    @Test
    void testLegacyConstructorConservativelyUsesUndoAfterCopy() {
        FlinkTableSink sink = createLegacySink();
        sink.getChangelogMode(ChangelogMode.insertOnly());

        assertThat(sink.getRecoveryAction(new int[] {0, 1})).isEqualTo(RecoveryAction.UNDO);
        assertThat(((FlinkTableSink) sink.copy()).getRecoveryAction(new int[] {0, 1}))
                .isEqualTo(RecoveryAction.UNDO);
    }

    private static FlinkTableSink createSink(
            Schema schema, boolean ignoreDelete, DeleteBehavior deleteBehavior) {
        return new FlinkTableSink(
                TablePath.of("db", "mixed"),
                new Configuration(),
                FLINK_ROW_TYPE,
                schema,
                new int[] {0},
                Collections.emptyList(),
                true,
                MergeEngineType.AGGREGATION,
                null,
                ignoreDelete,
                deleteBehavior,
                1,
                Collections.singletonList("id"),
                DistributionMode.NONE,
                null);
    }

    private static FlinkTableSink createLegacySink() {
        return new FlinkTableSink(
                TablePath.of("db", "legacy"),
                new Configuration(),
                FLINK_ROW_TYPE,
                new int[] {0},
                Collections.emptyList(),
                true,
                MergeEngineType.AGGREGATION,
                null,
                false,
                DeleteBehavior.ALLOW,
                1,
                Collections.singletonList("id"),
                DistributionMode.NONE,
                null);
    }
}
