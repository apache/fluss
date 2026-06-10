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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CoordinatorContext}. */
class CoordinatorContextTest {

    @Test
    void testGetLakeTableCount() {
        CoordinatorContext context = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);

        // Initially, there should be no tables
        assertThat(context.allTables()).isEmpty();
        assertThat(context.getLakeTableCount()).isEqualTo(0);

        // Add a non-lake table
        TableInfo nonLakeTable = createTableInfo(1L, TablePath.of("db1", "table1"), false);
        context.putTablePath(1L, nonLakeTable.getTablePath());
        context.putTableInfo(nonLakeTable);

        assertThat(context.allTables()).hasSize(1);
        assertThat(context.getLakeTableCount()).isEqualTo(0);

        // Add a lake table
        TableInfo lakeTable = createTableInfo(2L, TablePath.of("db1", "table2"), true);
        context.putTablePath(2L, lakeTable.getTablePath());
        context.putTableInfo(lakeTable);

        assertThat(context.allTables()).hasSize(2);
        assertThat(context.getLakeTableCount()).isEqualTo(1);

        // Add another lake table
        TableInfo lakeTable2 = createTableInfo(3L, TablePath.of("db2", "table3"), true);
        context.putTablePath(3L, lakeTable2.getTablePath());
        context.putTableInfo(lakeTable2);

        assertThat(context.allTables()).hasSize(3);
        assertThat(context.getLakeTableCount()).isEqualTo(2);
    }

    @Test
    void testIneligibleForDeletionMarkAndRemove() {
        CoordinatorContext context = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);

        long queuedTableId = 100L;
        long unqueuedTableId = 200L;
        TablePartition queuedPartition = new TablePartition(300L, 1L);
        TablePartition unqueuedPartition = new TablePartition(400L, 2L);

        // only tables/partitions that are queued for deletion can be marked ineligible
        context.queueTableDeletion(Collections.singleton(queuedTableId));
        context.queuePartitionDeletion(Collections.singleton(queuedPartition));

        // initially nothing is ineligible
        assertThat(context.isTableIneligibleForDeletion(queuedTableId)).isFalse();
        assertThat(context.isPartitionIneligibleForDeletion(queuedPartition)).isFalse();

        // (a) mark-when-queued sets the ineligible flag
        context.markTableIneligibleForDeletion(queuedTableId, "first reason");
        context.markPartitionIneligibleForDeletion(queuedPartition, "first reason");
        assertThat(context.isTableIneligibleForDeletion(queuedTableId)).isTrue();
        assertThat(context.isPartitionIneligibleForDeletion(queuedPartition)).isTrue();

        // (b) re-mark same id with new reason keeps the flag set (reason is overwritten)
        context.markTableIneligibleForDeletion(queuedTableId, "updated reason");
        context.markPartitionIneligibleForDeletion(queuedPartition, "updated reason");
        assertThat(context.isTableIneligibleForDeletion(queuedTableId)).isTrue();
        assertThat(context.isPartitionIneligibleForDeletion(queuedPartition)).isTrue();

        // (c) mark-when-NOT-queued is a no-op
        context.markTableIneligibleForDeletion(unqueuedTableId, "should be ignored");
        context.markPartitionIneligibleForDeletion(unqueuedPartition, "should be ignored");
        assertThat(context.isTableIneligibleForDeletion(unqueuedTableId)).isFalse();
        assertThat(context.isPartitionIneligibleForDeletion(unqueuedPartition)).isFalse();

        // (d) removeFromIneligibleForDeletion removes the ineligible flag
        context.removeTableFromIneligibleForDeletion(queuedTableId);
        context.removePartitionFromIneligibleForDeletion(queuedPartition);
        assertThat(context.isTableIneligibleForDeletion(queuedTableId)).isFalse();
        assertThat(context.isPartitionIneligibleForDeletion(queuedPartition)).isFalse();

        // re-marking after eligible should set it back to ineligible
        context.markTableIneligibleForDeletion(queuedTableId, "second-time reason");
        context.markPartitionIneligibleForDeletion(queuedPartition, "second-time reason");
        assertThat(context.isTableIneligibleForDeletion(queuedTableId)).isTrue();
        assertThat(context.isPartitionIneligibleForDeletion(queuedPartition)).isTrue();
    }

    @Test
    void testResetContextClearsAllDeletionState() {
        CoordinatorContext context = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);

        long tableId = 100L;
        TablePartition partition = new TablePartition(200L, 1L);

        // pre-seed all four collections covered by the fix
        context.queueTableDeletion(Collections.singleton(tableId));
        context.queuePartitionDeletion(Collections.singleton(partition));
        context.markTableIneligibleForDeletion(tableId, "blocked");
        context.markPartitionIneligibleForDeletion(partition, "blocked");

        // sanity-check pre-conditions before reset
        assertThat(context.getTablesToBeDeleted()).contains(tableId);
        assertThat(context.getPartitionsToBeDeleted()).contains(partition);
        assertThat(context.isTableIneligibleForDeletion(tableId)).isTrue();
        assertThat(context.isPartitionIneligibleForDeletion(partition)).isTrue();

        context.resetContext();

        assertThat(context.getTablesToBeDeleted()).isEmpty();
        assertThat(context.getPartitionsToBeDeleted()).isEmpty();
        assertThat(context.isTableIneligibleForDeletion(tableId)).isFalse();
        assertThat(context.isPartitionIneligibleForDeletion(partition)).isFalse();
    }

    private TableInfo createTableInfo(long tableId, TablePath tablePath, boolean isLake) {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ZERO)
                        .property(TABLE_DATALAKE_ENABLED, isLake)
                        .distributedBy(1)
                        .build();

        return TableInfo.of(
                tablePath,
                tableId,
                1,
                tableDescriptor,
                DEFAULT_REMOTE_DATA_DIR,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }
}
