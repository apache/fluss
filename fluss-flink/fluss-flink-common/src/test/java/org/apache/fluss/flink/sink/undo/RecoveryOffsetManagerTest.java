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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.admin.ProducerOffsetsResult;
import org.apache.fluss.client.admin.RegisterResult;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.sink.testutils.TestAdminAdapter;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RecoveryOffsetManager}'s public API.
 *
 * <p>These tests use a test Admin implementation that simulates real Admin behavior to verify
 * determineRecoveryStrategy() without requiring a real Fluss cluster.
 */
public class RecoveryOffsetManagerTest {

    private static final long TABLE_ID = 1L;
    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");
    private static final String PRODUCER_ID = "test-producer";

    // ==================== Test Data Helpers ====================

    private static TableInfo createTableInfo(int numBuckets, boolean isPartitioned) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        List<String> partitionKeys =
                isPartitioned ? Collections.singletonList("pt") : Collections.emptyList();

        return new TableInfo(
                TABLE_PATH,
                TABLE_ID,
                0, // schemaId
                schema,
                Collections.emptyList(), // bucketKeys
                partitionKeys,
                numBuckets,
                new Configuration(),
                new Configuration(),
                null, // comment
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    private static PartitionInfo createPartitionInfo(long partitionId, String partitionName) {
        ResolvedPartitionSpec spec = ResolvedPartitionSpec.fromPartitionValue("pt", partitionName);
        return new PartitionInfo(partitionId, spec);
    }

    // ==================== FRESH_START Tests ====================

    @Test
    void testFreshStartWithEmptyCheckpoint() throws Exception {
        // Setup: current offsets are all 0 (no data written)
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 0), 0L);
        currentOffsets.put(new TableBucket(TABLE_ID, 1), 0L);

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        TableInfo tableInfo = createTableInfo(2, false);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        // Execute: empty checkpoint (checkpoint exists but no data written)
        RecoveryOffsetManager.RecoveryDecision decision =
                manager.determineRecoveryStrategy(new ArrayList<>());

        // Verify
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.FRESH_START);
        assertThat(decision.needsUndoRecovery()).isFalse();
        assertThat(decision.getRecoveryOffsets()).isNull();
    }

    @Test
    void testFreshStartWhenOffsetsMatch() throws Exception {
        // Setup: checkpoint offsets match current offsets
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        currentOffsets.put(new TableBucket(TABLE_ID, 1), 200L);

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        TableInfo tableInfo = createTableInfo(2, false);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        // Checkpoint offsets match current
        Map<TableBucket, Long> checkpointOffsets = new HashMap<>(currentOffsets);
        WriterState state = new WriterState(checkpointOffsets);

        // Execute
        RecoveryOffsetManager.RecoveryDecision decision =
                manager.determineRecoveryStrategy(Collections.singletonList(state));

        // Verify
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.FRESH_START);
        assertThat(decision.needsUndoRecovery()).isFalse();
        assertThat(decision.getRecoveryOffsets()).isNull();
    }

    // ==================== CHECKPOINT_RECOVERY Tests ====================

    @Test
    void testCheckpointRecoveryWithChangedOffsets() throws Exception {
        // Setup: current offsets are ahead of checkpoint
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 0), 150L);
        currentOffsets.put(new TableBucket(TABLE_ID, 1), 250L);

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        TableInfo tableInfo = createTableInfo(2, false);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        // Checkpoint offsets are behind current
        Map<TableBucket, Long> checkpointOffsets = new HashMap<>();
        checkpointOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        checkpointOffsets.put(new TableBucket(TABLE_ID, 1), 200L);
        WriterState state = new WriterState(checkpointOffsets);

        // Execute
        RecoveryOffsetManager.RecoveryDecision decision =
                manager.determineRecoveryStrategy(Collections.singletonList(state));

        // Verify
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.CHECKPOINT_RECOVERY);
        assertThat(decision.needsUndoRecovery()).isTrue();
        assertThat(decision.getRecoveryOffsets()).hasSize(2);
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 0))).isEqualTo(100L);
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 1))).isEqualTo(200L);
    }

    @Test
    void testCheckpointRecoveryWithRescaling() throws Exception {
        // Simulates rescaling from parallelism=4 to parallelism=4 with state redistribution.
        // Previously each subtask had one unique bucket. After rescaling, Flink may redistribute
        // all 4 states to subtask 0. The manager should filter by bucket assignment and only
        // return the bucket assigned to this subtask.

        // Setup: 4 buckets, all with data ahead of checkpoint
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 0), 200L);
        currentOffsets.put(new TableBucket(TABLE_ID, 1), 300L);
        currentOffsets.put(new TableBucket(TABLE_ID, 2), 400L);
        currentOffsets.put(new TableBucket(TABLE_ID, 3), 500L);

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        TableInfo tableInfo = createTableInfo(4, false);
        // subtaskIndex=0, parallelism=4: only bucket 0 assigned (0 % 4 = 0)
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 4, 10L, 5000L, TABLE_PATH, tableInfo);

        // 4 distinct states from previous subtasks, each with a unique bucket
        Map<TableBucket, Long> offsets0 = new HashMap<>();
        offsets0.put(new TableBucket(TABLE_ID, 0), 100L);
        WriterState state0 = new WriterState(offsets0);

        Map<TableBucket, Long> offsets1 = new HashMap<>();
        offsets1.put(new TableBucket(TABLE_ID, 1), 150L);
        WriterState state1 = new WriterState(offsets1);

        Map<TableBucket, Long> offsets2 = new HashMap<>();
        offsets2.put(new TableBucket(TABLE_ID, 2), 200L);
        WriterState state2 = new WriterState(offsets2);

        Map<TableBucket, Long> offsets3 = new HashMap<>();
        offsets3.put(new TableBucket(TABLE_ID, 3), 250L);
        WriterState state3 = new WriterState(offsets3);

        // Execute: subtask 0 receives all 4 states after rescaling
        RecoveryOffsetManager.RecoveryDecision decision =
                manager.determineRecoveryStrategy(Arrays.asList(state0, state1, state2, state3));

        // Verify: only bucket 0 is assigned to subtask 0 (0 % 4 = 0)
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.CHECKPOINT_RECOVERY);
        assertThat(decision.needsUndoRecovery()).isTrue();
        assertThat(decision.getRecoveryOffsets()).hasSize(1);
        assertThat(decision.getRecoveryOffsets()).containsKey(new TableBucket(TABLE_ID, 0));
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 0))).isEqualTo(100L);
    }

    @Test
    void testNewBucketUsesZeroAsRecoveryOffset() throws Exception {
        // Setup: bucket 1 has data but not in checkpoint
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        currentOffsets.put(new TableBucket(TABLE_ID, 1), 200L); // new bucket with data

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        TableInfo tableInfo = createTableInfo(2, false);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        // Checkpoint only has bucket 0
        Map<TableBucket, Long> checkpointOffsets = new HashMap<>();
        checkpointOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        WriterState state = new WriterState(checkpointOffsets);

        // Execute
        RecoveryOffsetManager.RecoveryDecision decision =
                manager.determineRecoveryStrategy(Collections.singletonList(state));

        // Verify: bucket 0 unchanged, bucket 1 needs recovery from 0
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.CHECKPOINT_RECOVERY);
        assertThat(decision.needsUndoRecovery()).isTrue();
        assertThat(decision.getRecoveryOffsets()).hasSize(1);
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 1))).isEqualTo(0L);
    }

    // ==================== PRODUCER_OFFSET_RECOVERY Tests ====================

    @Test
    void testProducerOffsetRecoveryTask0() throws Exception {
        // Setup: Task0 registers offsets, then data is written
        Map<TableBucket, Long> initialOffsets = new HashMap<>();
        initialOffsets.put(new TableBucket(TABLE_ID, 0), 100L);

        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 0), 150L); // data written after registration

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        admin.setInitialOffsetsForRegistration(initialOffsets);

        TableInfo tableInfo = createTableInfo(1, false);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        // Execute: null means no checkpoint
        RecoveryOffsetManager.RecoveryDecision decision = manager.determineRecoveryStrategy(null);

        // Verify
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.PRODUCER_OFFSET_RECOVERY);
        assertThat(decision.needsUndoRecovery()).isTrue();
        assertThat(decision.getRecoveryOffsets()).hasSize(1);
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 0))).isEqualTo(100L);

        // Verify Task0 registered offsets
        assertThat(admin.wasRegisterCalled()).isTrue();
    }

    @Test
    void testProducerOffsetRecoveryNonTask0() throws Exception {
        // Setup: Non-Task0 should poll for offsets registered by Task0
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 0), 150L);
        currentOffsets.put(new TableBucket(TABLE_ID, 1), 250L);

        // Pre-registered offsets (simulating Task0 already registered)
        Map<TableBucket, Long> registeredOffsets = new HashMap<>();
        registeredOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        registeredOffsets.put(new TableBucket(TABLE_ID, 1), 200L);

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        admin.setPreRegisteredOffsets(registeredOffsets);

        TableInfo tableInfo = createTableInfo(2, false);
        // subtaskIndex=1, parallelism=2: bucket 1 assigned
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 1, 2, 10L, 5000L, TABLE_PATH, tableInfo);

        // Execute
        RecoveryOffsetManager.RecoveryDecision decision = manager.determineRecoveryStrategy(null);

        // Verify: non-Task0 should NOT call register
        assertThat(admin.wasRegisterCalled()).isFalse();
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.PRODUCER_OFFSET_RECOVERY);
        assertThat(decision.needsUndoRecovery()).isTrue();
        // Only bucket 1 assigned to subtask 1
        assertThat(decision.getRecoveryOffsets()).hasSize(1);
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 1))).isEqualTo(200L);
    }

    @Test
    void testNoUndoNeededWhenProducerOffsetsMatchCurrent() throws Exception {
        // Setup: registered offsets match current (no writes after registration)
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 0), 100L);

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        admin.setInitialOffsetsForRegistration(currentOffsets);

        TableInfo tableInfo = createTableInfo(1, false);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        // Execute
        RecoveryOffsetManager.RecoveryDecision decision = manager.determineRecoveryStrategy(null);

        // Verify: no change needed
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.FRESH_START);
        assertThat(decision.needsUndoRecovery()).isFalse();
    }

    // ==================== Sharding Tests ====================

    @Test
    void testShardingFiltersBuckets() throws Exception {
        // Setup: 3 buckets, parallelism 3
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 0), 150L);
        currentOffsets.put(new TableBucket(TABLE_ID, 1), 250L);
        currentOffsets.put(new TableBucket(TABLE_ID, 2), 350L);

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        TableInfo tableInfo = createTableInfo(3, false);
        // subtask 0 with parallelism 3: only bucket 0 assigned (0 % 3 = 0)
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 3, 10L, 5000L, TABLE_PATH, tableInfo);

        Map<TableBucket, Long> checkpointOffsets = new HashMap<>();
        checkpointOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        checkpointOffsets.put(new TableBucket(TABLE_ID, 1), 200L);
        checkpointOffsets.put(new TableBucket(TABLE_ID, 2), 300L);
        WriterState state = new WriterState(checkpointOffsets);

        // Execute
        RecoveryOffsetManager.RecoveryDecision decision =
                manager.determineRecoveryStrategy(Collections.singletonList(state));

        // Verify: only bucket 0 in recovery offsets
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.CHECKPOINT_RECOVERY);
        assertThat(decision.needsUndoRecovery()).isTrue();
        assertThat(decision.getRecoveryOffsets()).hasSize(1);
        assertThat(decision.getRecoveryOffsets()).containsKey(new TableBucket(TABLE_ID, 0));
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 0))).isEqualTo(100L);
    }

    @Test
    void testSubtaskWithNoBuckets() throws Exception {
        // Setup: 2 buckets, parallelism 4 -> subtask 2 and 3 have no buckets
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        currentOffsets.put(new TableBucket(TABLE_ID, 1), 200L);

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        TableInfo tableInfo = createTableInfo(2, false);
        // subtask 2 with parallelism 4: no buckets assigned
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 2, 4, 10L, 5000L, TABLE_PATH, tableInfo);

        Map<TableBucket, Long> checkpointOffsets = new HashMap<>();
        checkpointOffsets.put(new TableBucket(TABLE_ID, 0), 50L);
        checkpointOffsets.put(new TableBucket(TABLE_ID, 1), 100L);
        WriterState state = new WriterState(checkpointOffsets);

        // Execute
        RecoveryOffsetManager.RecoveryDecision decision =
                manager.determineRecoveryStrategy(Collections.singletonList(state));

        // Verify
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.FRESH_START);
        assertThat(decision.needsUndoRecovery()).isFalse();
        assertThat(decision.getRecoveryOffsets()).isNull();
    }

    // ==================== Partitioned Table Tests ====================

    @Test
    void testPartitionedTableRecovery() throws Exception {
        // Setup: partitioned table with 2 partitions
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 1L, 0), 150L);
        currentOffsets.put(new TableBucket(TABLE_ID, 2L, 0), 250L);

        List<PartitionInfo> partitions =
                Arrays.asList(createPartitionInfo(1L, "p1"), createPartitionInfo(2L, "p2"));

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        admin.setPartitions(partitions);

        TableInfo tableInfo = createTableInfo(1, true);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        Map<TableBucket, Long> checkpointOffsets = new HashMap<>();
        checkpointOffsets.put(new TableBucket(TABLE_ID, 1L, 0), 100L);
        checkpointOffsets.put(new TableBucket(TABLE_ID, 2L, 0), 200L);
        WriterState state = new WriterState(checkpointOffsets);

        // Execute
        RecoveryOffsetManager.RecoveryDecision decision =
                manager.determineRecoveryStrategy(Collections.singletonList(state));

        // Verify
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.CHECKPOINT_RECOVERY);
        assertThat(decision.needsUndoRecovery()).isTrue();
        assertThat(decision.getRecoveryOffsets()).hasSize(2);
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 1L, 0)))
                .isEqualTo(100L);
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 2L, 0)))
                .isEqualTo(200L);
    }

    @Test
    void testNewPartitionUsesZeroAsRecoveryOffset() throws Exception {
        // Setup: partition 2 is new (not in checkpoint)
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 1L, 0), 100L);
        currentOffsets.put(new TableBucket(TABLE_ID, 2L, 0), 200L); // new partition

        List<PartitionInfo> partitions =
                Arrays.asList(createPartitionInfo(1L, "p1"), createPartitionInfo(2L, "p2"));

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        admin.setPartitions(partitions);

        TableInfo tableInfo = createTableInfo(1, true);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        // Checkpoint only has partition 1
        Map<TableBucket, Long> checkpointOffsets = new HashMap<>();
        checkpointOffsets.put(new TableBucket(TABLE_ID, 1L, 0), 100L);
        WriterState state = new WriterState(checkpointOffsets);

        // Execute
        RecoveryOffsetManager.RecoveryDecision decision =
                manager.determineRecoveryStrategy(Collections.singletonList(state));

        // Verify: partition 1 unchanged, partition 2 needs recovery from 0
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.CHECKPOINT_RECOVERY);
        assertThat(decision.needsUndoRecovery()).isTrue();
        assertThat(decision.getRecoveryOffsets()).hasSize(1);
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 2L, 0)))
                .isEqualTo(0L);
    }

    @Test
    void testPartitionedTableProducerOffsetRecoveryTask0() throws Exception {
        // Setup: Task0 registers offsets for partitioned table, then data is written
        Map<TableBucket, Long> initialOffsets = new HashMap<>();
        initialOffsets.put(new TableBucket(TABLE_ID, 1L, 0), 100L);
        initialOffsets.put(new TableBucket(TABLE_ID, 2L, 0), 200L);

        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(
                new TableBucket(TABLE_ID, 1L, 0), 150L); // data written after registration
        currentOffsets.put(new TableBucket(TABLE_ID, 2L, 0), 250L);

        List<PartitionInfo> partitions =
                Arrays.asList(createPartitionInfo(1L, "p1"), createPartitionInfo(2L, "p2"));

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        admin.setPartitions(partitions);
        admin.setInitialOffsetsForRegistration(initialOffsets);

        TableInfo tableInfo = createTableInfo(1, true);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        // Execute: null means no checkpoint (producer offset recovery)
        RecoveryOffsetManager.RecoveryDecision decision = manager.determineRecoveryStrategy(null);

        // Verify
        assertThat(decision.getStrategy())
                .isEqualTo(RecoveryOffsetManager.RecoveryStrategy.PRODUCER_OFFSET_RECOVERY);
        assertThat(decision.needsUndoRecovery()).isTrue();
        assertThat(decision.getRecoveryOffsets()).hasSize(2);
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 1L, 0)))
                .isEqualTo(100L);
        assertThat(decision.getRecoveryOffsets().get(new TableBucket(TABLE_ID, 2L, 0)))
                .isEqualTo(200L);

        // Verify Task0 registered offsets
        assertThat(admin.wasRegisterCalled()).isTrue();
    }

    // ==================== Error Handling Tests ====================

    @Test
    void testTableRecreatedThrowsException() throws Exception {
        // Simulates: savepoint → drop table → re-create table → restore from savepoint.
        // The checkpoint state contains buckets with the old table ID (TABLE_ID=1),
        // but the re-created table has a new table ID (NEW_TABLE_ID=999).
        long oldTableId = TABLE_ID;
        long newTableId = 999L;

        // Setup: re-created table with new tableId, current offsets are 0 (fresh table)
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(newTableId, 0), 0L);

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        // Use the package-private constructor to set the new tableId directly
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, newTableId, 1, false);

        // Checkpoint state from before table was dropped (old tableId)
        Map<TableBucket, Long> checkpointOffsets = new HashMap<>();
        checkpointOffsets.put(new TableBucket(oldTableId, 0), 100L);
        WriterState state = new WriterState(checkpointOffsets);

        // Execute & Verify: should detect table re-creation and throw
        assertThatThrownBy(
                        () -> manager.determineRecoveryStrategy(Collections.singletonList(state)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("re-created")
                .hasMessageContaining(String.valueOf(oldTableId))
                .hasMessageContaining(String.valueOf(newTableId));
    }

    @Test
    void testDataInconsistencyThrowsException() throws Exception {
        // Setup: checkpoint offset > current offset (data loss)
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 0), 50L); // current < checkpoint

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        TableInfo tableInfo = createTableInfo(1, false);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        Map<TableBucket, Long> checkpointOffsets = new HashMap<>();
        checkpointOffsets.put(new TableBucket(TABLE_ID, 0), 100L); // checkpoint > current
        WriterState state = new WriterState(checkpointOffsets);

        // Execute & Verify
        assertThatThrownBy(
                        () -> manager.determineRecoveryStrategy(Collections.singletonList(state)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Data inconsistency");
    }

    @Test
    void testListOffsetsFailureThrowsException() throws Exception {
        // Setup: Admin throws exception on listOffsets
        RecoveryTestAdmin admin = new RecoveryTestAdmin(new HashMap<>());
        admin.setListOffsetsFailure(new RuntimeException("Connection failed"));

        TableInfo tableInfo = createTableInfo(1, false);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        Map<TableBucket, Long> checkpointOffsets = new HashMap<>();
        checkpointOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        WriterState state = new WriterState(checkpointOffsets);

        // Execute & Verify
        assertThatThrownBy(
                        () -> manager.determineRecoveryStrategy(Collections.singletonList(state)))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasMessageContaining("Connection failed");
    }

    @Test
    void testListPartitionInfosFailureThrowsException() throws Exception {
        // Setup: Admin throws exception on listPartitionInfos
        Map<TableBucket, Long> currentOffsets = new HashMap<>();
        currentOffsets.put(new TableBucket(TABLE_ID, 1L, 0), 100L);

        RecoveryTestAdmin admin = new RecoveryTestAdmin(currentOffsets);
        admin.setListPartitionInfosFailure(new RuntimeException("Partition lookup failed"));

        TableInfo tableInfo = createTableInfo(1, true);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        Map<TableBucket, Long> checkpointOffsets = new HashMap<>();
        checkpointOffsets.put(new TableBucket(TABLE_ID, 1L, 0), 50L);
        WriterState state = new WriterState(checkpointOffsets);

        // Execute & Verify
        assertThatThrownBy(
                        () -> manager.determineRecoveryStrategy(Collections.singletonList(state)))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasMessageContaining("Partition lookup failed");
    }

    // ==================== cleanupOffsets Tests ====================

    @Test
    void testCleanupOffsetsTask0() {
        // Setup
        RecoveryTestAdmin admin = new RecoveryTestAdmin(new HashMap<>());
        TableInfo tableInfo = createTableInfo(1, false);
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 0, 1, 10L, 5000L, TABLE_PATH, tableInfo);

        // Execute
        manager.cleanupOffsets();

        // Verify: Task0 should call deleteProducerOffsets
        assertThat(admin.wasDeleteCalled()).isTrue();
        assertThat(admin.getDeletedProducerId()).isEqualTo(PRODUCER_ID);
    }

    @Test
    void testCleanupOffsetsNonTask0() {
        // Setup
        RecoveryTestAdmin admin = new RecoveryTestAdmin(new HashMap<>());
        TableInfo tableInfo = createTableInfo(1, false);
        // subtaskIndex=1 (non-Task0)
        RecoveryOffsetManager manager =
                new RecoveryOffsetManager(
                        admin, PRODUCER_ID, 1, 2, 10L, 5000L, TABLE_PATH, tableInfo);

        // Execute
        manager.cleanupOffsets();

        // Verify: non-Task0 should NOT call deleteProducerOffsets
        assertThat(admin.wasDeleteCalled()).isFalse();
    }

    // ==================== Test Admin Implementation ====================

    /**
     * A test Admin implementation that simulates real Admin behavior.
     *
     * <p>Key behaviors:
     *
     * <ul>
     *   <li>listOffsets - returns configured current offsets
     *   <li>listPartitionInfos - returns configured partitions
     *   <li>registerProducerOffsets - stores offsets atomically (simulates Task0 behavior)
     *   <li>getProducerOffsets - returns registered offsets (simulates polling)
     *   <li>deleteProducerOffsets - tracks deletion calls
     * </ul>
     */
    private static class RecoveryTestAdmin extends TestAdminAdapter {
        private final Map<TableBucket, Long> currentOffsets;
        private List<PartitionInfo> partitions = new ArrayList<>();

        // For simulating registration behavior
        private Map<TableBucket, Long> initialOffsetsForRegistration;
        private Map<TableBucket, Long> preRegisteredOffsets;
        private Map<TableBucket, Long> registeredOffsets;
        private boolean registerCalled = false;

        // For simulating deletion
        private boolean deleteCalled = false;
        private String deletedProducerId;

        // For simulating failures
        private RuntimeException listOffsetsFailure;
        private RuntimeException listPartitionInfosFailure;

        RecoveryTestAdmin(Map<TableBucket, Long> currentOffsets) {
            this.currentOffsets = currentOffsets;
        }

        void setPartitions(List<PartitionInfo> partitions) {
            this.partitions = partitions;
        }

        void setInitialOffsetsForRegistration(Map<TableBucket, Long> offsets) {
            this.initialOffsetsForRegistration = offsets;
        }

        void setPreRegisteredOffsets(Map<TableBucket, Long> offsets) {
            this.preRegisteredOffsets = offsets;
        }

        void setListOffsetsFailure(RuntimeException e) {
            this.listOffsetsFailure = e;
        }

        void setListPartitionInfosFailure(RuntimeException e) {
            this.listPartitionInfosFailure = e;
        }

        boolean wasRegisterCalled() {
            return registerCalled;
        }

        boolean wasDeleteCalled() {
            return deleteCalled;
        }

        String getDeletedProducerId() {
            return deletedProducerId;
        }

        @Override
        public ListOffsetsResult listOffsets(
                TablePath tablePath, Collection<Integer> buckets, OffsetSpec offsetSpec) {
            if (listOffsetsFailure != null) {
                return createFailedListOffsetsResult(buckets, listOffsetsFailure);
            }
            return createListOffsetsResult(null, buckets);
        }

        @Override
        public ListOffsetsResult listOffsets(
                TablePath tablePath,
                String partitionName,
                Collection<Integer> buckets,
                OffsetSpec offsetSpec) {
            if (listOffsetsFailure != null) {
                return createFailedListOffsetsResult(buckets, listOffsetsFailure);
            }
            Long partitionId = findPartitionId(partitionName);
            return createListOffsetsResult(partitionId, buckets);
        }

        private Long findPartitionId(String partitionName) {
            for (PartitionInfo p : partitions) {
                if (p.getPartitionName().equals(partitionName)) {
                    return p.getPartitionId();
                }
            }
            return null;
        }

        private ListOffsetsResult createListOffsetsResult(
                Long partitionId, Collection<Integer> buckets) {
            Map<Integer, CompletableFuture<Long>> futures = new HashMap<>();
            for (Integer bucketId : buckets) {
                TableBucket tb =
                        partitionId != null
                                ? new TableBucket(TABLE_ID, partitionId, bucketId)
                                : new TableBucket(TABLE_ID, bucketId);
                long offset = currentOffsets.getOrDefault(tb, 0L);
                futures.put(bucketId, CompletableFuture.completedFuture(offset));
            }
            return new ListOffsetsResult(futures);
        }

        private ListOffsetsResult createFailedListOffsetsResult(
                Collection<Integer> buckets, RuntimeException failure) {
            Map<Integer, CompletableFuture<Long>> futures = new HashMap<>();
            for (Integer bucketId : buckets) {
                CompletableFuture<Long> future = new CompletableFuture<>();
                future.completeExceptionally(failure);
                futures.put(bucketId, future);
            }
            return new ListOffsetsResult(futures);
        }

        @Override
        public CompletableFuture<List<PartitionInfo>> listPartitionInfos(TablePath tablePath) {
            if (listPartitionInfosFailure != null) {
                CompletableFuture<List<PartitionInfo>> future = new CompletableFuture<>();
                future.completeExceptionally(listPartitionInfosFailure);
                return future;
            }
            return CompletableFuture.completedFuture(partitions);
        }

        @Override
        public CompletableFuture<RegisterResult> registerProducerOffsets(
                String producerId, Map<TableBucket, Long> offsets) {
            this.registerCalled = true;
            // Simulate atomic registration: use initialOffsetsForRegistration if set
            if (initialOffsetsForRegistration != null) {
                this.registeredOffsets = new HashMap<>(initialOffsetsForRegistration);
            } else {
                this.registeredOffsets = new HashMap<>(offsets);
            }
            return CompletableFuture.completedFuture(RegisterResult.CREATED);
        }

        @Override
        public CompletableFuture<ProducerOffsetsResult> getProducerOffsets(String producerId) {
            // Return pre-registered offsets if set (simulates non-Task0 polling)
            // Otherwise return offsets registered by this admin
            Map<TableBucket, Long> offsets =
                    preRegisteredOffsets != null ? preRegisteredOffsets : registeredOffsets;
            if (offsets == null) {
                offsets = new HashMap<>();
            }
            Map<Long, Map<TableBucket, Long>> byTable = new HashMap<>();
            byTable.put(TABLE_ID, offsets);
            long farFutureExpiration = System.currentTimeMillis() + 3600_000L;
            return CompletableFuture.completedFuture(
                    new ProducerOffsetsResult(producerId, byTable, farFutureExpiration));
        }

        @Override
        public CompletableFuture<Void> deleteProducerOffsets(String producerId) {
            this.deleteCalled = true;
            this.deletedProducerId = producerId;
            return CompletableFuture.completedFuture(null);
        }
    }
}
