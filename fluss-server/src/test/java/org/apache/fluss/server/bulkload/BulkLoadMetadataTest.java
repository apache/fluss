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

package org.apache.fluss.server.bulkload;

import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Stable tests for the minimal persisted BulkLoad facts. */
class BulkLoadMetadataTest {

    private static final String SHA256 =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    @Test
    void testOwnershipRoundTripPreservesBucketRouting() {
        String id = "550e8400-e29b-41d4-a716-446655440000";
        TableRegistration table =
                TableRegistration.newTable(41L, "file:///warehouse", DATA1_TABLE_DESCRIPTOR)
                        .newBucketCount(8);
        TableRegistration loadingTable =
                ZkData.TableZNode.decode(ZkData.TableZNode.encode(table.withBulkLoadId(id)));
        assertThat(loadingTable.bucketCount).isEqualTo(8);
        assertThat(loadingTable.bucketCountEpoch).isEqualTo(1L);
        assertThat(
                        ZkData.TableZNode.decode(
                                ZkData.TableZNode.encode(loadingTable.withBulkLoadId(null))))
                .isEqualTo(table);

        PartitionRegistration partition =
                new PartitionRegistration(41L, 71L, "file:///warehouse", 3);
        PartitionRegistration loadingPartition =
                ZkData.PartitionZNode.decode(
                        ZkData.PartitionZNode.encode(partition.withBulkLoadId(id)));
        assertThat(loadingPartition.getBucketCount()).isEqualTo(3);
        assertThat(
                        ZkData.PartitionZNode.decode(
                                ZkData.PartitionZNode.encode(
                                        loadingPartition.withBulkLoadId(null))))
                .isEqualTo(partition);
    }

    @Test
    void testTransactionGoldenJsonRoundTrip() {
        BulkLoadTransaction transaction = committingTransaction();

        byte[] transactionBytes = ZkData.BulkLoadTransactionsZNode.encode(transaction);

        assertThat(new String(transactionBytes, StandardCharsets.UTF_8))
                .isEqualTo(
                        "{\"version\":1,\"bulk_load_id\":\"550e8400-e29b-41d4-a716-446655440000\",\"state\":1,\"database_name\":\"db\",\"table_name\":\"table\",\"table_id\":41,\"creator_name\":\"alice\",\"creator_type\":\"USER\",\"targets\":[{\"remote_data_dir\":\"file:///warehouse\",\"schema_id\":3,\"registration_version\":7,\"snapshot_ids\":[17]}],\"deadline_ms\":400,\"input_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}");
        assertThat(ZkData.BulkLoadTransactionsZNode.decode(transactionBytes))
                .isEqualTo(transaction);
    }

    @Test
    void testTransactionRejectsCoercedFields() {
        byte[] encoded = ZkData.BulkLoadTransactionsZNode.encode(committingTransaction());
        assertThatThrownBy(
                        () ->
                                ZkData.BulkLoadTransactionsZNode.decode(
                                        new String(encoded, StandardCharsets.UTF_8)
                                                .replace(
                                                        "\"snapshot_ids\":[17]",
                                                        "\"snapshot_ids\":[]")
                                                .getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot ID");
        assertThatThrownBy(
                        () ->
                                ZkData.BulkLoadTransactionsZNode.decode(
                                        new String(encoded, StandardCharsets.UTF_8)
                                                .replace(
                                                        "\"snapshot_ids\":[17]",
                                                        "\"snapshot_ids\":[-1]")
                                                .getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot ID");
        assertThatThrownBy(
                        () ->
                                ZkData.BulkLoadTransactionsZNode.decode(
                                        new String(encoded, StandardCharsets.UTF_8)
                                                .replace("\"state\":1,", "")
                                                .getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("state");
    }

    @Test
    void testAbortMessageIsBoundedWithoutSplittingUtf8() {
        String error = String.join("", Collections.nCopies(2000, "错误"));
        BulkLoadTransaction transaction =
                transaction(
                        BulkLoadState.ABORTED,
                        new long[] {17L},
                        null,
                        500L,
                        BulkLoadAbortReason.ABORTED_BY_CALLER,
                        error);

        assertThat(transaction.getAbortMessage()).isNotNull();
        assertThat(transaction.getAbortMessage().getBytes(StandardCharsets.UTF_8).length)
                .isLessThanOrEqualTo(4096);
        assertThat(transaction.getAbortMessage()).doesNotEndWith("�");
    }

    @Test
    void testSharedTargetsAreSortedAndImmutable() {
        long[] ids = {23L, 29L};
        BulkLoadTarget first =
                new BulkLoadTarget(
                        PhysicalTablePath.of("db", "table", "p1"), 71L, "file:///p1", 3, 7, ids);
        BulkLoadTarget second =
                new BulkLoadTarget(
                        PhysicalTablePath.of("db", "table", "p2"),
                        72L,
                        "file:///p2",
                        4,
                        8,
                        new long[] {31L});
        List<BulkLoadTarget> targets = new ArrayList<>(Arrays.asList(second, first));
        BulkLoadTransaction transaction = sharedTransaction(targets);
        targets.clear();
        ids[0] = 99L;
        first.getSnapshotIds()[0] = 100L;
        assertThat(transaction.getTargets()).containsExactly(first, second);
        assertThat(first.getSnapshotIds()).containsExactly(23L, 29L);
        assertThat(first.getRegistrationVersion()).isEqualTo(7);
        assertThat(first.getMetadataPath())
                .isEqualTo(ZkData.PartitionZNode.path(TablePath.of("db", "table"), "p1"));
        assertThatThrownBy(() -> transaction.getTargets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(ZkData.BulkLoadTransactionsZNode.encode(transaction))
                .isEqualTo(
                        ZkData.BulkLoadTransactionsZNode.encode(
                                sharedTransaction(Arrays.asList(first, second))));
        assertThat(
                        ZkData.BulkLoadTransactionsZNode.decode(
                                ZkData.BulkLoadTransactionsZNode.encode(transaction)))
                .isEqualTo(transaction);
        assertThat(transaction.withInput(SHA256).committing().committed(500L).getState())
                .isEqualTo(BulkLoadState.COMMITTED);
        assertThat(transaction.withInput(SHA256).committing().committed(500L).getDeadlineMs())
                .isEqualTo(500L);
        assertThat(
                        transaction
                                .aborted(BulkLoadAbortReason.ABORTED_BY_CALLER, "cancelled", 600L)
                                .getAbortMessage())
                .isEqualTo("cancelled");
    }

    @Test
    void testRejectInvalidTargetSets() {
        BulkLoadTarget first =
                new BulkLoadTarget(
                        PhysicalTablePath.of("db", "table", "p1"),
                        71L,
                        "file:///p1",
                        3,
                        7,
                        new long[] {17L});
        assertThatThrownBy(() -> sharedTransaction(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sharedTransaction(Arrays.asList(first, first)))
                .isInstanceOf(IllegalArgumentException.class);
        BulkLoadTarget sameId =
                new BulkLoadTarget(
                        PhysicalTablePath.of("db", "table", "p2"),
                        71L,
                        "file:///p2",
                        3,
                        7,
                        new long[] {17L});
        assertThatThrownBy(() -> sharedTransaction(Arrays.asList(first, sameId)))
                .isInstanceOf(IllegalArgumentException.class);
        BulkLoadTarget samePath =
                new BulkLoadTarget(first.getTarget(), 72L, "file:///p2", 3, 7, new long[] {17L});
        assertThatThrownBy(() -> sharedTransaction(Arrays.asList(first, samePath)))
                .isInstanceOf(IllegalArgumentException.class);
        BulkLoadTarget otherTable =
                new BulkLoadTarget(
                        PhysicalTablePath.of("db", "other", "p2"),
                        72L,
                        "file:///p2",
                        3,
                        7,
                        new long[] {17L});
        assertThatThrownBy(() -> sharedTransaction(Arrays.asList(first, otherTable)))
                .isInstanceOf(IllegalArgumentException.class);
        BulkLoadTarget table =
                new BulkLoadTarget(
                        PhysicalTablePath.of("db", "table", null),
                        null,
                        "file:///warehouse",
                        3,
                        7,
                        new long[] {17L});
        assertThatThrownBy(() -> sharedTransaction(Arrays.asList(first, table)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(table.getMetadataPath())
                .isEqualTo(ZkData.TableZNode.path(TablePath.of("db", "table")));
    }

    @Test
    void testRejectMalformedTargetJson() {
        String json =
                new String(
                        ZkData.BulkLoadTransactionsZNode.encode(committingTransaction()),
                        StandardCharsets.UTF_8);
        for (String malformed :
                new String[] {
                    json.replace("\"registration_version\":7", "\"registration_version\":\"7\""),
                    json.replace(
                            "\"registration_version\":7", "\"registration_version\":2147483648"),
                    json.replace("\"snapshot_ids\":[17]", "\"snapshot_ids\":[17.0]"),
                    json.replace("\"snapshot_ids\":[17]", "\"snapshot_ids\":[9223372036854775808]"),
                    json.replace("\"snapshot_ids\":[17]", "\"snapshot_ids\":null"),
                    json.replace("\"registration_version\":7,", ""),
                    json.replace("\"version\":1", "\"version\":2")
                }) {
            assertThatThrownBy(
                            () ->
                                    ZkData.BulkLoadTransactionsZNode.decode(
                                            malformed.getBytes(StandardCharsets.UTF_8)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static BulkLoadTransaction sharedTransaction(List<BulkLoadTarget> targets) {
        return new BulkLoadTransaction(
                new BulkLoadHandle(
                        TablePath.of("db", "table"), 41L, "550e8400-e29b-41d4-a716-446655440000"),
                BulkLoadState.BEGUN,
                "alice",
                "USER",
                targets,
                400L,
                null,
                null,
                null);
    }

    private static BulkLoadTransaction committingTransaction() {
        return transaction(BulkLoadState.COMMITTING, new long[] {17L}, SHA256, 400L, null, null);
    }

    private static BulkLoadTransaction transaction(
            BulkLoadState state,
            long[] snapshotIds,
            String inputSha256,
            long deadlineMs,
            BulkLoadAbortReason abortReason,
            String abortMessage) {
        return new BulkLoadTransaction(
                new BulkLoadHandle(
                        TablePath.of("db", "table"), 41L, "550e8400-e29b-41d4-a716-446655440000"),
                state,
                "alice",
                "USER",
                Collections.singletonList(
                        new BulkLoadTarget(
                                PhysicalTablePath.of("db", "table", null),
                                null,
                                "file:///warehouse",
                                3,
                                7,
                                snapshotIds)),
                deadlineMs,
                inputSha256,
                abortReason,
                abortMessage);
    }
}
