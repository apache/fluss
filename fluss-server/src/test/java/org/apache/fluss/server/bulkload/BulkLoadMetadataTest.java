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
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Stable tests for the minimal persisted BulkLoad facts. */
class BulkLoadMetadataTest {

    private static final String SHA256 =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    @Test
    void testFenceRoundTripPreservesBucketRouting() {
        String id = "550e8400-e29b-41d4-a716-446655440000";
        TableRegistration table =
                TableRegistration.newTable(41L, "file:///warehouse", DATA1_TABLE_DESCRIPTOR)
                        .newBucketCount(8);
        TableRegistration loadingTable =
                ZkData.TableZNode.decode(
                        ZkData.TableZNode.encode(
                                table.withDataState(BulkLoadDataState.LOADING, id)));
        assertThat(loadingTable.bucketCount).isEqualTo(8);
        assertThat(loadingTable.bucketCountEpoch).isEqualTo(1L);
        assertThat(
                        ZkData.TableZNode.decode(
                                ZkData.TableZNode.encode(
                                        loadingTable.withDataState(
                                                BulkLoadDataState.ACTIVE, null))))
                .isEqualTo(table);

        PartitionRegistration partition =
                new PartitionRegistration(41L, 71L, "file:///warehouse", 3);
        PartitionRegistration loadingPartition =
                ZkData.PartitionZNode.decode(
                        ZkData.PartitionZNode.encode(
                                partition.withDataState(BulkLoadDataState.LOADING, id)));
        assertThat(loadingPartition.getBucketCount()).isEqualTo(3);
        assertThat(
                        ZkData.PartitionZNode.decode(
                                ZkData.PartitionZNode.encode(
                                        loadingPartition.withDataState(
                                                BulkLoadDataState.ACTIVE, null))))
                .isEqualTo(partition);
    }

    @Test
    void testTransactionGoldenJsonRoundTrip() {
        BulkLoadTransaction transaction = committingTransaction();

        byte[] transactionBytes = ZkData.BulkLoadTransactionsZNode.encode(transaction);

        assertThat(new String(transactionBytes, StandardCharsets.UTF_8))
                .isEqualTo(
                        "{\"version\":1,\"bulk_load_id\":\"550e8400-e29b-41d4-a716-446655440000\",\"state\":1,\"database_name\":\"db\",\"table_name\":\"table\",\"table_id\":41,\"creator_name\":\"alice\",\"creator_type\":\"USER\",\"remote_data_dir\":\"file:///warehouse\",\"schema_id\":3,\"metadata_version\":7,\"snapshot_ids\":[17],\"deadline_ms\":400,\"input_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}");
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
                        null,
                        null,
                        500L,
                        BulkLoadAbortReason.ABORTED_BY_CALLER,
                        error);

        assertThat(transaction.getAbortMessage()).isNotNull();
        assertThat(transaction.getAbortMessage().getBytes(StandardCharsets.UTF_8).length)
                .isLessThanOrEqualTo(4096);
        assertThat(transaction.getAbortMessage()).doesNotEndWith("�");
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
                        PhysicalTablePath.of("db", "table", null),
                        41L,
                        null,
                        "550e8400-e29b-41d4-a716-446655440000"),
                state,
                "alice",
                "USER",
                "file:///warehouse",
                3,
                7,
                snapshotIds,
                deadlineMs,
                inputSha256,
                abortReason,
                abortMessage);
    }
}
