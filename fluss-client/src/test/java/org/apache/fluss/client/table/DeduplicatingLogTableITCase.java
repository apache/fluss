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

package org.apache.fluss.client.table;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.TableAppend;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.testutils.InternalRowListAssert;
import org.apache.fluss.types.DataTypes;

import io.github.fluss_contrib.rocksdb.RocksIterator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.client.table.scanner.batch.BatchScanUtils.collectRows;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end first-write filtering through the public append interface. */
class DeduplicatingLogTableITCase extends ClientToServerITCaseBase {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRetryPreservesAcceptedRows(boolean rejectedBeforeAppend) throws Exception {
        clientConf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ofSeconds(30));
        TablePath path = TablePath.of("dedup_retry", "events_" + rejectedBeforeAppend);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .deduplicationKey("id")
                        .build();
        long id =
                createTable(
                        path,
                        TableDescriptor.builder().schema(schema).distributedBy(1).build(),
                        false);
        TableBucket bucket = new TableBucket(id, null, 0);
        AtomicBoolean lost = new AtomicBoolean();
        AtomicInteger stopped = new AtomicInteger(-1);
        RpcClient delegate = RpcClient.create(clientConf, TestingClientMetricGroup.newInstance());
        RpcClient transport =
                new RpcClient() {
                    @Override
                    public boolean connect(ServerNode node) {
                        return delegate.connect(node);
                    }

                    @Override
                    public boolean isReady(ServerNode node) {
                        return delegate.isReady(node);
                    }

                    @Override
                    public CompletableFuture<Void> disconnect(ServerNode node) {
                        return delegate.disconnect(node);
                    }

                    @Override
                    public CompletableFuture<Void> disconnect(String uid) {
                        return delegate.disconnect(uid);
                    }

                    @Override
                    public CompletableFuture<ApiMessage> sendRequest(
                            ServerNode node, ApiKeys api, ApiMessage request) {
                        if (api == ApiKeys.PUT_KV
                                && rejectedBeforeAppend
                                && lost.compareAndSet(false, true)) {
                            PutKvResponse rejected = new PutKvResponse();
                            rejected.addBucketsResp()
                                    .setBucketId(0)
                                    .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                                    .setErrorMessage("Leader is not ready before append");
                            return CompletableFuture.completedFuture(rejected);
                        }
                        CompletableFuture<ApiMessage> response =
                                delegate.sendRequest(node, api, request);
                        if (api == ApiKeys.PUT_KV
                                && !rejectedBeforeAppend
                                && lost.compareAndSet(false, true)) {
                            return response.thenCompose(
                                    committed ->
                                            CompletableFuture.supplyAsync(
                                                    () -> {
                                                        try {
                                                            FLUSS_CLUSTER_EXTENSION
                                                                    .triggerAndWaitSnapshot(path);
                                                            int leader =
                                                                    FLUSS_CLUSTER_EXTENSION
                                                                            .getZooKeeperClient()
                                                                            .getLeaderAndIsr(bucket)
                                                                            .get()
                                                                            .leader();
                                                            stopped.set(leader);
                                                            FLUSS_CLUSTER_EXTENSION
                                                                    .stopTabletServer(leader);
                                                        } catch (Exception error) {
                                                            throw new CompletionException(error);
                                                        }
                                                        throw new CompletionException(
                                                                new NetworkException(
                                                                        "Committed response was lost"));
                                                    }));
                        }
                        return response;
                    }

                    @Override
                    public void close() throws Exception {
                        delegate.close();
                    }
                };
        MetadataUpdater metadata = new MetadataUpdater(clientConf, transport);
        WriterClient client =
                new WriterClient(
                        clientConf, metadata, TestingWriterMetricGroup.newInstance(), admin);
        try (Table table = conn.getTable(path)) {
            AppendWriter writer =
                    new TableAppend(path, table.getTableInfo(), client).createWriter();
            CompletableFuture<AppendResult> first = writer.append(row(1, "first"));
            CompletableFuture<AppendResult> duplicate = writer.append(row(1, "replacement"));
            CompletableFuture<AppendResult> second = writer.append(row(2, "second"));
            writer.flush();
            first.get(60, TimeUnit.SECONDS);
            duplicate.get(60, TimeUnit.SECONDS);
            second.get(60, TimeUnit.SECONDS);
            try (Connection fresh = ConnectionFactory.createConnection(clientConf);
                    Table result = fresh.getTable(path)) {
                InternalRowListAssert.assertThatRows(
                                collectRows(result.newScan().limit(100).createBatchScanner(bucket)))
                        .withSchema(schema.getRowType())
                        .isEqualTo(Arrays.asList(row(1, "first"), row(2, "second")));
            }
            assertThat(lost).isTrue();
        } finally {
            client.close(Duration.ZERO);
            transport.close();
            if (stopped.get() >= 0) {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(stopped.get());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(LogFormat.class)
    void testSchemaEvolutionAndFilteredBatches(LogFormat format) throws Exception {
        TablePath path = TablePath.of("dedup_evolution", format.name().toLowerCase(Locale.ROOT));
        Schema schema =
                Schema.newBuilder().column("id", DataTypes.INT()).deduplicationKey("id").build();
        long id =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(1)
                                .logFormat(format)
                                .build(),
                        false);
        TableBucket bucket = new TableBucket(id, null, 0);
        try (Table original = conn.getTable(path)) {
            AppendWriter oldWriter = original.newAppend().createWriter();
            oldWriter.append(row(1)).get(30, TimeUnit.SECONDS);
            // A completely filtered batch must still advance the writer sequence.
            oldWriter.append(row(1)).get(30, TimeUnit.SECONDS);
            admin.alterTable(
                            path,
                            Collections.singletonList(
                                    TableChange.addColumn(
                                            "payload",
                                            DataTypes.STRING(),
                                            null,
                                            TableChange.ColumnPosition.last())),
                            false)
                    .get();
            oldWriter.append(row(2)).get(30, TimeUnit.SECONDS);
            try (Connection fresh = ConnectionFactory.createConnection(clientConf);
                    Table evolved = fresh.getTable(path)) {
                AppendWriter writer = evolved.newAppend().createWriter();
                writer.append(row(1, "replacement")).get(30, TimeUnit.SECONDS);
                writer.append(row(3, "new")).get(30, TimeUnit.SECONDS);
                writer.append(row(3, "ignored")).get(30, TimeUnit.SECONDS);
                InternalRowListAssert.assertThatRows(
                                collectRows(evolved.newScan().limit(1).createBatchScanner(bucket)))
                        .withSchema(evolved.getTableInfo().getRowType())
                        .isEqualTo(Collections.singletonList(row(3, "new")));
                InternalRowListAssert.assertThatRows(
                                collectRows(
                                        evolved.newScan().limit(100).createBatchScanner(bucket)))
                        .withSchema(evolved.getTableInfo().getRowType())
                        .isEqualTo(Arrays.asList(row(1, null), row(2, null), row(3, "new")));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(LogFormat.class)
    void testFirstWriteAndSnapshotRecovery(LogFormat format) throws Exception {
        TablePath path = TablePath.of("dedup_" + format.name().toLowerCase(Locale.ROOT), "events");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING().copy(false))
                        .deduplicationKey("id")
                        .build();
        long tableId =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(1)
                                .logFormat(format)
                                .build(),
                        false);
        TableBucket bucket = new TableBucket(tableId, null, 0);
        try (Table table = conn.getTable(path)) {
            assertThat(table.getTableInfo().hasPrimaryKey()).isFalse();
            assertThatThrownBy(() -> table.newScan().createBatchScanner(bucket))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("requires limit");
            assertThatThrownBy(() -> table.newScan().createBatchScanner(bucket, 1L))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("KV snapshot row scans");
            AppendWriter writer = table.newAppend().createWriter();
            List<CompletableFuture<AppendResult>> results = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                results.add(writer.append(row(i % 3, "value-" + i)));
            }
            writer.flush();
            for (int i = 0; i < results.size(); i++) {
                results.get(i).get(30, TimeUnit.SECONDS);
            }
            InternalRowListAssert.assertThatRows(
                            collectRows(table.newScan().limit(10).createBatchScanner(bucket)))
                    .withSchema(schema.getRowType())
                    .isEqualTo(
                            Arrays.asList(row(0, "value-0"), row(1, "value-1"), row(2, "value-2")));
            assertThatThrownBy(
                            () ->
                                    writer.append(
                                            GenericRow.of(
                                                    null, BinaryString.fromString("invalid"))))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> writer.append(GenericRow.of(42, null)))
                    .isInstanceOf(IllegalArgumentException.class);
            writer.append(row(42, "valid")).get(30, TimeUnit.SECONDS);
            assertThat(
                            collectRows(
                                    table.newScan()
                                            .limit(100)
                                            .project(new int[] {0})
                                            .createBatchScanner(bucket)))
                    .hasSize(4);
            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(path);
            Replica leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(bucket);
            RocksDBKv state = leader.getKvTablet().getRocksDBKv();
            try (RocksIterator keys =
                    state.getDb().newIterator(state.getDefaultColumnFamilyHandle())) {
                int count = 0;
                for (keys.seekToFirst(); keys.isValid(); keys.next()) {
                    assertThat(keys.value()).containsExactly((byte) 1);
                    count++;
                }
                keys.status();
                assertThat(count).isEqualTo(4);
            }
            assertThatThrownBy(() -> leader.appendRecordsToLeader(MemoryLogRecords.EMPTY, -1))
                    .isInstanceOf(InvalidPartitionException.class);
            int server =
                    FLUSS_CLUSTER_EXTENSION
                            .getZooKeeperClient()
                            .getLeaderAndIsr(bucket)
                            .get()
                            .leader();
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(server);
            try (Connection second = ConnectionFactory.createConnection(clientConf);
                    Table recovered = second.getTable(path)) {
                AppendWriter next = recovered.newAppend().createWriter();
                next.append(row(0, "replacement")).get(30, TimeUnit.SECONDS);
                assertThat(collectRows(recovered.newScan().limit(1).createBatchScanner(bucket)))
                        .hasSize(1);
                next.append(row(10, "new")).get(30, TimeUnit.SECONDS);
                InternalRowListAssert.assertThatRows(
                                collectRows(
                                        recovered.newScan().limit(100).createBatchScanner(bucket)))
                        .withSchema(schema.getRowType())
                        .isEqualTo(
                                Arrays.asList(
                                        row(0, "value-0"),
                                        row(1, "value-1"),
                                        row(2, "value-2"),
                                        row(42, "valid"),
                                        row(10, "new")));
            } finally {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(server);
            }
        }
    }
}
