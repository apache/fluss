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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.OperationNotAttemptedException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.ZkData.TableZNode;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests DDL identity protection through metadata manager entry points. */
class MetadataManagerTest {
    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private ZooKeeperClient client;
    private MetadataManager metadata;
    private final TablePath table = TablePath.of("db", "t");
    private final TableDescriptor descriptor =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("id", DataTypes.INT())
                                    .column("p", DataTypes.STRING())
                                    .build())
                    .partitionedBy("p")
                    .distributedBy(1)
                    .build();

    @BeforeEach
    void setUp() throws Exception {
        client = ZOOKEEPER.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
        int epoch = client.fenceBecomeCoordinatorLeader("leader").getCoordinatorEpochZkVersion();
        metadata =
                new MetadataManager(
                                client,
                                new Configuration(),
                                new LakeCatalogDynamicLoader(new Configuration(), null, true))
                        .forLeader(epoch);
        metadata.createDatabase("db", DatabaseDescriptor.builder().build(), false);
    }

    @AfterEach
    void tearDown() {
        ZOOKEEPER.getCustomExtension().cleanupRoot();
    }

    @Test
    void testCreateRecoversUnpublishedSchemasAfterFailover() throws Exception {
        Schema abandonedSchema =
                Schema.newBuilder().column("abandoned", DataTypes.STRING()).build();
        client.registerFirstSchema(table, abandonedSchema);
        assertThat(client.tableExist(table)).isFalse();

        int epoch =
                client.fenceBecomeCoordinatorLeader("next-leader").getCoordinatorEpochZkVersion();
        MetadataManager next = metadata.forLeader(epoch);
        long tableId = next.createTable(table, "/table", descriptor, null, false);

        assertThat(next.getTable(table).getTableId()).isEqualTo(tableId);
        assertThat(next.getTable(table).getSchema()).isEqualTo(descriptor.getSchema());
        assertThat(client.getSchemaById(table, 1).get().getSchema())
                .isEqualTo(descriptor.getSchema());
    }

    @Test
    void testDropIfExistsCompletesWithdrawnNamespaceCleanup() throws Exception {
        metadata.createTable(table, "/table", descriptor, null, false);
        client.getCuratorClient().setData().forPath(TableZNode.path(table), new byte[0]);
        assertThat(metadata.tableExists(table)).isFalse();

        metadata.dropTable(table, true);
        metadata.dropTable(table, true);

        assertThat(client.getOrEmpty(TableZNode.path(table))).isEmpty();
    }

    @Test
    void testOldPartitionTaskCannotCreateOrDeleteInRecreatedTable() throws Exception {
        long oldId = metadata.createTable(table, "/old", descriptor, null, false);
        metadata.dropTable(table, false);
        long newId = metadata.createTable(table, "/new", descriptor, null, false);
        ResolvedPartitionSpec partition =
                new ResolvedPartitionSpec(
                        Collections.singletonList("p"), Collections.singletonList("value"));
        PartitionAssignment assignment =
                new PartitionAssignment(
                        newId,
                        Collections.singletonMap(
                                0, new BucketAssignment(Collections.singletonList(1))));
        metadata.createPartition(table, newId, "/new", assignment, partition, false, 1);
        long partitionId = client.getPartition(table, "value").get().getPartitionId();
        assertThatThrownBy(
                        () ->
                                metadata.createPartition(
                                        table,
                                        oldId,
                                        "/old",
                                        new PartitionAssignment(
                                                oldId, assignment.getBucketAssignments()),
                                        partition,
                                        true,
                                        1))
                .isInstanceOf(OperationNotAttemptedException.class);
        assertThatThrownBy(() -> metadata.dropPartition(table, oldId, partition, true))
                .isInstanceOf(OperationNotAttemptedException.class);
        assertThat(client.getPartition(table, "value").get().getPartitionId())
                .isEqualTo(partitionId);
        assertThat(metadata.getTable(table).getTableId()).isEqualTo(newId).isNotEqualTo(oldId);
    }

    @Test
    void testPartitionAssignmentCannotReferenceAnotherTable() throws Exception {
        long id = metadata.createTable(table, "/table", descriptor, null, false);
        ResolvedPartitionSpec partition =
                new ResolvedPartitionSpec(
                        Collections.singletonList("p"), Collections.singletonList("value"));
        PartitionAssignment assignment =
                new PartitionAssignment(
                        id + 1, Collections.singletonMap(0, BucketAssignment.of(1)));
        assertThatThrownBy(
                        () ->
                                metadata.createPartition(
                                        table, id, "/table", assignment, partition, false, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(client.getPartition(table, "value")).isEmpty();
    }

    @Test
    void testRequestWaitsForBackgroundTableDdl() throws Exception {
        CompletableFuture<Long> created = new CompletableFuture<>();
        Thread request =
                new Thread(
                        () -> {
                            try {
                                created.complete(
                                        metadata.createTable(
                                                table, "/table", descriptor, null, false));
                            } catch (Throwable error) {
                                created.completeExceptionally(error);
                            }
                        });
        try {
            metadata.runTableDdl(
                    table,
                    scoped -> {
                        request.start();
                        try {
                            waitUntil(
                                    () ->
                                            created.isDone()
                                                    || request.getState() == Thread.State.WAITING,
                                    Duration.ofSeconds(10),
                                    "Request neither waited for DDL nor completed");
                        } catch (Exception error) {
                            throw new RuntimeException(error);
                        }
                        assertThat(created).isNotDone();
                        return null;
                    });
            long tableId = created.get();
            assertThat(metadata.getTable(table).getTableId()).isEqualTo(tableId);
        } finally {
            request.interrupt();
            request.join();
        }
    }

    @Test
    void testScopeCoversNestedOperationsAndReleasesAfterFailure() {
        assertThatThrownBy(
                        () ->
                                metadata.runDatabaseDdl(
                                        "db",
                                        scoped -> {
                                            scoped.createTable(
                                                    table, "/table", descriptor, null, false);
                                            assertThatThrownBy(
                                                            () ->
                                                                    metadata.tryRunTableDdl(
                                                                            table, other -> null))
                                                    .isInstanceOf(
                                                            OperationNotAttemptedException.class);
                                            scoped.dropTable(table, false);
                                            throw new IllegalArgumentException(
                                                    "failure after nested work");
                                        }))
                .isInstanceOf(IllegalArgumentException.class);
        metadata.dropDatabase("db", false, true);
        assertThat(metadata.databaseExists("db")).isFalse();
    }

    @Test
    void testOldLeaderCannotStartDdlAfterEpochAdvances() throws Exception {
        metadata.createTable(table, "/table", descriptor, null, false);
        client.fenceBecomeCoordinatorLeader("next-leader");
        assertThatThrownBy(() -> metadata.dropTable(table, false))
                .hasRootCauseInstanceOf(
                        org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException
                                .BadVersionException.class);
        assertThat(client.getTable(table)).isPresent();
    }

    @Test
    void testMissingDatabaseFallbackProceedsWithOnlyOneRemainingRequestSlot() throws Exception {
        Deque<Runnable> heldTasks = new ArrayDeque<>();
        Deque<Runnable> stages = new ArrayDeque<>();
        List<CompletableFuture<String>> heldRequests = new ArrayList<>();
        TablePath target = TablePath.of("missing", "created");
        try {
            for (int i = 0; i < DdlAdmission.MAX_ASYNC_REQUESTS - 1; i++) {
                heldRequests.add(
                        metadata.runTableDdlAsync(
                                TablePath.of("db", "held"), heldTasks::add, scoped -> "held"));
            }
            CompletableFuture<Long> created =
                    metadata.runTableOrDatabaseDdlAsync(
                            target,
                            stages::add,
                            () -> descriptor,
                            (scoped, prepared) -> {
                                scoped.createDatabase(
                                        "missing", DatabaseDescriptor.builder().build(), true);
                                return scoped.createTable(
                                        target, "/created", prepared, null, false);
                            });
            while (!stages.isEmpty()) {
                stages.remove().run();
            }
            assertThat(created.get(10, TimeUnit.SECONDS)).isNotNegative();
            assertThat(metadata.getTable(target).getSchema()).isEqualTo(descriptor.getSchema());
        } finally {
            heldRequests.forEach(request -> request.cancel(false));
            while (!heldTasks.isEmpty()) {
                heldTasks.remove().run();
            }
            while (!stages.isEmpty()) {
                stages.remove().run();
            }
        }
    }
}
