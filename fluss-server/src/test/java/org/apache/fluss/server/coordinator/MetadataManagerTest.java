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
import org.apache.fluss.exception.PartitionAlreadyExistsException;
import org.apache.fluss.exception.TableNotExistException;
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
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.ZkData.PartitionIdsZNode;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.CheckedThread;
import org.apache.fluss.testutils.common.OneShotLatch;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests partition registration against concurrent table and database deletion. */
class MetadataManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final TablePath TABLE_PATH =
            TablePath.of("partition_registration", "test_table");
    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("id", DataTypes.INT())
                                    .column("dt", DataTypes.STRING())
                                    .build())
                    .partitionedBy("dt")
                    .distributedBy(1)
                    .build();

    private ZooKeeperClient zookeeperClient;
    private MetadataManager metadataManager;
    private String remoteDataDir;
    private long tableId;

    @BeforeEach
    void beforeEach() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        remoteDataDir = ZOO_KEEPER_EXTENSION.getCustomExtension().getRemoteDataDir();
        metadataManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        metadataManager.createDatabase(
                TABLE_PATH.getDatabaseName(), DatabaseDescriptor.EMPTY, false);
        tableId = createTable();
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION.getCustomExtension().cleanupRoot();
    }

    @Test
    void testCreateFirstPartitionAndIgnoreExistingPartition() throws Exception {
        assertThat(zookeeperClient.getPartitions(TABLE_PATH)).isEmpty();
        createPartition(metadataManager, tableId, "p1", false);
        PartitionRegistration partition = zookeeperClient.getPartition(TABLE_PATH, "p1").get();
        assertThat(partition.getTableId()).isEqualTo(tableId);
        assertThat(
                        zookeeperClient
                                .getPartitionAssignment(partition.getPartitionId())
                                .get()
                                .getTableId())
                .isEqualTo(tableId);

        createPartition(metadataManager, tableId, "p1", true);
        assertThat(zookeeperClient.getPartition(TABLE_PATH, "p1")).contains(partition);
        assertThatThrownBy(() -> createPartition(metadataManager, tableId, "p1", false))
                .isInstanceOf(PartitionAlreadyExistsException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLatePartitionRegistrationAfterDrop(boolean dropDatabase) throws Exception {
        dropTableOrDatabase(metadataManager, dropDatabase);

        for (boolean ignoreIfExists : new boolean[] {false, true}) {
            assertThatThrownBy(
                            () -> createPartition(metadataManager, tableId, "p1", ignoreIfExists))
                    .isInstanceOf(TableNotExistException.class);
        }
        assertThat(zookeeperClient.getTable(TABLE_PATH)).isEmpty();
        assertThat(zookeeperClient.getPartitions(TABLE_PATH)).isEmpty();
        assertThat(zookeeperClient.getChildren(PartitionIdsZNode.path())).isEmpty();
        assertThat(zookeeperClient.databaseExists(TABLE_PATH.getDatabaseName()))
                .isEqualTo(!dropDatabase);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLatePartitionRegistrationAfterTableRecreation(boolean ignoreIfExists)
            throws Exception {
        metadataManager.dropTable(TABLE_PATH, false);
        long newTableId = createTable();
        assertThat(newTableId).isNotEqualTo(tableId);

        assertThatThrownBy(() -> createPartition(metadataManager, tableId, "p1", ignoreIfExists))
                .isInstanceOf(TableNotExistException.class);
        assertThat(zookeeperClient.getPartitions(TABLE_PATH)).isEmpty();
        assertThat(zookeeperClient.getChildren(PartitionIdsZNode.path())).isEmpty();

        createPartition(metadataManager, newTableId, "p1", false);
        PartitionRegistration partition = zookeeperClient.getPartition(TABLE_PATH, "p1").get();
        List<String> assignments = zookeeperClient.getChildren(PartitionIdsZNode.path());
        // Validate the table identity before ignoreIfExists or PartitionAlreadyExists handling.
        assertThatThrownBy(() -> createPartition(metadataManager, tableId, "p1", ignoreIfExists))
                .isInstanceOf(TableNotExistException.class);
        assertThat(zookeeperClient.getPartition(TABLE_PATH, "p1")).contains(partition);
        assertThat(partition.getTableId()).isEqualTo(newTableId);
        assertThat(zookeeperClient.getChildren(PartitionIdsZNode.path()))
                .containsExactlyInAnyOrderElementsOf(assignments);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDropWaitsForValidatedPartitionRegistration(boolean dropDatabase) throws Exception {
        OneShotLatch tableValidated = new OneShotLatch();
        OneShotLatch continueRegistration = new OneShotLatch();
        MetadataManager blockingManager =
                new MetadataManager(
                        zookeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true)) {
                    @Override
                    Optional<PartitionRegistration> getOptionalPartitionRegistration(
                            TablePath tablePath, String partitionName) {
                        Optional<PartitionRegistration> partition =
                                super.getOptionalPartitionRegistration(tablePath, partitionName);
                        // Table identity is already validated. Pause before writing the partition.
                        tableValidated.trigger();
                        continueRegistration.awaitQuietly();
                        return partition;
                    }
                };
        CheckedThread creation =
                new CheckedThread("partition-creation") {
                    @Override
                    public void go() {
                        createPartition(blockingManager, tableId, "p1", false);
                    }
                };
        CheckedThread deletion =
                new CheckedThread("table-deletion") {
                    @Override
                    public void go() throws Exception {
                        tableValidated.await(10, TimeUnit.SECONDS);
                        dropTableOrDatabase(blockingManager, dropDatabase);
                    }
                };
        creation.start();
        deletion.start();
        try {
            tableValidated.await(10, TimeUnit.SECONDS);
            // Wait for actual contention, rather than assuming the deletion thread has run.
            waitUntil(
                    () ->
                            deletion.getState() == Thread.State.TERMINATED
                                    || waitsForThread(deletion, creation),
                    Duration.ofSeconds(10),
                    "Deletion should wait for the partition registration to finish.");
            assertThat(waitsForThread(deletion, creation)).isTrue();
            assertThat(zookeeperClient.getTable(TABLE_PATH)).isPresent();
            assertThat(zookeeperClient.getPartitions(TABLE_PATH)).isEmpty();
        } finally {
            continueRegistration.trigger();
            try {
                creation.sync(10000);
            } finally {
                deletion.sync(10000);
            }
        }
        assertThat(zookeeperClient.getTable(TABLE_PATH)).isEmpty();
        assertThat(zookeeperClient.getPartitions(TABLE_PATH)).isEmpty();
        assertThat(zookeeperClient.databaseExists(TABLE_PATH.getDatabaseName()))
                .isEqualTo(!dropDatabase);
    }

    private long createTable() {
        return metadataManager.createTable(
                TABLE_PATH, remoteDataDir, TABLE_DESCRIPTOR, null, false);
    }

    private void createPartition(
            MetadataManager manager, long targetTableId, String name, boolean ignoreIfExists) {
        manager.createPartition(
                TABLE_PATH,
                targetTableId,
                remoteDataDir,
                new PartitionAssignment(
                        targetTableId, Collections.singletonMap(0, BucketAssignment.of(0))),
                new ResolvedPartitionSpec(
                        Collections.singletonList("dt"), Collections.singletonList(name)),
                ignoreIfExists,
                1);
    }

    private void dropTableOrDatabase(MetadataManager manager, boolean dropDatabase) {
        if (dropDatabase) {
            manager.dropDatabase(TABLE_PATH.getDatabaseName(), false, true);
        } else {
            manager.dropTable(TABLE_PATH, false);
        }
    }

    private static boolean waitsForThread(Thread waiting, Thread owner) {
        ThreadInfo threadInfo = ManagementFactory.getThreadMXBean().getThreadInfo(waiting.getId());
        return threadInfo != null && threadInfo.getLockOwnerId() == owner.getId();
    }
}
