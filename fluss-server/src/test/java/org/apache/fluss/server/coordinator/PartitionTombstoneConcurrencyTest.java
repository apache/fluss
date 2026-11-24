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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.metadata.ResolvedPartitionSpec.fromPartitionName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/** Tests that index cleanup cannot tombstone a concurrently published live partition. */
class PartitionTombstoneConcurrencyTest {

    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private final TablePath tablePath = TablePath.of("db", "indexed");
    private ZooKeeperClient client;
    private Configuration config;
    private MetadataManager metadata;
    private Schema schema;
    private long tableId;
    private long oldPartitionId;
    private PartitionAssignment assignment;

    @BeforeEach
    void setUp() throws Exception {
        client = ZK.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
        config = new Configuration();
        config.set(ConfigOptions.REMOTE_DATA_DIR, client.getDefaultRemoteDataDir());
        metadata = manager(client);
        metadata.createDatabase("db", DatabaseDescriptor.builder().build(), false);
        schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("p", DataTypes.STRING())
                        .column("v", DataTypes.INT())
                        .primaryKey("id", "p")
                        .index(
                                "idx",
                                IndexType.SECONDARY,
                                Collections.singletonList("v"),
                                IndexVisibility.SYNC,
                                null)
                        .build();
        createIndexedTable();
    }

    private void createOldPartition() throws Exception {
        createPartition(metadata, "old");
        oldPartitionId = client.getPartition(tablePath, "old").get().getPartitionId();
    }

    private void createIndexedTable() {
        tableId =
                metadata.createTable(
                        tablePath,
                        client.getDefaultRemoteDataDir(),
                        TableDescriptor.builder()
                                .schema(schema)
                                .partitionedBy("p")
                                .distributedBy(1, "id")
                                .build(),
                        null,
                        false);
        assignment =
                new PartitionAssignment(
                        tableId, Collections.singletonMap(0, BucketAssignment.of(0)));
    }

    @AfterEach
    void cleanUp() {
        ZK.getCustomExtension().cleanupRoot();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void creationRetriesWhenDropOvertakesPublication(boolean tombstoneExists) throws Exception {
        if (tombstoneExists) {
            createOldPartition();
        }
        AtomicBoolean interleave = new AtomicBoolean(true);
        ZooKeeperClient delayedCreator =
                new ZooKeeperClient(client.getCuratorFrameworkWrapper(), config) {
                    @Override
                    public Tuple2<PartitionTombstone, Optional<Integer>>
                            getPartitionTombstoneWithVersion(TablePath path, long id)
                                    throws Exception {
                        Tuple2<PartitionTombstone, Optional<Integer>> observed =
                                super.getPartitionTombstoneWithVersion(path, id);
                        if (interleave.getAndSet(false)) {
                            if (!tombstoneExists) {
                                createOldPartition();
                            }
                            createPartition(metadata, "newer");
                            metadata.dropPartition(
                                    tablePath,
                                    tableId,
                                    fromPartitionName(Collections.singletonList("p"), "old"),
                                    false);
                        }
                        return observed;
                    }
                };
        createPartition(manager(delayedCreator), "delayed");

        long newerId = client.getPartition(tablePath, "newer").get().getPartitionId();
        long delayedId = client.getPartition(tablePath, "delayed").get().getPartitionId();
        PartitionTombstone tombstone = client.getPartitionTombstone(tablePath, tableId);
        assertThat(tombstone.isTombstoned(oldPartitionId)).isTrue();
        assertThat(tombstone.isTombstoned(newerId)).isFalse();
        assertThat(tombstone.isTombstoned(delayedId)).isFalse();
        assertThat(delayedId).isGreaterThan(newerId);
    }

    @Test
    void registrationInvalidatesConcurrentDropSnapshot() throws Exception {
        createOldPartition();
        long pendingId = client.getPartitionIdAndIncrement();
        createPartition(metadata, "newer");
        AtomicBoolean interleave = new AtomicBoolean(true);
        ZooKeeperClient delayedDropper =
                new ZooKeeperClient(client.getCuratorFrameworkWrapper(), config) {
                    @Override
                    public void deletePartitionAndSetTombstone(
                            TablePath path,
                            String partitionName,
                            long expectedTableId,
                            long expectedPartitionId,
                            PartitionTombstone tombstone,
                            int expectedTombstoneVersion)
                            throws Exception {
                        if (interleave.getAndSet(false)) {
                            client.registerPartitionAssignmentAndMetadata(
                                    pendingId,
                                    "pending",
                                    assignment,
                                    client.getDefaultRemoteDataDir(),
                                    tablePath,
                                    tableId,
                                    1,
                                    true);
                        }
                        super.deletePartitionAndSetTombstone(
                                path,
                                partitionName,
                                expectedTableId,
                                expectedPartitionId,
                                tombstone,
                                expectedTombstoneVersion);
                    }
                };
        manager(delayedDropper)
                .dropPartition(
                        tablePath,
                        tableId,
                        fromPartitionName(Collections.singletonList("p"), "old"),
                        false);

        assertThat(client.getPartition(tablePath, "pending")).isPresent();
        PartitionTombstone tombstone = client.getPartitionTombstone(tablePath, tableId);
        assertThat(tombstone.isTombstoned(oldPartitionId)).isTrue();
        assertThat(tombstone.isTombstoned(pendingId)).isFalse();
    }

    @Test
    void staleDropCannotDeletePartitionInRecreatedTable() throws Exception {
        createOldPartition();
        long originalTableId = tableId;
        CuratorFramework curator = client.getCuratorClient();
        CuratorFramework delayed = mock(CuratorFramework.class, delegatesTo(curator));
        doAnswer(
                        call -> {
                            // The drop has read both registrations and built its version checks.
                            metadata.dropTable(tablePath, false);
                            createIndexedTable();
                            createPartition(metadata, "old");
                            return curator.transaction();
                        })
                .when(delayed)
                .transaction();
        MetadataManager dropper =
                manager(
                        new ZooKeeperClient(
                                new CuratorFrameworkWithUnhandledErrorListener(
                                        delayed, (message, failure) -> {}),
                                config));

        assertThatThrownBy(
                        () ->
                                dropper.dropPartition(
                                        tablePath,
                                        originalTableId,
                                        fromPartitionName(Collections.singletonList("p"), "old"),
                                        false))
                .hasRootCauseInstanceOf(KeeperException.NoNodeException.class);
        assertThat(client.getTable(tablePath).get().tableId)
                .isEqualTo(tableId)
                .isNotEqualTo(originalTableId);
        assertThat(client.getPartition(tablePath, "old").get().getPartitionId())
                .isGreaterThan(oldPartitionId);
        assertThat(client.getPartitionTombstone(tablePath, tableId))
                .isEqualTo(PartitionTombstone.EMPTY);
        assertThat(client.getPartitionTombstoneWithVersion(tablePath, tableId).f1).contains(0);
    }

    private MetadataManager manager(ZooKeeperClient zkClient) {
        return new MetadataManager(
                zkClient, config, new LakeCatalogDynamicLoader(config, null, true));
    }

    private void createPartition(MetadataManager manager, String name) {
        manager.createPartition(
                tablePath,
                schema,
                client.getDefaultRemoteDataDir(),
                assignment,
                fromPartitionName(Collections.singletonList("p"), name),
                false,
                1);
    }
}
