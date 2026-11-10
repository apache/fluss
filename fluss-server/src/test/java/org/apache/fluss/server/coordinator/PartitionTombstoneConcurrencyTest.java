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
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.PartitionTombstoneBinarySerde;
import org.apache.fluss.server.zk.data.ZkData.PartitionTombstoneZNode;
import org.apache.fluss.server.zk.data.ZkData.PartitionZNode;
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

/** Tests that partition deletion cannot tombstone a concurrently published live partition. */
class PartitionTombstoneConcurrencyTest {

    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private final TablePath tablePath = TablePath.of("db", "partitioned");
    private ZooKeeperClient client;
    private Configuration config;
    private MetadataManager metadata;
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
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("p", DataTypes.STRING())
                        .primaryKey("id", "p")
                        .build();
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
        oldPartitionId = client.getPartitionIdAndIncrement();
        // Existing tables can have partitions registered before tombstone metadata is present.
        client.getCuratorClient()
                .create()
                .creatingParentsIfNeeded()
                .forPath(
                        PartitionZNode.path(tablePath, "old"),
                        PartitionZNode.encode(
                                new PartitionRegistration(
                                        tableId,
                                        oldPartitionId,
                                        client.getDefaultRemoteDataDir(),
                                        1)));
    }

    @AfterEach
    void cleanUp() {
        ZK.getCustomExtension().cleanupRoot();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void creationRetriesWhenDropOvertakesPublication(boolean tombstoneExists) throws Exception {
        if (tombstoneExists) {
            client.getCuratorClient()
                    .create()
                    .forPath(
                            PartitionTombstoneZNode.path(tablePath),
                            PartitionTombstoneBinarySerde.serialize(PartitionTombstone.EMPTY));
        }
        AtomicBoolean interleave = new AtomicBoolean(true);
        ZooKeeperClient delayedCreator =
                new ZooKeeperClient(client.getCuratorFrameworkWrapper(), config) {
                    @Override
                    public Tuple2<PartitionTombstone, Optional<Integer>>
                            getPartitionTombstoneWithVersion(TablePath path) throws Exception {
                        Tuple2<PartitionTombstone, Optional<Integer>> observed =
                                super.getPartitionTombstoneWithVersion(path);
                        if (interleave.getAndSet(false)) {
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
        PartitionTombstone tombstone = client.getPartitionTombstone(tablePath);
        assertThat(tombstone.isTombstoned(oldPartitionId)).isTrue();
        assertThat(tombstone.isTombstoned(newerId)).isFalse();
        assertThat(tombstone.isTombstoned(delayedId)).isFalse();
        assertThat(delayedId).isGreaterThan(newerId);
    }

    @Test
    void registrationInvalidatesConcurrentDropSnapshot() throws Exception {
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
                            int expectedPartitionsVersion,
                            PartitionTombstone tombstone,
                            Optional<Integer> expectedTombstoneVersion)
                            throws Exception {
                        if (interleave.getAndSet(false)) {
                            client.registerPartitionAssignmentAndMetadata(
                                    pendingId,
                                    "pending",
                                    assignment,
                                    client.getDefaultRemoteDataDir(),
                                    tablePath,
                                    tableId,
                                    1);
                        }
                        super.deletePartitionAndSetTombstone(
                                path,
                                partitionName,
                                expectedTableId,
                                expectedPartitionId,
                                expectedPartitionsVersion,
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
        PartitionTombstone tombstone = client.getPartitionTombstone(tablePath);
        assertThat(tombstone.isTombstoned(oldPartitionId)).isTrue();
        assertThat(tombstone.isTombstoned(pendingId)).isFalse();
    }

    private MetadataManager manager(ZooKeeperClient zkClient) {
        return new MetadataManager(
                zkClient, config, new LakeCatalogDynamicLoader(config, null, true));
    }

    private void createPartition(MetadataManager manager, String name) {
        manager.createPartition(
                tablePath,
                tableId,
                client.getDefaultRemoteDataDir(),
                assignment,
                fromPartitionName(Collections.singletonList("p"), name),
                false,
                1);
    }
}
