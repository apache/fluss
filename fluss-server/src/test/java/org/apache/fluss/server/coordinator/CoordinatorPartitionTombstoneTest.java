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

import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.Clock;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess;
import static org.apache.fluss.server.coordinator.statemachine.BucketState.OnlineBucket;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getUpdateMetadataRequestData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/** Coordinator lifecycle and metadata delivery when a partition tombstone read fails. */
class CoordinatorPartitionTombstoneTest extends CoordinatorEventProcessorTestBase {
    private final TablePath indexed = TablePath.of(defaultDatabase, "indexed");
    private final AtomicBoolean failRead = new AtomicBoolean();
    private final Map<Integer, TabletServerMetadataCache> caches = new ConcurrentHashMap<>();

    @Override
    protected CoordinatorEventProcessor buildCoordinatorEventProcessor(Clock clock) {
        ZooKeeperClient client = mock(ZooKeeperClient.class, delegatesTo(zookeeperClient));
        try {
            doAnswer(
                            call -> {
                                if (failRead.getAndSet(false)) {
                                    throw new IOException("Injected tombstone read failure");
                                }
                                return zookeeperClient.getPartitionTombstone(
                                        indexed, call.getArgument(1));
                            })
                    .when(client)
                    .getPartitionTombstone(eq(indexed), anyLong());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return buildCoordinatorEventProcessor(clock, client);
    }

    @Override
    protected TestCoordinatorChannelManager createCoordinatorChannelManager() {
        return new TestCoordinatorChannelManager() {
            @Override
            public void sendUpdateMetadataRequest(
                    int server,
                    UpdateMetadataRequest request,
                    BiConsumer<UpdateMetadataResponse, ? super Throwable> callback) {
                caches.computeIfAbsent(
                                server, ignored -> new TabletServerMetadataCache(metadataManager))
                        .updateClusterMetadata(getUpdateMetadataRequestData(request));
                callback.accept(new UpdateMetadataResponse(), null);
            }
        };
    }

    @Test
    void lifecycleAndTombstoneDeliveryRecoverAfterReadFailure() throws Exception {
        makeSendLeaderAndStopRequestAlwaysSuccess(
                testCoordinatorChannelManager,
                new HashSet<>(Arrays.asList(0, 1, 2, 3)),
                Collections.emptySet());
        Schema schema =
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
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("p")
                        .distributedBy(1, "id")
                        .build()
                        .withReplicationFactor(1);
        failRead.set(true);
        long tableId = metadataManager.createTable(indexed, remoteDataDir, descriptor, null, false);
        retryVerifyContext(
                ctx -> {
                    assertThat(caches.get(0).getTableId(indexed)).hasValue(tableId);
                    assertThat(caches.get(0).getPartitionTombstoneIfInitialized(tableId))
                            .contains(PartitionTombstone.EMPTY);
                });

        ResolvedPartitionSpec spec =
                ResolvedPartitionSpec.fromPartitionName(Collections.singletonList("p"), "one");
        metadataManager.createPartition(
                indexed,
                schema,
                remoteDataDir,
                new PartitionAssignment(
                        tableId, Collections.singletonMap(0, BucketAssignment.of(0))),
                spec,
                false,
                1);
        long partitionId = zookeeperClient.getPartition(indexed, "one").get().getPartitionId();
        retryVerifyContext(ctx -> assertThat(ctx.containsPartitionId(partitionId)).isTrue());
        failRead.set(true);
        metadataManager.dropPartition(indexed, tableId, spec, false);
        PartitionTombstone expected = zookeeperClient.getPartitionTombstone(indexed, tableId);
        assertThat(expected.isTombstoned(partitionId)).isTrue();
        retryVerifyContext(
                ctx -> {
                    assertThat(ctx.containsPartitionId(partitionId)).isFalse();
                    assertThat(caches.get(0).getPartitionTombstoneIfInitialized(tableId))
                            .contains(expected);
                });

        TableDescriptor ordinary =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build()
                        .withReplicationFactor(1);
        long ordinaryId =
                metadataManager.createTable(
                        TablePath.of(defaultDatabase, "ordinary"),
                        remoteDataDir,
                        ordinary,
                        new TableAssignment(Collections.singletonMap(0, BucketAssignment.of(3))),
                        false);
        TableBucket bucket = new TableBucket(ordinaryId, 0);
        retryVerifyContext(ctx -> assertThat(ctx.containsTableId(ordinaryId)).isTrue());
        failRead.set(true);
        registerTabletServer(3);
        retryVerifyContext(
                ctx -> {
                    assertThat(ctx.getBucketState(bucket)).isEqualTo(OnlineBucket);
                    assertThat(caches.get(3).getPartitionTombstoneIfInitialized(tableId))
                            .contains(expected);
                });
    }
}
