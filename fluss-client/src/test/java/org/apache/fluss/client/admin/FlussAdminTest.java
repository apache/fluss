/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.admin;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.utils.TestingRpcClient;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.BucketLogOffset;
import org.apache.fluss.metadata.MvccSnapshotRef;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.DescribeClusterResponse;
import org.apache.fluss.rpc.messages.ErrorResponse;
import org.apache.fluss.rpc.messages.GetMvccSnapshotOffsetsResponse;
import org.apache.fluss.rpc.messages.ListMvccReadBoundsResponse;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.RegisterMvccSnapshotOffsetsResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for cluster discovery and inspection through {@link FlussAdmin}. */
class FlussAdminTest {

    private static final ServerNode OLD_COORDINATOR =
            new ServerNode(0, "old-coordinator", 9123, ServerType.COORDINATOR);

    @ParameterizedTest
    @CsvSource({
        "NETWORK_EXCEPTION, true, new-coordinator, 9123",
        "NETWORK_EXCEPTION, false, new-coordinator, 9123",
        "NOT_COORDINATOR_LEADER_EXCEPTION, true, new-coordinator, 9123",
        "NOT_COORDINATOR_LEADER_EXCEPTION, false, new-coordinator, 9123",
        "NETWORK_EXCEPTION, true, old-coordinator, 9124",
        "NETWORK_EXCEPTION, false, old-coordinator, 9124",
        "NOT_COORDINATOR_LEADER_EXCEPTION, true, old-coordinator, 9124",
        "NOT_COORDINATOR_LEADER_EXCEPTION, false, old-coordinator, 9124"
    })
    void testDescribeClusterRefreshesAndRetriesOnce(
            Errors error, boolean retrySucceeds, String host, int port) throws Exception {
        ServerNode newCoordinator =
                new ServerNode(OLD_COORDINATOR.id(), host, port, ServerType.COORDINATOR);
        RuntimeException failure = error.exception("coordinator unavailable");
        AtomicInteger refreshes = new AtomicInteger();
        List<ServerNode> contactedNodes = new ArrayList<>();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.DESCRIBE_CLUSTER);
                            contactedNodes.add(node);
                            if (node.equals(OLD_COORDINATOR) || !retrySucceeds) {
                                return failed(failure);
                            }
                            DescribeClusterResponse response = new DescribeClusterResponse();
                            response.setCoordinatorServer()
                                    .setNodeId(node.id())
                                    .setHost(node.host())
                                    .setPort(node.port());
                            return CompletableFuture.completedFuture(response);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getCoordinatorServer() {
                        return refreshes.get() == 0 ? OLD_COORDINATOR : newCoordinator;
                    }

                    @Override
                    public void refreshClusterUntilAvailable() {
                        refreshes.incrementAndGet();
                    }
                };
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            if (retrySucceeds) {
                assertThat(admin.describeCluster().get(10, TimeUnit.SECONDS).getCoordinatorServer())
                        .contains(newCoordinator);
            } else {
                assertThatThrownBy(() -> admin.describeCluster().get(10, TimeUnit.SECONDS))
                        .rootCause()
                        .isSameAs(failure);
            }
            assertThat(contactedNodes).containsExactly(OLD_COORDINATOR, newCoordinator);
            assertThat(refreshes).hasValue(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDescribeClusterDoesNotHideNonRetriableFailures(boolean unsupported) throws Exception {
        RuntimeException error =
                unsupported
                        ? new UnsupportedVersionException("DescribeCluster is unavailable")
                        : new AuthorizationException("cluster DESCRIBE denied");
        AtomicInteger requests = new AtomicInteger();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.DESCRIBE_CLUSTER);
                            requests.incrementAndGet();
                            return failed(error);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getCoordinatorServer() {
                        return OLD_COORDINATOR;
                    }

                    @Override
                    public void refreshClusterUntilAvailable() {
                        throw new AssertionError(
                                "Non-retriable failures must not refresh metadata");
                    }
                };
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            assertThatThrownBy(() -> admin.describeCluster().get(10, TimeUnit.SECONDS))
                    .rootCause()
                    .isSameAs(error);
            assertThat(requests).hasValue(1);
        }
    }

    @Test
    void testDiscoveryUsesMetadataWithoutDescribeCluster() throws Exception {
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.GET_METADATA);
                            MetadataResponse response = new MetadataResponse();
                            response.setCoordinatorServer()
                                    .setNodeId(0)
                                    .setHost("coordinator")
                                    .setPort(9123);
                            response.addTabletServer().setNodeId(1).setHost("tablet").setPort(9124);
                            return CompletableFuture.completedFuture(response);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getRandomTabletServer() {
                        return new ServerNode(1, "tablet", 9124, ServerType.TABLET_SERVER);
                    }
                };
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            assertThat(admin.getServerNodes().get(10, TimeUnit.SECONDS))
                    .containsExactly(
                            new ServerNode(0, "coordinator", 9123, ServerType.COORDINATOR),
                            new ServerNode(1, "tablet", 9124, ServerType.TABLET_SERVER));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMalformedBoundsResponseIsIsolatedToItsDestination(boolean omitEntry) throws Exception {
        TableBucket good = new TableBucket(1L, 0);
        TableBucket bad = new TableBucket(1L, 1);
        ServerNode first = new ServerNode(1, "first", 9124, ServerType.TABLET_SERVER);
        ServerNode second = new ServerNode(2, "second", 9125, ServerType.TABLET_SERVER);
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.LIST_MVCC_READ_BOUNDS);
                            ListMvccReadBoundsResponse reply = new ListMvccReadBoundsResponse();
                            if (node.equals(OLD_COORDINATOR)) {
                                reply.addResult()
                                        .setLeaderId(first.id())
                                        .setBucket()
                                        .setTableId(1L)
                                        .setBucketId(0);
                                reply.addResult()
                                        .setLeaderId(second.id())
                                        .setBucket()
                                        .setTableId(1L)
                                        .setBucketId(1);
                            } else if (node.equals(first)) {
                                reply.addResult()
                                        .setEarliestOffset(0)
                                        .setLatestOffset(0)
                                        .setBucket()
                                        .setTableId(1L)
                                        .setBucketId(0);
                            } else {
                                assertThat(node).isEqualTo(second);
                                if (!omitEntry) {
                                    reply.addResult()
                                            .setEarliestOffset(0)
                                            .setBucket()
                                            .setTableId(1L)
                                            .setBucketId(1);
                                }
                            }
                            return CompletableFuture.completedFuture(reply);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getCoordinatorServer() {
                        return OLD_COORDINATOR;
                    }

                    @Override
                    public TabletServerGateway newTabletServerClientForNode(int id) {
                        ServerNode node = id == first.id() ? first : second;
                        return GatewayClientProxy.createGatewayProxy(
                                () -> node, client, TabletServerGateway.class);
                    }
                };
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            ListMvccReadBoundsResult result = admin.listMvccReadBounds(Arrays.asList(good, bad));
            assertThat(result.bucketResult(good).get(10, TimeUnit.SECONDS).getLatestOffset())
                    .isZero();
            assertThatThrownBy(() -> result.bucketResult(bad).get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(FlussRuntimeException.class);
        }
    }

    @Test
    void testSnapshotRegistrationPreservesAttributedTimeoutButLosesWholeReply() throws Exception {
        TableBucket first = new TableBucket(1L, 0);
        TableBucket second = new TableBucket(1L, 1);
        AtomicBoolean loseReply = new AtomicBoolean();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.REGISTER_MVCC_SNAPSHOT_OFFSETS);
                            if (loseReply.get()) {
                                return failed(
                                        Errors.NETWORK_EXCEPTION.exception("whole reply lost"));
                            }
                            RegisterMvccSnapshotOffsetsResponse reply =
                                    new RegisterMvccSnapshotOffsetsResponse();
                            reply.addResult().setBucket().setTableId(1L).setBucketId(0);
                            reply.addResult()
                                    .setError(
                                            new ErrorResponse()
                                                    .setErrorCode(Errors.REQUEST_TIME_OUT.code()))
                                    .setBucket()
                                    .setTableId(1L)
                                    .setBucketId(1);
                            return CompletableFuture.completedFuture(reply);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getCoordinatorServer() {
                        return OLD_COORDINATOR;
                    }

                    @Override
                    public void refreshClusterUntilAvailable() {}
                };
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            MvccSnapshotRef snapshot = MvccSnapshotRef.of(1L, "baseline");
            List<BucketLogOffset> offsets =
                    Arrays.asList(BucketLogOffset.of(first, 0), BucketLogOffset.of(second, 1));
            RegisterMvccSnapshotOffsetsResult result =
                    admin.registerMvccSnapshotOffsets(snapshot, offsets).get(10, TimeUnit.SECONDS);
            assertThat(result.getRegisteredBuckets()).containsExactly(first);
            assertThat(result.getErrors().get(second)).isInstanceOf(TimeoutException.class);
            loseReply.set(true);
            assertThatThrownBy(
                            () ->
                                    admin.registerMvccSnapshotOffsets(snapshot, offsets)
                                            .get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(NetworkException.class);
        }
    }

    @Test
    void testSnapshotResolutionDistinguishesZeroMissingAndOmittedResult() throws Exception {
        TableBucket first = new TableBucket(1L, 0);
        TableBucket second = new TableBucket(1L, 1);
        AtomicBoolean omit = new AtomicBoolean();
        TestingRpcClient client =
                new TestingRpcClient(
                        (node, api) -> {
                            assertThat(api).isEqualTo(ApiKeys.GET_MVCC_SNAPSHOT_OFFSETS);
                            GetMvccSnapshotOffsetsResponse reply =
                                    new GetMvccSnapshotOffsetsResponse();
                            reply.addResult()
                                    .setNextOffset(0)
                                    .setBucket()
                                    .setTableId(1L)
                                    .setBucketId(0);
                            if (!omit.get()) {
                                reply.addResult().setBucket().setTableId(1L).setBucketId(1);
                            }
                            return CompletableFuture.completedFuture(reply);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getCoordinatorServer() {
                        return OLD_COORDINATOR;
                    }
                };
        try (FlussAdmin admin = new FlussAdmin(client, updater)) {
            MvccSnapshotRef snapshot = MvccSnapshotRef.of(1L, "baseline");
            List<TableBucket> buckets = Arrays.asList(first, second);
            GetMvccSnapshotOffsetsResult result =
                    admin.getMvccSnapshotOffsets(snapshot, buckets).get(10, TimeUnit.SECONDS);
            assertThat(result.getOffsets()).containsEntry(first, BucketLogOffset.of(first, 0));
            assertThat(result.getMissingBuckets()).containsExactly(second);
            omit.set(true);
            assertThatThrownBy(
                            () ->
                                    admin.getMvccSnapshotOffsets(snapshot, buckets)
                                            .get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(FlussRuntimeException.class);
        }
    }

    private static CompletableFuture<ApiMessage> failed(Throwable error) {
        CompletableFuture<ApiMessage> result = new CompletableFuture<>();
        result.completeExceptionally(error);
        return result;
    }
}
