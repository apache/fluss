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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;
import org.apache.fluss.server.entity.RemoteLogManifestCommitResult;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.VersionedRemoteLogManifestHandle;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests transport-unknown reconciliation for {@link RemoteLogManifestCommitter}. */
class RemoteLogManifestCommitterTest {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zooKeeperClient;

    @BeforeAll
    static void beforeAll() {
        zooKeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @Test
    void testReconcileLostAbsentCreateResponse() throws Exception {
        TableBucket tableBucket = new TableBucket(1L, 0);
        CommitRemoteLogManifestData data = absentData(tableBucket, "m1");
        CoordinatorGateway gateway = mock(CoordinatorGateway.class);
        when(gateway.commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class)))
                .thenAnswer(
                        ignored -> {
                            zooKeeperClient.createRemoteLogManifestHandleIfAbsent(
                                    tableBucket, handleFor(data));
                            return failedFuture();
                        });

        RemoteLogManifestCommitResult result =
                new RemoteLogManifestCommitter(gateway, zooKeeperClient).commit(data);

        assertThat(result).isEqualTo(RemoteLogManifestCommitResult.COMMITTED);
        verify(gateway).commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class));
    }

    @Test
    void testReconcileAbsentCreateConflict() throws Exception {
        TableBucket tableBucket = new TableBucket(2L, 0);
        CommitRemoteLogManifestData data = absentData(tableBucket, "ours");
        zooKeeperClient.createRemoteLogManifestHandleIfAbsent(
                tableBucket,
                RemoteLogManifestHandle.v2(new FsPath("file:///remote/other"), 1L, 0L, 10L));
        CoordinatorGateway gateway = mock(CoordinatorGateway.class);
        when(gateway.commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class)))
                .thenReturn(failedFuture());

        assertThat(new RemoteLogManifestCommitter(gateway, zooKeeperClient).commit(data))
                .isEqualTo(RemoteLogManifestCommitResult.CONFLICT);
    }

    @Test
    void testRetryWhenHandleRemainsAbsent() throws Exception {
        TableBucket tableBucket = new TableBucket(3L, 0);
        CommitRemoteLogManifestData data = absentData(tableBucket, "m1");
        CoordinatorGateway gateway = mock(CoordinatorGateway.class);
        when(gateway.commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class)))
                .thenReturn(
                        failedFuture(),
                        CompletableFuture.completedFuture(
                                new CommitRemoteLogManifestResponse()
                                        .setCommitSuccess(true)
                                        .setCommitResult(
                                                RemoteLogManifestCommitResult.COMMITTED.code())));

        assertThat(new RemoteLogManifestCommitter(gateway, zooKeeperClient).commit(data))
                .isEqualTo(RemoteLogManifestCommitResult.COMMITTED);
        verify(gateway, times(2))
                .commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class));
    }

    @Test
    void testLegacyBooleanResponseIsNotAcceptedAsV2Commit() throws Exception {
        TableBucket tableBucket = new TableBucket(8L, 0);
        CommitRemoteLogManifestData data = absentData(tableBucket, "m1");
        CoordinatorGateway gateway = mock(CoordinatorGateway.class);
        when(gateway.commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new CommitRemoteLogManifestResponse().setCommitSuccess(true)));

        assertThatThrownBy(
                        () -> new RemoteLogManifestCommitter(gateway, zooKeeperClient).commit(data))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("remains unknown");
        verify(gateway, times(10))
                .commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class));
    }

    @Test
    void testReconcileLostPresentCasResponse() throws Exception {
        TableBucket tableBucket = new TableBucket(4L, 0);
        FsPath basePath = new FsPath("file:///remote/v1");
        zooKeeperClient.createRemoteLogManifestHandleIfAbsent(
                tableBucket, new RemoteLogManifestHandle(basePath, 10L));
        VersionedRemoteLogManifestHandle base =
                zooKeeperClient.getVersionedRemoteLogManifestHandle(tableBucket).get();
        CommitRemoteLogManifestData data =
                CommitRemoteLogManifestData.v2Present(
                        tableBucket,
                        new FsPath("file:///remote/v2"),
                        0L,
                        20L,
                        1L,
                        basePath,
                        0L,
                        base.zkVersion(),
                        1,
                        1);
        CoordinatorGateway gateway = mock(CoordinatorGateway.class);
        when(gateway.commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class)))
                .thenAnswer(
                        ignored -> {
                            zooKeeperClient.compareAndSetRemoteLogManifestHandle(
                                    tableBucket, basePath, 0L, base.zkVersion(), handleFor(data));
                            return failedFuture();
                        });

        assertThat(new RemoteLogManifestCommitter(gateway, zooKeeperClient).commit(data))
                .isEqualTo(RemoteLogManifestCommitResult.COMMITTED);
    }

    @Test
    void testRetryWhenPresentBaseRemainsAuthoritative() throws Exception {
        TableBucket tableBucket = new TableBucket(5L, 0);
        FsPath basePath = new FsPath("file:///remote/v1");
        zooKeeperClient.createRemoteLogManifestHandleIfAbsent(
                tableBucket, new RemoteLogManifestHandle(basePath, 10L));
        VersionedRemoteLogManifestHandle base =
                zooKeeperClient.getVersionedRemoteLogManifestHandle(tableBucket).get();
        CommitRemoteLogManifestData data = presentData(tableBucket, base, "v2");
        CoordinatorGateway gateway = mock(CoordinatorGateway.class);
        when(gateway.commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class)))
                .thenReturn(
                        failedFuture(),
                        CompletableFuture.completedFuture(
                                new CommitRemoteLogManifestResponse()
                                        .setCommitSuccess(true)
                                        .setCommitResult(
                                                RemoteLogManifestCommitResult.COMMITTED.code())));

        assertThat(new RemoteLogManifestCommitter(gateway, zooKeeperClient).commit(data))
                .isEqualTo(RemoteLogManifestCommitResult.COMMITTED);
        verify(gateway, times(2))
                .commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class));
    }

    @Test
    void testReconcilePresentConflict() throws Exception {
        TableBucket tableBucket = new TableBucket(6L, 0);
        FsPath basePath = new FsPath("file:///remote/v1");
        zooKeeperClient.createRemoteLogManifestHandleIfAbsent(
                tableBucket, new RemoteLogManifestHandle(basePath, 10L));
        VersionedRemoteLogManifestHandle base =
                zooKeeperClient.getVersionedRemoteLogManifestHandle(tableBucket).get();
        CommitRemoteLogManifestData data = presentData(tableBucket, base, "ours");
        zooKeeperClient.compareAndSetRemoteLogManifestHandle(
                tableBucket,
                basePath,
                0L,
                base.zkVersion(),
                RemoteLogManifestHandle.v2(new FsPath("file:///remote/other"), 1L, 0L, 20L));
        CoordinatorGateway gateway = mock(CoordinatorGateway.class);
        when(gateway.commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class)))
                .thenReturn(failedFuture());

        assertThat(new RemoteLogManifestCommitter(gateway, zooKeeperClient).commit(data))
                .isEqualTo(RemoteLogManifestCommitResult.CONFLICT);
    }

    @Test
    void testCommitAndApplyFullManifestReplacement() throws Exception {
        TableBucket tableBucket = new TableBucket(7L, 0);
        CommitRemoteLogManifestData data = absentData(tableBucket, "m1");
        RemoteLogSegment segment =
                RemoteLogSegment.Builder.builder()
                        .remoteLogSegmentId(UUID.randomUUID())
                        .remoteLogStartOffset(0L)
                        .remoteLogEndOffset(10L)
                        .maxTimestamp(1L)
                        .segmentSizeInBytes(100)
                        .tableBucket(tableBucket)
                        .physicalTablePath(DATA1_PHYSICAL_TABLE_PATH)
                        .build();
        RemoteLogManifest manifest =
                RemoteLogManifest.createV2(
                        1L,
                        DATA1_PHYSICAL_TABLE_PATH,
                        tableBucket,
                        Collections.singletonList(segment),
                        0L,
                        Collections.emptyList());
        RemoteLogTablet tablet = new RemoteLogTablet(DATA1_PHYSICAL_TABLE_PATH, tableBucket, -1L);
        CoordinatorGateway gateway = mock(CoordinatorGateway.class);
        when(gateway.commitRemoteLogManifest(any(CommitRemoteLogManifestRequest.class)))
                .thenAnswer(
                        ignored -> {
                            zooKeeperClient.createRemoteLogManifestHandleIfAbsent(
                                    tableBucket, handleFor(data));
                            return CompletableFuture.completedFuture(
                                    new CommitRemoteLogManifestResponse()
                                            .setCommitSuccess(true)
                                            .setCommitResult(
                                                    RemoteLogManifestCommitResult.COMMITTED
                                                            .code()));
                        });

        RemoteLogManifestCommitResult result =
                new RemoteLogManifestCommitter(gateway, zooKeeperClient)
                        .commitAndApply(data, manifest, tablet);

        assertThat(result).isEqualTo(RemoteLogManifestCommitResult.COMMITTED);
        assertThat(tablet.currentManifest()).isSameAs(manifest);
        assertThat(tablet.currentHandle()).isNotNull();
        assertThat(tablet.currentHandle().handle()).isEqualTo(handleFor(data));
    }

    private static CommitRemoteLogManifestData absentData(
            TableBucket tableBucket, String manifestName) {
        return CommitRemoteLogManifestData.v2Absent(
                tableBucket, new FsPath("file:///remote/" + manifestName), 0L, 10L, 1L, 1, 1);
    }

    private static CommitRemoteLogManifestData presentData(
            TableBucket tableBucket, VersionedRemoteLogManifestHandle base, String manifestName) {
        return CommitRemoteLogManifestData.v2Present(
                tableBucket,
                new FsPath("file:///remote/" + manifestName),
                0L,
                20L,
                1L,
                base.handle().getRemoteLogManifestPath(),
                base.handle().getManifestGeneration().orElse(0L),
                base.zkVersion(),
                1,
                1);
    }

    private static RemoteLogManifestHandle handleFor(CommitRemoteLogManifestData data) {
        return RemoteLogManifestHandle.v2(
                data.getRemoteLogManifestPath(),
                data.getNewManifestGeneration(),
                data.getRemoteLogStartOffset(),
                data.getRemoteLogEndOffset());
    }

    private static CompletableFuture<CommitRemoteLogManifestResponse> failedFuture() {
        CompletableFuture<CommitRemoteLogManifestResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("response lost"));
        return future;
    }
}
