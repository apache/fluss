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

import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;
import org.apache.fluss.server.entity.RemoteLogManifestCommitResult;
import org.apache.fluss.server.entity.RemoteLogManifestExpectedHandleState;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.VersionedRemoteLogManifestHandle;

import java.util.Optional;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeCommitRemoteLogManifestRequest;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Publishes Manifest V2 handles and reconciles transport-unknown commit outcomes. */
public final class RemoteLogManifestCommitter {
    private static final int MAX_COMMIT_ATTEMPTS = 10;

    private final CoordinatorGateway coordinatorGateway;
    private final ZooKeeperClient zooKeeperClient;

    public RemoteLogManifestCommitter(
            CoordinatorGateway coordinatorGateway, ZooKeeperClient zooKeeperClient) {
        this.coordinatorGateway = coordinatorGateway;
        this.zooKeeperClient = zooKeeperClient;
    }

    public RemoteLogManifestCommitResult commit(CommitRemoteLogManifestData data) throws Exception {
        checkArgument(data.isV2CasCommit(), "CAS committer only accepts Manifest V2 commits");
        Exception lastTransportError = null;
        for (int attempt = 0; attempt < MAX_COMMIT_ATTEMPTS; attempt++) {
            try {
                CommitRemoteLogManifestResponse response =
                        coordinatorGateway
                                .commitRemoteLogManifest(makeCommitRemoteLogManifestRequest(data))
                                .get();
                if (!response.hasCommitResult()) {
                    throw new IllegalStateException(
                            "Coordinator did not return a V2 manifest commit result");
                }
                return RemoteLogManifestCommitResult.fromCode(response.getCommitResult());
            } catch (Exception transportError) {
                lastTransportError = transportError;
                ReconciliationResult reconciliationResult = reconcile(data);
                if (reconciliationResult == ReconciliationResult.COMMITTED) {
                    return RemoteLogManifestCommitResult.COMMITTED;
                }
                if (reconciliationResult == ReconciliationResult.CONFLICT) {
                    return RemoteLogManifestCommitResult.CONFLICT;
                }
                // The authoritative state is still the expected base (or remains absent), so
                // retry exactly the same idempotent CAS/create request.
            }
        }
        throw new IllegalStateException(
                "Remote log manifest commit remains unknown after reconciliation retries",
                lastTransportError);
    }

    /** Commits a V2 snapshot and atomically installs it only after its handle is authoritative. */
    public RemoteLogManifestCommitResult commitAndApply(
            CommitRemoteLogManifestData data,
            RemoteLogManifest manifest,
            RemoteLogTablet remoteLogTablet)
            throws Exception {
        validateManifest(data, manifest);
        RemoteLogManifestCommitResult result = commit(data);
        if (result != RemoteLogManifestCommitResult.COMMITTED) {
            return result;
        }

        Optional<VersionedRemoteLogManifestHandle> authoritativeOpt =
                zooKeeperClient.getVersionedRemoteLogManifestHandle(data.getTableBucket());
        if (!authoritativeOpt.isPresent()
                || !isPublishedResult(authoritativeOpt.get().handle(), data)) {
            throw new IllegalStateException(
                    "Committed remote log manifest is no longer authoritative before local apply");
        }
        remoteLogTablet.replaceManifest(manifest, authoritativeOpt.get());
        return RemoteLogManifestCommitResult.COMMITTED;
    }

    private static void validateManifest(
            CommitRemoteLogManifestData data, RemoteLogManifest manifest) {
        checkArgument(
                manifest.getVersion() == RemoteLogManifest.VERSION_2,
                "CAS commit requires a Manifest V2 snapshot");
        checkArgument(
                manifest.getTableBucket().equals(data.getTableBucket()),
                "Manifest table bucket does not match commit request");
        checkArgument(
                manifest.getGeneration() == data.getNewManifestGeneration(),
                "Manifest generation does not match commit request");
        checkArgument(
                manifest.getRemoteLogStartOffset() == data.getRemoteLogStartOffset(),
                "Manifest start offset does not match commit request");
        checkArgument(
                manifest.getRemoteLogEndOffset() == data.getRemoteLogEndOffset(),
                "Manifest end offset does not match commit request");
    }

    private ReconciliationResult reconcile(CommitRemoteLogManifestData data) throws Exception {
        Optional<VersionedRemoteLogManifestHandle> currentOpt =
                zooKeeperClient.getVersionedRemoteLogManifestHandle(data.getTableBucket());
        if (currentOpt.isPresent() && isPublishedResult(currentOpt.get().handle(), data)) {
            return ReconciliationResult.COMMITTED;
        }

        if (data.getExpectedHandleState() == RemoteLogManifestExpectedHandleState.ABSENT) {
            return currentOpt.isPresent()
                    ? ReconciliationResult.CONFLICT
                    : ReconciliationResult.RETRY;
        }
        if (!currentOpt.isPresent()) {
            return ReconciliationResult.CONFLICT;
        }

        VersionedRemoteLogManifestHandle current = currentOpt.get();
        long currentGeneration = current.handle().getManifestGeneration().orElse(0L);
        if (current.zkVersion() == data.getExpectedZkVersion()
                && current.handle()
                        .getRemoteLogManifestPath()
                        .equals(data.getExpectedManifestPath())
                && currentGeneration == data.getExpectedManifestGeneration()) {
            return ReconciliationResult.RETRY;
        }
        return ReconciliationResult.CONFLICT;
    }

    private static boolean isPublishedResult(
            RemoteLogManifestHandle current, CommitRemoteLogManifestData data) {
        return current.getVersion() == RemoteLogManifestHandle.VERSION_2
                && current.getRemoteLogManifestPath().equals(data.getRemoteLogManifestPath())
                && current.getManifestGeneration().isPresent()
                && current.getManifestGeneration().getAsLong() == data.getNewManifestGeneration()
                && current.getRemoteLogStartOffset().isPresent()
                && current.getRemoteLogStartOffset().getAsLong() == data.getRemoteLogStartOffset()
                && current.getRemoteLogEndOffset() == data.getRemoteLogEndOffset();
    }

    private enum ReconciliationResult {
        COMMITTED,
        CONFLICT,
        RETRY
    }
}
