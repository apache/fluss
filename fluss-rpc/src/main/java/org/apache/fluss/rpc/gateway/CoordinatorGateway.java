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

package org.apache.fluss.rpc.gateway;

import org.apache.fluss.rpc.RpcGateway;
import org.apache.fluss.rpc.messages.AdjustIsrRequest;
import org.apache.fluss.rpc.messages.AdjustIsrResponse;
import org.apache.fluss.rpc.messages.CommitKvSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitKvSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import org.apache.fluss.rpc.messages.ControlledShutdownRequest;
import org.apache.fluss.rpc.messages.ControlledShutdownResponse;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.PrepareLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PrepareLakeTableSnapshotResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.RPC;

import java.util.concurrent.CompletableFuture;

/** The entry point of RPC gateway interface for coordinator server. */
public interface CoordinatorGateway extends RpcGateway, AdminGateway {

    /**
     * AdjustIsr request to adjust (expend or shrink) the ISR set for request table bucket.
     *
     * @param request the adjust isr request
     * @return adjust isr response
     */
    @RPC(api = ApiKeys.ADJUST_ISR)
    CompletableFuture<AdjustIsrResponse> adjustIsr(AdjustIsrRequest request);

    /**
     * Add a completed snapshot for a bucket.
     *
     * @param request the request for adding a completed snapshot
     * @return add snapshot response
     */
    @RPC(api = ApiKeys.COMMIT_KV_SNAPSHOT)
    CompletableFuture<CommitKvSnapshotResponse> commitKvSnapshot(CommitKvSnapshotRequest request);

    /**
     * Commit remote log manifest.
     *
     * @param request the request for committing remote log manifest.
     * @return commit remote log manifest response.
     */
    @RPC(api = ApiKeys.COMMIT_REMOTE_LOG_MANIFEST)
    CompletableFuture<CommitRemoteLogManifestResponse> commitRemoteLogManifest(
            CommitRemoteLogManifestRequest request);

    /**
     * Prepares lake table snapshots by merging them with existing snapshots and storing them to the
     * remote file system.
     *
     * <p>This method is called during the two-phase commit process for lake table snapshots. It
     * performs the following operations for each table in the request:
     *
     * <ul>
     *   <li>Merges the new snapshot with the previous latest snapshot (if exists) to ensure
     *       completeness
     *   <li>Stores the merged snapshot to the remote file system. The stored file contains the log
     *       end offset information for each bucket in the table
     *   <li>Returns the file path where the snapshot is stored
     *   <li>Call {@link #commitLakeTableSnapshot(CommitLakeTableSnapshotRequest)} with the offset
     *       file path to finalize the snapshot commit to ZooKeeper in the second phase.
     * </ul>
     *
     * @param request the request containing lake table snapshot information for one or more tables
     * @return a future that completes with a response containing the file paths where snapshots
     *     (containing bucket log end offset information) are stored, or error information for
     *     tables that failed to process
     */
    @RPC(api = ApiKeys.PREPARE_LAKE_TABLE_SNAPSHOT)
    CompletableFuture<PrepareLakeTableSnapshotResponse> prepareLakeTableSnapshot(
            PrepareLakeTableSnapshotRequest request);

    /**
     * Commit lakehouse table snapshot to Fluss.
     *
     * @param request the request for committing lakehouse table snapshot.
     * @return commit lakehouse data response.
     */
    @RPC(api = ApiKeys.COMMIT_LAKE_TABLE_SNAPSHOT)
    CompletableFuture<CommitLakeTableSnapshotResponse> commitLakeTableSnapshot(
            CommitLakeTableSnapshotRequest request);

    /** Report lake tiering heartbeats to Fluss for lake tiering service. */
    @RPC(api = ApiKeys.LAKE_TIERING_HEARTBEAT)
    CompletableFuture<LakeTieringHeartbeatResponse> lakeTieringHeartbeat(
            LakeTieringHeartbeatRequest request);

    /** Try to controlled shutdown for tabletServer with specify tabletServerId. */
    @RPC(api = ApiKeys.CONTROLLED_SHUTDOWN)
    CompletableFuture<ControlledShutdownResponse> controlledShutdown(
            ControlledShutdownRequest request);
}
