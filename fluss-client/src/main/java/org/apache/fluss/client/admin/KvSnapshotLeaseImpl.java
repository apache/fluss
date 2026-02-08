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

package org.apache.fluss.client.admin;

import org.apache.fluss.client.metadata.AcquireKvSnapshotLeaseResult;
import org.apache.fluss.client.utils.ClientRpcMessageUtils;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.messages.AcquireKvSnapshotLeaseRequest;
import org.apache.fluss.rpc.messages.DropKvSnapshotLeaseRequest;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makeAcquireKvSnapshotLeaseRequest;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makeReleaseKvSnapshotLeaseRequest;

/** The default implementation of KvSnapshotLease. */
public class KvSnapshotLeaseImpl implements KvSnapshotLease {
    private final String leaseId;
    private final long leaseDurationMs;
    private final AdminGateway gateway;

    public KvSnapshotLeaseImpl(String leaseId, long leaseDurationMs, AdminGateway gateway) {
        this.leaseId = leaseId;
        this.leaseDurationMs = leaseDurationMs;
        this.gateway = gateway;
    }

    @Override
    public String leaseId() {
        return leaseId;
    }

    @Override
    public long leaseDurationMs() {
        return leaseDurationMs;
    }

    @Override
    public CompletableFuture<AcquireKvSnapshotLeaseResult> acquireSnapshots(
            Map<TableBucket, Long> snapshotIds) {
        if (snapshotIds.isEmpty()) {
            throw new IllegalArgumentException(
                    "The snapshotIds to acquire kv snapshot lease is empty");
        }

        return gateway.acquireKvSnapshotLease(
                        makeAcquireKvSnapshotLeaseRequest(leaseId, snapshotIds, leaseDurationMs))
                .thenApply(ClientRpcMessageUtils::toAcquireKvSnapshotLeaseResult);
    }

    @Override
    public CompletableFuture<Void> renew() {
        AcquireKvSnapshotLeaseRequest request =
                new AcquireKvSnapshotLeaseRequest()
                        .setLeaseId(leaseId)
                        .setLeaseDurationMs(leaseDurationMs);
        return gateway.acquireKvSnapshotLease(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> releaseSnapshots(Set<TableBucket> bucketsToRelease) {
        return gateway.releaseKvSnapshotLease(
                        makeReleaseKvSnapshotLeaseRequest(leaseId, bucketsToRelease))
                .thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> dropLease() {
        DropKvSnapshotLeaseRequest request = new DropKvSnapshotLeaseRequest().setLeaseId(leaseId);
        return gateway.dropKvSnapshotLease(request).thenApply(r -> null);
    }
}
