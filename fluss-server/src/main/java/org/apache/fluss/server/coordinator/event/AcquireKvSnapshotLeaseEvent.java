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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.metadata.KvSnapshotLeaseForBucket;
import org.apache.fluss.rpc.messages.AcquireKvSnapshotLeaseResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** An event for acquire a kv snapshot lease. */
public class AcquireKvSnapshotLeaseEvent implements CoordinatorEvent {
    private final String leaseId;
    private final long leaseDuration;
    private final Map<Long, List<KvSnapshotLeaseForBucket>> tableIdToLeasedBucket;
    private final CompletableFuture<AcquireKvSnapshotLeaseResponse> respCallback;

    public AcquireKvSnapshotLeaseEvent(
            String leaseId,
            long leaseDuration,
            Map<Long, List<KvSnapshotLeaseForBucket>> tableIdToLeasedBucket,
            CompletableFuture<AcquireKvSnapshotLeaseResponse> respCallback) {
        this.leaseId = leaseId;
        this.leaseDuration = leaseDuration;
        this.tableIdToLeasedBucket = tableIdToLeasedBucket;
        this.respCallback = respCallback;
    }

    public String getLeaseId() {
        return leaseId;
    }

    public long getLeaseDuration() {
        return leaseDuration;
    }

    public Map<Long, List<KvSnapshotLeaseForBucket>> getTableIdToLeasedBucket() {
        return tableIdToLeasedBucket;
    }

    public CompletableFuture<AcquireKvSnapshotLeaseResponse> getRespCallback() {
        return respCallback;
    }
}
