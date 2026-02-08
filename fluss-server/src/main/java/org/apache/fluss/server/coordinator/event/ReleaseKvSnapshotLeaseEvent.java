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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.ReleaseKvSnapshotLeaseResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** An event for release a kv snapshot lease for table buckets. */
public class ReleaseKvSnapshotLeaseEvent implements CoordinatorEvent {
    private final String leaseId;
    private final Map<Long, List<TableBucket>> tableIdToReleasedBucket;
    private final CompletableFuture<ReleaseKvSnapshotLeaseResponse> respCallback;

    public ReleaseKvSnapshotLeaseEvent(
            String leaseId,
            Map<Long, List<TableBucket>> tableIdToReleasedBucket,
            CompletableFuture<ReleaseKvSnapshotLeaseResponse> respCallback) {
        this.leaseId = leaseId;
        this.tableIdToReleasedBucket = tableIdToReleasedBucket;
        this.respCallback = respCallback;
    }

    public String getLeaseId() {
        return leaseId;
    }

    public Map<Long, List<TableBucket>> getTableIdToReleasedBucket() {
        return tableIdToReleasedBucket;
    }

    public CompletableFuture<ReleaseKvSnapshotLeaseResponse> getRespCallback() {
        return respCallback;
    }
}
