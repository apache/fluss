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
import org.apache.fluss.rpc.messages.UnregisterKvSnapshotConsumerResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** An event for unregister a kv snapshot consumer. */
public class UnregisterKvSnapshotConsumerEvent implements CoordinatorEvent {
    private final String consumerId;
    private final Map<Long, List<TableBucket>> tableIdToUnregisterBucket;
    private final CompletableFuture<UnregisterKvSnapshotConsumerResponse> respCallback;

    public UnregisterKvSnapshotConsumerEvent(
            String consumerId,
            Map<Long, List<TableBucket>> tableIdToUnregisterBucket,
            CompletableFuture<UnregisterKvSnapshotConsumerResponse> respCallback) {
        this.consumerId = consumerId;
        this.tableIdToUnregisterBucket = tableIdToUnregisterBucket;
        this.respCallback = respCallback;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public Map<Long, List<TableBucket>> getTableIdToUnregisterBucket() {
        return tableIdToUnregisterBucket;
    }

    public CompletableFuture<UnregisterKvSnapshotConsumerResponse> getRespCallback() {
        return respCallback;
    }
}
