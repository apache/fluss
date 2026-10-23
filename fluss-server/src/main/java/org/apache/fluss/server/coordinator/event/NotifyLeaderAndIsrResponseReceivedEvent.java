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
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** An event for receive the response of {@link NotifyLeaderAndIsrRequest} from tablet server. */
public class NotifyLeaderAndIsrResponseReceivedEvent implements CoordinatorEvent {

    private final List<NotifyLeaderAndIsrResultForBucket> notifyLeaderAndIsrResultForBuckets;

    // the server id that return the response
    private final int responseServerId;

    private final Map<TableBucket, Long> pendingLeaderActivationIds;

    public NotifyLeaderAndIsrResponseReceivedEvent(
            List<NotifyLeaderAndIsrResultForBucket> notifyLeaderAndIsrResultForBuckets,
            int responseServerId) {
        this(notifyLeaderAndIsrResultForBuckets, responseServerId, Collections.emptyMap());
    }

    public NotifyLeaderAndIsrResponseReceivedEvent(
            List<NotifyLeaderAndIsrResultForBucket> notifyLeaderAndIsrResultForBuckets,
            int responseServerId,
            Map<TableBucket, Long> pendingLeaderActivationIds) {
        this.notifyLeaderAndIsrResultForBuckets = notifyLeaderAndIsrResultForBuckets;
        this.responseServerId = responseServerId;
        this.pendingLeaderActivationIds =
                Collections.unmodifiableMap(new HashMap<>(pendingLeaderActivationIds));
    }

    public int getResponseServerId() {
        return responseServerId;
    }

    public List<NotifyLeaderAndIsrResultForBucket> getNotifyLeaderAndIsrResultForBuckets() {
        return notifyLeaderAndIsrResultForBuckets;
    }

    public Map<TableBucket, Long> getPendingLeaderActivationIds() {
        return pendingLeaderActivationIds;
    }
}
