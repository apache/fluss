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
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
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

    // the request metadata used to correlate asynchronous responses
    private final Map<TableBucket, PbNotifyLeaderAndIsrReqForBucket> requestsForBuckets;

    public NotifyLeaderAndIsrResponseReceivedEvent(
            List<NotifyLeaderAndIsrResultForBucket> notifyLeaderAndIsrResultForBuckets,
            int responseServerId,
            Map<TableBucket, PbNotifyLeaderAndIsrReqForBucket> requestsForBuckets) {
        this.notifyLeaderAndIsrResultForBuckets = notifyLeaderAndIsrResultForBuckets;
        this.responseServerId = responseServerId;
        this.requestsForBuckets = Collections.unmodifiableMap(new HashMap<>(requestsForBuckets));
    }

    public int getResponseServerId() {
        return responseServerId;
    }

    public List<NotifyLeaderAndIsrResultForBucket> getNotifyLeaderAndIsrResultForBuckets() {
        return notifyLeaderAndIsrResultForBuckets;
    }

    public Map<TableBucket, PbNotifyLeaderAndIsrReqForBucket> getRequestsForBuckets() {
        return requestsForBuckets;
    }
}
