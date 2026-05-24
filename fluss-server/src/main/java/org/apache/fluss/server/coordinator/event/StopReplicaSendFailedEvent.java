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

import org.apache.fluss.metadata.TableBucketReplica;

import java.util.Collections;
import java.util.Set;

/**
 * Signals that a {@code stopReplica(delete=true)} request could not be delivered at the RPC layer
 * (e.g., gateway missing or network error). The event processor transitions affected replicas to
 * {@code ReplicaDeletionIneligible} until the tablet server reconnects.
 */
public class StopReplicaSendFailedEvent implements CoordinatorEvent {

    private final Set<TableBucketReplica> failedReplicas;

    public StopReplicaSendFailedEvent(Set<TableBucketReplica> failedReplicas) {
        this.failedReplicas = Collections.unmodifiableSet(failedReplicas);
    }

    public Set<TableBucketReplica> getFailedReplicas() {
        return failedReplicas;
    }
}
