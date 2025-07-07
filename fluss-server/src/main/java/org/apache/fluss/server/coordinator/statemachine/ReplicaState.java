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

package org.apache.fluss.server.coordinator.statemachine;

import org.apache.fluss.metadata.TableBucketReplica;

import java.util.EnumSet;
import java.util.Set;

/** An enum for the state of the {@link TableBucketReplica}. */
public enum ReplicaState implements BaseState<ReplicaState> {
    NonExistentReplica {
        @Override
        public Set<ReplicaState> getValidPreviousStates() {
            return EnumSet.of(ReplicaDeletionSuccessful);
        }
    },
    NewReplica {
        @Override
        public Set<ReplicaState> getValidPreviousStates() {
            return EnumSet.of(NonExistentReplica);
        }
    },
    OnlineReplica {
        @Override
        public Set<ReplicaState> getValidPreviousStates() {
            return EnumSet.of(NewReplica, OnlineReplica, OfflineReplica);
        }
    },
    OfflineReplica {
        @Override
        public Set<ReplicaState> getValidPreviousStates() {
            return EnumSet.of(NewReplica, OnlineReplica, OfflineReplica);
        }
    },
    ReplicaDeletionStarted {
        @Override
        public Set<ReplicaState> getValidPreviousStates() {
            // ReplicaDeletionStarted as a valid previous state since
            // we will try to delete the replica when deletion fail
            return EnumSet.of(OfflineReplica, ReplicaDeletionStarted);
        }
    },
    ReplicaDeletionSuccessful {
        @Override
        public Set<ReplicaState> getValidPreviousStates() {
            return EnumSet.of(ReplicaDeletionStarted);
        }
    }
}
