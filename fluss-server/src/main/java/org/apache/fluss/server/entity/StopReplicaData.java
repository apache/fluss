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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.StopReplicaRequest;

/** The data to stop replica for request {@link StopReplicaRequest}. */
public class StopReplicaData {
    private final TableBucket tableBucket;
    private final boolean deleteLocal;
    private final boolean deleteRemote;
    private final int coordinatorEpoch;
    private final int leaderEpoch;

    public StopReplicaData(
            TableBucket tableBucket,
            boolean deleteLocal,
            boolean deleteRemote,
            int coordinatorEpoch,
            int leaderEpoch) {
        this.tableBucket = tableBucket;
        this.deleteLocal = deleteLocal;
        this.deleteRemote = deleteRemote;
        this.coordinatorEpoch = coordinatorEpoch;
        this.leaderEpoch = leaderEpoch;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public boolean isDeleteLocal() {
        return deleteLocal;
    }

    public boolean isDeleteRemote() {
        return deleteRemote;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }
}
