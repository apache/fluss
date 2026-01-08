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

package org.apache.fluss.server.coordinator.rebalance.model;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.replica.Replica;

import java.util.Objects;

/** A class that holds the information of the {@link Replica} for rebalance. */
public class ReplicaModel {
    private final TableBucket tableBucket;
    private final ServerModel originalServer;
    private ServerModel server;
    private boolean isLeader;

    public ReplicaModel(TableBucket tableBucket, ServerModel server, boolean isLeader) {
        this.tableBucket = tableBucket;
        this.server = server;
        this.isLeader = isLeader;
        this.originalServer = server;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    public ServerModel originalServer() {
        return originalServer;
    }

    public ServerModel server() {
        return server;
    }

    public int serverId() {
        return server.id();
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void makeFollower() {
        setLeadership(false);
    }

    public void makeLeader() {
        setLeadership(true);
    }

    void setLeadership(boolean leader) {
        isLeader = leader;
    }

    public void setServer(ServerModel server) {
        this.server = server;
    }

    @Override
    public String toString() {
        return String.format(
                "ReplicaModel[TableBucket=%s,isLeader=%s,rack=%s,server=%s,originalServer=%s]",
                tableBucket, isLeader, server.rack(), server.id(), originalServer.id());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplicaModel that = (ReplicaModel) o;
        return Objects.equals(tableBucket, that.tableBucket)
                && originalServer.id() == that.originalServer.id();
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableBucket, originalServer.id());
    }
}
