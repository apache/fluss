/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.cluster;

/** The information of the coordinator server in Fluss cluster. */
public class CoordinatorServerInfo {
    private final String coordinatorId;
    private final ServerNode node;
    private final CoordinatorRole role;
    private final boolean isAlive;

    public CoordinatorServerInfo(
            String coordinatorId, ServerNode node, CoordinatorRole role, boolean isAlive) {
        this.coordinatorId = coordinatorId;
        this.node = node;
        this.role = role;
        this.isAlive = isAlive;
    }

    public String getCoordinatorId() {
        return coordinatorId;
    }

    public ServerNode getNode() {
        return node;
    }

    public CoordinatorRole getRole() {
        return role;
    }

    public boolean isAlive() {
        return isAlive;
    }

    @Override
    public String toString() {
        return "CoordinatorServerInfo{"
                + "coordinatorId='"
                + coordinatorId
                + '\''
                + ", node="
                + node
                + ", role="
                + role
                + ", isAlive="
                + isAlive
                + '}';
    }
}
