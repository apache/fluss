/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.cluster;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.Objects;

/**
 * Information about a Fluss server node.
 *
 * @since 0.6
 */
@PublicEvolving
public class ServerNode {
    private final int id;
    private final String uid;
    private final String host;
    private final int port;
    private final ServerType serverType;

    // Cache hashCode as it is called in performance sensitive parts of the code (e.g.
    // RecordAccumulator.ready)
    private Integer hash;

    public ServerNode(int id, String host, int port, ServerType serverType) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.serverType = serverType;
        if (serverType == ServerType.COORDINATOR) {
            this.uid = "cs-" + id;
        } else {
            this.uid = "ts-" + id;
        }
    }

    /**
     * The node id of this node. Note: coordinator server may have conflict node id with tablet
     * server.
     */
    public int id() {
        return id;
    }

    /**
     * Unique id of server node in the cluster. It distinguishes same node id of coordinator server
     * and tablet server with different string prefix.
     */
    public String uid() {
        return uid;
    }

    /** The host name for this node. */
    public String host() {
        return host;
    }

    /** The port for this node. */
    public int port() {
        return port;
    }

    /** The server type of this node. */
    public ServerType serverType() {
        return serverType;
    }

    /**
     * Check whether this node is empty, which may be the case if noNode() is used as a placeholder
     * in a response payload with an error.
     *
     * @return true if it is, false otherwise
     */
    public boolean isEmpty() {
        return host == null || host.isEmpty() || port < 0;
    }

    @Override
    public int hashCode() {
        Integer h = this.hash;
        if (h == null) {
            int result = 31 + ((host == null) ? 0 : host.hashCode());
            result = 31 * result + id;
            result = 31 * result + port;
            result = 31 * result + serverType.hashCode();
            this.hash = result;
            return result;
        } else {
            return h;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ServerNode other = (ServerNode) obj;
        return id == other.id
                && port == other.port
                && Objects.equals(host, other.host)
                && serverType == other.serverType;
    }

    @Override
    public String toString() {
        return host + ":" + port + " (id: " + uid + ")";
    }
}
