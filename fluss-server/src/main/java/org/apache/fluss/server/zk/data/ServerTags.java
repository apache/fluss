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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.cluster.rebalance.ServerTag;

import java.util.Map;
import java.util.Objects;

/**
 * The latest {@link ServerTags} of tabletServers in {@link ZkData.ServerTagsZNode}. It is used to
 * store the serverTags information in zookeeper.
 *
 * @see ServerTagsJsonSerde for json serialization and deserialization.
 */
public class ServerTags {

    // a mapping from tabletServer id to serverTag.
    private final Map<Integer, ServerTag> serverTags;

    public ServerTags(Map<Integer, ServerTag> serverTags) {
        this.serverTags = serverTags;
    }

    public Map<Integer, ServerTag> getServerTags() {
        return serverTags;
    }

    @Override
    public String toString() {
        return "ServerTags{" + "serverTags=" + serverTags + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServerTags that = (ServerTags) o;
        return Objects.equals(serverTags, that.serverTags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverTags);
    }
}
