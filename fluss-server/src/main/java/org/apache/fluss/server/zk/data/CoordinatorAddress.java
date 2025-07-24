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

import org.apache.fluss.cluster.Endpoint;

import java.util.List;
import java.util.Objects;

/**
 * The address information of an active coordinator stored in {@link ZkData.CoordinatorLeaderZNode}.
 *
 * @see CoordinatorAddressJsonSerde for json serialization and deserialization.
 */
public class CoordinatorAddress {
    private final int id;
    private final List<Endpoint> endpoints;

    public CoordinatorAddress(int id, List<Endpoint> endpoints) {
        this.id = id;
        this.endpoints = endpoints;
    }

    public int getId() {
        return id;
    }

    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CoordinatorAddress that = (CoordinatorAddress) o;
        return Objects.equals(id, that.id) && Objects.equals(endpoints, that.endpoints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, endpoints);
    }

    @Override
    public String toString() {
        return "CoordinatorAddress{" + "id='" + id + '\'' + ", endpoints=" + endpoints + '}';
    }
}
