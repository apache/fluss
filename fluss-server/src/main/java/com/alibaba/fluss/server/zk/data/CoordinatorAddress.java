/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.zk.data;

import java.util.Objects;

/**
 * The address information of an active coordinator stored in {@link ZkData.CoordinatorZNode}.
 *
 * @see CoordinatorAddressJsonSerde for json serialization and deserialization.
 */
public class CoordinatorAddress {
    private final String id;
    private final String host;
    private final int port;

    public CoordinatorAddress(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CoordinatorAddress that = (CoordinatorAddress) o;
        return port == that.port && Objects.equals(id, that.id) && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, host, port);
    }

    @Override
    public String toString() {
        return "CoordinatorAddress{"
                + "id='"
                + id
                + '\''
                + ", host='"
                + host
                + '\''
                + ", port="
                + port
                + '}';
    }
}
