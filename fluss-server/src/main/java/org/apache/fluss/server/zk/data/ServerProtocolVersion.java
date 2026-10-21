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

import org.apache.fluss.annotation.Internal;

import java.util.Collection;

/**
 * The protocol version a Fluss server advertises in its ZooKeeper registration znode, used to gate
 * features that require every server in the cluster to understand a newer protocol.
 */
@Internal
public final class ServerProtocolVersion {

    /**
     * The protocol version advertised by servers built from this code. Servers that predate bulk
     * load do not write the field into their registration znode at all, so they are read back as
     * {@code 0}.
     */
    public static final int CURRENT = 1;

    /** The minimum protocol version every server must advertise to admit a bulk load. */
    public static final int BULK_LOAD = 1;

    private ServerProtocolVersion() {}

    /**
     * Returns true only when the cluster has at least one registered Coordinator and every
     * registered Coordinator advertises a protocol version supporting bulk load.
     */
    public static boolean coordinatorsSupportBulkLoad(
            Collection<CoordinatorAddress> registrations) {
        if (registrations.isEmpty()) {
            return false;
        }
        for (CoordinatorAddress registration : registrations) {
            if (registration.getServerProtocolVersion() < BULK_LOAD) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true only when the cluster has at least one registered TabletServer and every
     * registered TabletServer advertises a protocol version supporting bulk load.
     */
    public static boolean tabletServersSupportBulkLoad(
            Collection<TabletServerRegistration> registrations) {
        if (registrations.isEmpty()) {
            return false;
        }
        for (TabletServerRegistration registration : registrations) {
            if (registration.getServerProtocolVersion() < BULK_LOAD) {
                return false;
            }
        }
        return true;
    }
}
