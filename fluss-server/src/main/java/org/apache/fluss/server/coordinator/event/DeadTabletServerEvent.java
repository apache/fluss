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

import java.util.Objects;

import static org.apache.fluss.server.metadata.ServerInfo.UNKNOWN_TABLET_SERVER_EPOCH;

/** An event for tablet server became dead. */
public class DeadTabletServerEvent implements CoordinatorEvent {

    private final int serverId;
    private final long tabletServerEpoch;

    public DeadTabletServerEvent(int serverId) {
        this(serverId, UNKNOWN_TABLET_SERVER_EPOCH);
    }

    public DeadTabletServerEvent(int serverId, long tabletServerEpoch) {
        this.serverId = serverId;
        this.tabletServerEpoch = tabletServerEpoch;
    }

    public int getServerId() {
        return serverId;
    }

    public long getTabletServerEpoch() {
        return tabletServerEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeadTabletServerEvent that = (DeadTabletServerEvent) o;
        return serverId == that.serverId && tabletServerEpoch == that.tabletServerEpoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId, tabletServerEpoch);
    }

    @Override
    public String toString() {
        return "DeadTabletServerEvent{"
                + "serverId="
                + serverId
                + ", tabletServerEpoch="
                + tabletServerEpoch
                + '}';
    }
}
