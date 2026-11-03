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

package org.apache.fluss.server.metadata;

import org.apache.fluss.server.zk.data.TabletServerRegistration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An immutable observation of a TabletServer registration and its ZooKeeper identity. */
public final class RegisteredTabletServer {
    private final int serverId;
    private final TabletServerRegistration registration;
    private final String path;
    private final int version;
    private final long ephemeralOwner;

    /** Captures the registration value, node version and owning session. */
    public RegisteredTabletServer(
            int serverId,
            TabletServerRegistration registration,
            String path,
            int version,
            long ephemeralOwner) {
        this.serverId = serverId;
        checkNotNull(registration);
        this.registration =
                new TabletServerRegistration(
                        registration.getRack(),
                        Collections.unmodifiableList(new ArrayList<>(registration.getEndpoints())),
                        registration.getRegisterTimestamp(),
                        registration.getResource(),
                        registration.getServerProtocolVersion());
        this.path = checkNotNull(path);
        this.version = version;
        this.ephemeralOwner = ephemeralOwner;
    }

    /** Returns the TabletServer ID. */
    public int getServerId() {
        return serverId;
    }

    /** Returns the observed registration. */
    public TabletServerRegistration getRegistration() {
        return registration;
    }

    /** Returns the registration node path. */
    public String getPath() {
        return path;
    }

    /** Returns the observed node version. */
    public int getVersion() {
        return version;
    }

    /** Returns the ZooKeeper session owning the registration. */
    public long getEphemeralOwner() {
        return ephemeralOwner;
    }

    /** Checks the complete registration observation, including the owning session. */
    public boolean matchesRegistration(RegisteredTabletServer other) {
        return path.equals(other.path)
                && version == other.version
                && ephemeralOwner == other.ephemeralOwner
                && registration.equals(other.registration);
    }

    /** Checks that the registration metadata agrees with the Coordinator's live server view. */
    public boolean matchesLive(ServerInfo live) {
        return Objects.equals(registration.getRack(), live.rack())
                && registration.getResource().equals(live.resource())
                && new HashSet<>(registration.getEndpoints())
                        .equals(new HashSet<>(live.endpoints()));
    }
}
