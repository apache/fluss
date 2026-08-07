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

import java.util.Objects;

/** Authoritative remote log manifest handle together with its ZooKeeper data version. */
public final class VersionedRemoteLogManifestHandle {
    private final RemoteLogManifestHandle handle;
    private final int zkVersion;

    public VersionedRemoteLogManifestHandle(RemoteLogManifestHandle handle, int zkVersion) {
        this.handle = Objects.requireNonNull(handle, "handle");
        this.zkVersion = zkVersion;
    }

    public RemoteLogManifestHandle handle() {
        return handle;
    }

    public int zkVersion() {
        return zkVersion;
    }
}
