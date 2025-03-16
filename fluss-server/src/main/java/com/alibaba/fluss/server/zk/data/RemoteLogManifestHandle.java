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

import com.alibaba.fluss.fs.FsPath;

/**
 * The remote log manifest handle of a table bucket stored in {@link ZkData.BucketRemoteLogsZNode}.
 *
 * @see RemoteLogManifestHandleJsonSerde for json serialization and deserialization.
 */
public class RemoteLogManifestHandle {
    private final FsPath remoteLogManifestPath;
    private final long remoteLogEndOffset;

    public RemoteLogManifestHandle(FsPath remoteLogManifestPath, long remoteLogEndOffset) {
        this.remoteLogManifestPath = remoteLogManifestPath;
        this.remoteLogEndOffset = remoteLogEndOffset;
    }

    public static FsPath fromRemoteLogManifestPath(String remoteLogManifestPath) {
        return new FsPath(remoteLogManifestPath);
    }

    public FsPath getRemoteLogManifestPath() {
        return remoteLogManifestPath;
    }

    public long getRemoteLogEndOffset() {
        return remoteLogEndOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteLogManifestHandle that = (RemoteLogManifestHandle) o;
        return remoteLogManifestPath.equals(that.remoteLogManifestPath)
                && remoteLogEndOffset == that.remoteLogEndOffset;
    }

    @Override
    public String toString() {
        return "RemoteLogManifestHandle{"
                + "remoteLogManifestPath="
                + remoteLogManifestPath
                + ", remoteLogEndOffset="
                + remoteLogEndOffset
                + '}';
    }
}
