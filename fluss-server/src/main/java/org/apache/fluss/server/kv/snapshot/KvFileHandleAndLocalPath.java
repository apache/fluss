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

package org.apache.fluss.server.kv.snapshot;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A Holder of KvFileHandle which contains a remote path for the kv file after updated and the
 * corresponding localPath.
 */
public final class KvFileHandleAndLocalPath {

    private final KvFileHandle kvFileHandle;
    private final String localPath;

    private KvFileHandleAndLocalPath(KvFileHandle handle, String localPath) {
        this.kvFileHandle = handle;
        this.localPath = localPath;
    }

    public static KvFileHandleAndLocalPath of(KvFileHandle kvFileHandle, String localPath) {
        checkNotNull(kvFileHandle, "KvFileHandle cannot be null");
        checkNotNull(localPath, "localPath cannot be null");
        return new KvFileHandleAndLocalPath(kvFileHandle, localPath);
    }

    public KvFileHandle getKvFileHandle() {
        return kvFileHandle;
    }

    public String getLocalPath() {
        return localPath;
    }

    @Override
    public String toString() {
        return "KvFileHandleAndLocalPath{"
                + "kvFileHandle="
                + kvFileHandle
                + ", localPath='"
                + localPath
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KvFileHandleAndLocalPath that = (KvFileHandleAndLocalPath) o;
        return Objects.equals(kvFileHandle, that.kvFileHandle)
                && Objects.equals(localPath, that.localPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kvFileHandle, localPath);
    }
}
