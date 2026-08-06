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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FsPath;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A bucket-scoped remote storage object discovered by the orphan sweeper. */
@Internal
public final class RemoteLogStorageObject {
    private final FsPath path;
    private final long modificationTimeMs;

    /** Creates a discovered object with its storage modification timestamp. */
    public RemoteLogStorageObject(FsPath path, long modificationTimeMs) {
        this.path = checkNotNull(path);
        this.modificationTimeMs = modificationTimeMs;
    }

    /** Returns the full remote path. */
    public FsPath path() {
        return path;
    }

    /** Returns the storage modification time, or {@link Long#MAX_VALUE} when unavailable. */
    public long modificationTimeMs() {
        return modificationTimeMs;
    }
}
