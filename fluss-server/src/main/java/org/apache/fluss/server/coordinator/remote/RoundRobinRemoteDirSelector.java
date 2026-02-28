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

package org.apache.fluss.server.coordinator.remote;

import org.apache.fluss.fs.FsPath;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round-robin remote data dir selector.
 *
 * <p>This implementation cycles through the available remote data directories in order, ensuring
 * each directory is selected once before repeating.
 *
 * <p>Example: For directories [A, B, C], the selection sequence would be: A, B, C, A, B, C, ...
 */
public class RoundRobinRemoteDirSelector implements RemoteDirSelector {

    private final FsPath defaultRemoteDataDir;
    private final List<FsPath> remoteDataDirs;

    // Current position in the round-robin cycle.
    private final AtomicInteger position = new AtomicInteger(0);

    public RoundRobinRemoteDirSelector(FsPath defaultRemoteDataDir, List<FsPath> remoteDataDirs) {
        this.defaultRemoteDataDir = defaultRemoteDataDir;
        this.remoteDataDirs = Collections.unmodifiableList(remoteDataDirs);
    }

    @Override
    public FsPath nextDataDir() {
        if (remoteDataDirs.isEmpty()) {
            return defaultRemoteDataDir;
        }

        int index = position.getAndUpdate(i -> (i + 1) % remoteDataDirs.size());
        return remoteDataDirs.get(index);
    }
}
