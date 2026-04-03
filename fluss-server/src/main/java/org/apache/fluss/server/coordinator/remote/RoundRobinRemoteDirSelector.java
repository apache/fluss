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

    private final String remoteDataDir;
    private final List<String> remoteDataDirs;

    // Current position in the round-robin cycle.
    private final AtomicInteger position;

    public RoundRobinRemoteDirSelector(String remoteDataDir, List<String> remoteDataDirs) {
        this.remoteDataDir = remoteDataDir;
        this.remoteDataDirs = Collections.unmodifiableList(remoteDataDirs);
        this.position = new AtomicInteger(0);
    }

    @Override
    public String nextDataDir() {
        if (remoteDataDirs.isEmpty()) {
            return remoteDataDir;
        }

        int index = position.getAndUpdate(i -> (i + 1) % remoteDataDirs.size());
        return remoteDataDirs.get(index);
    }
}
