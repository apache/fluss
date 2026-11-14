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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.annotation.Internal;

/**
 * Work item for cleaning an orphan table or partition directory. The directory has already been
 * identified as an orphan candidate by {@link ScopeEnumeratorFunction} (ID guard satisfied).
 */
@Internal
public final class OrphanDirCleanTask implements CleanTask {

    private static final long serialVersionUID = 1L;

    private final String dirPath;
    private final long cutoffMillis;
    private final boolean dryRun;
    private final boolean allowDeleteManifest;

    public OrphanDirCleanTask(
            String dirPath, long cutoffMillis, boolean dryRun, boolean allowDeleteManifest) {
        this.dirPath = dirPath;
        this.cutoffMillis = cutoffMillis;
        this.dryRun = dryRun;
        this.allowDeleteManifest = allowDeleteManifest;
    }

    public String dirPath() {
        return dirPath;
    }

    public long cutoffMillis() {
        return cutoffMillis;
    }

    public boolean dryRun() {
        return dryRun;
    }

    public boolean allowDeleteManifest() {
        return allowDeleteManifest;
    }
}
