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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Aggregatable cleanup statistics emitted by each {@link ScanAndCleanFunction} subtask. The {@code
 * touchedDirs} list is collected by the final aggregator for empty-directory sweeping after all
 * subtasks complete.
 */
@Internal
public final class CleanStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long scanned;
    private final long deleted;
    private final long deleteFailures;
    private final long bytesReclaimed;
    private final List<String> touchedDirs;

    public CleanStats(
            long scanned,
            long deleted,
            long deleteFailures,
            long bytesReclaimed,
            List<String> touchedDirs) {
        this.scanned = scanned;
        this.deleted = deleted;
        this.deleteFailures = deleteFailures;
        this.bytesReclaimed = bytesReclaimed;
        this.touchedDirs = new ArrayList<>(touchedDirs);
    }

    public static CleanStats empty() {
        return new CleanStats(0L, 0L, 0L, 0L, new ArrayList<String>());
    }

    public long scanned() {
        return scanned;
    }

    public long deleted() {
        return deleted;
    }

    public long deleteFailures() {
        return deleteFailures;
    }

    public long bytesReclaimed() {
        return bytesReclaimed;
    }

    public List<String> touchedDirs() {
        return touchedDirs;
    }

    public CleanStats merge(CleanStats other) {
        List<String> mergedDirs = new ArrayList<>(this.touchedDirs);
        mergedDirs.addAll(other.touchedDirs);
        return new CleanStats(
                this.scanned + other.scanned,
                this.deleted + other.deleted,
                this.deleteFailures + other.deleteFailures,
                this.bytesReclaimed + other.bytesReclaimed,
                mergedDirs);
    }
}
