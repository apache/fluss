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

/** Fixed-size counters for one orphan cleanup task. */
@Internal
public final class CleanupCounters implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long scannedFiles;
    private final long plannedFiles;
    private final long plannedDirs;
    private final long plannedBytes;
    private final long deletedFiles;
    private final long emptyDirsRemoved;
    private final long deleteFailures;
    private final long bytesReclaimed;

    public CleanupCounters(
            long scannedFiles,
            long plannedFiles,
            long plannedDirs,
            long plannedBytes,
            long deletedFiles,
            long emptyDirsRemoved,
            long deleteFailures,
            long bytesReclaimed) {
        this.scannedFiles = scannedFiles;
        this.plannedFiles = plannedFiles;
        this.plannedDirs = plannedDirs;
        this.plannedBytes = plannedBytes;
        this.deletedFiles = deletedFiles;
        this.emptyDirsRemoved = emptyDirsRemoved;
        this.deleteFailures = deleteFailures;
        this.bytesReclaimed = bytesReclaimed;
    }

    public static CleanupCounters empty() {
        return new CleanupCounters(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    public CleanupCounters add(CleanupCounters other) {
        return new CleanupCounters(
                scannedFiles + other.scannedFiles,
                plannedFiles + other.plannedFiles,
                plannedDirs + other.plannedDirs,
                plannedBytes + other.plannedBytes,
                deletedFiles + other.deletedFiles,
                emptyDirsRemoved + other.emptyDirsRemoved,
                deleteFailures + other.deleteFailures,
                bytesReclaimed + other.bytesReclaimed);
    }

    public long scannedFiles() {
        return scannedFiles;
    }

    public long plannedFiles() {
        return plannedFiles;
    }

    public long plannedDirs() {
        return plannedDirs;
    }

    public long plannedBytes() {
        return plannedBytes;
    }

    public long deletedFiles() {
        return deletedFiles;
    }

    public long emptyDirsRemoved() {
        return emptyDirsRemoved;
    }

    public long deleteFailures() {
        return deleteFailures;
    }

    public long bytesReclaimed() {
        return bytesReclaimed;
    }
}
