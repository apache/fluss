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

package org.apache.fluss.server.storage;

import org.apache.fluss.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Collects the local data disk usage ratio for the tablet server. The reported ratio is the
 * <b>maximum</b> usage across all distinct {@link FileStore}s backing the configured data
 * directories. A per-disk maximum (rather than a weighted average over total/used bytes) is used so
 * that a single nearly-full disk cannot be masked by other low-usage disks in a multi-disk
 * deployment: any single disk crossing the limit ratio must trip the write protection, because
 * partitions pinned to that disk would otherwise fail to write. Multiple data directories sharing
 * the same physical {@link FileStore} are still counted only once.
 */
@Internal
public final class DiskUsageCollector {

    private static final Logger LOG = LoggerFactory.getLogger(DiskUsageCollector.class);

    private final List<File> dataDirs;

    public DiskUsageCollector(List<File> dataDirs) {
        checkNotNull(dataDirs, "dataDirs");
        this.dataDirs = Collections.unmodifiableList(dataDirs);
    }

    /**
     * Collects the current disk usage ratio in the range {@code [0.0, 1.0]}, defined as the maximum
     * usage across all distinct {@link FileStore}s. Returns {@code 0.0} when no data directory is
     * configured or every reachable file store reports a non-positive total space.
     *
     * <p>Individual directories that fail (e.g. deleted at runtime, or whose {@link FileStore}
     * throws during {@code getTotalSpace}/{@code getUsableSpace}) are skipped with a warning so
     * that one unhealthy directory does not prevent monitoring of the remaining disks. An {@link
     * IOException} is thrown only when <b>all</b> directories fail.
     */
    public double collect() throws IOException {
        double maxRatio = 0.0;
        Set<FileStore> counted = new HashSet<>();
        int failures = 0;
        for (File dir : dataDirs) {
            try {
                FileStore fs = Files.getFileStore(dir.toPath());
                if (counted.add(fs)) {
                    long total = fs.getTotalSpace();
                    if (total <= 0L) {
                        continue;
                    }
                    double ratio = (double) (total - fs.getUsableSpace()) / total;
                    if (ratio > maxRatio) {
                        maxRatio = ratio;
                    }
                }
            } catch (IOException e) {
                LOG.warn("Failed to stat FileStore for data directory {}; skipping.", dir, e);
                failures++;
            }
        }
        if (failures > 0 && failures == dataDirs.size()) {
            throw new IOException("All " + failures + " data directories failed FileStore lookup.");
        }
        return maxRatio;
    }
}
