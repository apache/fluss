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

package org.apache.fluss.server.coordinator.compaction;

import org.apache.fluss.cluster.compaction.CompactionInfo;
import org.apache.fluss.cluster.compaction.CompactionStatus;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable bounded task metadata; bucket details live in remote files. */
public final class CompactionTask {
    private final CompactionInfo info;
    private final long deadline;
    private final long total;
    private final long succeeded;
    private final long failed;
    private final long cancelled;
    private final long finishTime;
    private final long persistedTime;
    @Nullable private final FsPath planHandle;
    @Nullable private final FsPath progressHandle;
    @Nullable private final String firstError;
    private final int version;

    /** Creates a task record. The version is supplied by ZooKeeper when loaded. */
    public CompactionTask(
            CompactionInfo info,
            long deadline,
            long total,
            long succeeded,
            long failed,
            long cancelled,
            long finishTime,
            long persistedTime,
            @Nullable FsPath planHandle,
            @Nullable FsPath progressHandle,
            @Nullable String firstError,
            int version) {
        checkNotNull(info);
        checkArgument(deadline >= info.createTime(), "Invalid deadline");
        checkArgument(
                total >= 0
                        && succeeded >= 0
                        && failed >= 0
                        && cancelled >= 0
                        && succeeded <= total
                        && failed <= total - succeeded
                        && cancelled <= total - succeeded - failed,
                "Invalid compaction counts");
        this.info = info;
        this.deadline = deadline;
        this.total = total;
        this.succeeded = succeeded;
        this.failed = failed;
        this.cancelled = cancelled;
        this.finishTime = finishTime;
        this.persistedTime = persistedTime;
        this.planHandle = planHandle;
        this.progressHandle = progressHandle;
        this.firstError = firstError;
        this.version = version;
    }

    /** Returns the submitted scope and current status. */
    public CompactionInfo info() {
        return info;
    }

    /** Returns the absolute deadline in epoch milliseconds. */
    public long deadline() {
        return deadline;
    }

    /** Returns the fixed number of target buckets. */
    public long total() {
        return total;
    }

    /** Returns the number of published successful results. */
    public long succeeded() {
        return succeeded;
    }

    /** Returns the number of failed buckets. */
    public long failed() {
        return failed;
    }

    /** Returns the number of cancelled buckets. */
    public long cancelled() {
        return cancelled;
    }

    /** Returns the terminal time in epoch milliseconds, or zero while active. */
    public long finishTime() {
        return finishTime;
    }

    /** Returns the timestamp stored atomically with the most recent published summary. */
    public long persistedTime() {
        return persistedTime;
    }

    /** Returns the published plan manifest, or null before publication. */
    @Nullable
    public FsPath planHandle() {
        return planHandle;
    }

    /** Returns the published progress chain head, or null before publication. */
    @Nullable
    public FsPath progressHandle() {
        return progressHandle;
    }

    /** Returns the first operation error, if any. */
    @Nullable
    public String firstError() {
        return firstError;
    }

    /** Returns the observed ZooKeeper record version. */
    public int version() {
        return version;
    }

    /** Whether no more task updates may be published. */
    public boolean isTerminal() {
        return info.status() == CompactionStatus.SUCCEEDED
                || info.status() == CompactionStatus.FAILED
                || info.status() == CompactionStatus.CANCELLED;
    }

    /** Fixed ordered targets and the cluster configuration bound at submission. */
    public static final class Plan {
        private final List<TableBucket> buckets;
        private final Map<String, String> configuration;

        /** Creates an immutable plan. */
        public Plan(List<TableBucket> buckets, Map<String, String> configuration) {
            this.buckets = Collections.unmodifiableList(new ArrayList<>(checkNotNull(buckets)));
            this.configuration =
                    Collections.unmodifiableMap(new LinkedHashMap<>(checkNotNull(configuration)));
        }

        /** Returns the ordered target buckets. */
        public List<TableBucket> buckets() {
            return buckets;
        }

        /** Returns the configuration baseline. */
        public Map<String, String> configuration() {
            return configuration;
        }
    }
}
