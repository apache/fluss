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

package org.apache.fluss.flink.source.split;

import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/** The split for log. It's used to describe the log data of a table bucket. */
public class LogSplit extends SourceSplitBase {

    public static final long NO_STOPPING_OFFSET = LogScanner.NO_STOPPING_OFFSET;

    private static final String LOG_SPLIT_PREFIX = "log-";

    private final long startingOffset;
    private final long stoppingOffset;
    private final long backlogMarkedOffset;

    public LogSplit(TableBucket tableBucket, @Nullable String partitionName, long startingOffset) {
        this(tableBucket, partitionName, startingOffset, NO_STOPPING_OFFSET, NO_STOPPING_OFFSET);
    }

    public LogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset) {
        this(tableBucket, partitionName, startingOffset, stoppingOffset, NO_STOPPING_OFFSET);
    }

    public LogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset,
            long backlogMarkedOffset) {
        super(tableBucket, partitionName);
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        this.backlogMarkedOffset = backlogMarkedOffset;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public Optional<Long> getStoppingOffset() {
        return stoppingOffset >= 0 ? Optional.of(stoppingOffset) : Optional.empty();
    }

    public Optional<Long> getBacklogMarkedOffset() {
        return backlogMarkedOffset >= 0 ? Optional.of(backlogMarkedOffset) : Optional.empty();
    }

    @Override
    protected byte splitKind() {
        return LOG_SPLIT_FLAG;
    }

    @Override
    public String splitId() {
        return toSplitId(LOG_SPLIT_PREFIX, tableBucket);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof LogSplit)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        LogSplit that = (LogSplit) object;
        return startingOffset == that.startingOffset
                && stoppingOffset == that.stoppingOffset
                && backlogMarkedOffset == that.backlogMarkedOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), startingOffset, stoppingOffset, backlogMarkedOffset);
    }

    @Override
    public String toString() {
        return "LogSplit{"
                + "tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", startingOffset="
                + startingOffset
                + ", stoppingOffset="
                + stoppingOffset
                + ", backlogMarkedOffset="
                + backlogMarkedOffset
                + '}';
    }
}
