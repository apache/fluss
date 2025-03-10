/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.source.split;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Objects;

/** The split for log. It's used to describe the log data of a table bucket. */
public class LogSplit extends SourceSplitBase {

    private static final String LOG_SPLIT_PREFIX = "log-";

    private final long startingOffset;

    public LogSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset) {
        super(tablePath, tableBucket, partitionName);
        this.startingOffset = startingOffset;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LogSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogSplit logSplit = (LogSplit) o;
        return startingOffset == logSplit.startingOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), startingOffset);
    }

    @Override
    public String toString() {
        return "LogSplit{"
                + "startingOffset="
                + startingOffset
                + ", tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + '}';
    }

    @Override
    public String splitId() {
        return toSplitId(LOG_SPLIT_PREFIX, tableBucket);
    }
}
