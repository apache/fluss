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

package com.alibaba.fluss.server.log.remote;

import com.alibaba.fluss.remote.RemoteLogSegment;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * This represents all the required data and indexed for a specific log segment that needs to be
 * stored in the tiered storage. is passed with {@link
 * RemoteLogStorage#copyLogSegmentFiles(RemoteLogSegment, LogSegmentFiles)} while copying a specific
 * log segment to the tiered storage.
 */
public class LogSegmentFiles {

    private final Path logSegment;
    private final Path offsetIndex;
    private final Path timeIndex;
    private final @Nullable Path writerIdIndex;
    // TODO add leader epoch index after introduce leader epoch.

    public LogSegmentFiles(
            Path logSegment, Path offsetIndex, Path timeIndex, @Nullable Path writerIdIndex) {
        this.logSegment = checkNotNull(logSegment, "logSegment can not be null");
        this.offsetIndex = checkNotNull(offsetIndex, "offsetIndex can not be null");
        this.timeIndex = checkNotNull(timeIndex, "timeIndex can not be null");
        this.writerIdIndex = writerIdIndex;
    }

    public Path logSegment() {
        return logSegment;
    }

    public Path offsetIndex() {
        return offsetIndex;
    }

    public Path timeIndex() {
        return timeIndex;
    }

    public @Nullable Path writerIdIndex() {
        return writerIdIndex;
    }

    public List<Path> getAllPaths() {
        return Arrays.asList(logSegment, offsetIndex, timeIndex, writerIdIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogSegmentFiles that = (LogSegmentFiles) o;
        return Objects.equals(logSegment, that.logSegment)
                && Objects.equals(offsetIndex, that.offsetIndex)
                && Objects.equals(timeIndex, that.timeIndex)
                && Objects.equals(writerIdIndex, that.writerIdIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logSegment, offsetIndex, timeIndex, writerIdIndex);
    }

    @Override
    public String toString() {
        return "LogSegmentData{"
                + "logSegment="
                + logSegment
                + ", offsetIndex="
                + offsetIndex
                + ", timeIndex="
                + timeIndex
                + ", writerIdIndex="
                + writerIdIndex
                + '}';
    }
}
