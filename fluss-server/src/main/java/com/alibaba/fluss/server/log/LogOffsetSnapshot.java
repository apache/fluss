/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.log;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A snapshot of the different log offsets. */
public class LogOffsetSnapshot {
    public final long logStartOffset;
    public final long localLogStartOffset;
    public final LogOffsetMetadata logEndOffset;
    public final LogOffsetMetadata highWatermark;

    public LogOffsetSnapshot(
            long logStartOffset,
            long localLogStartOffset,
            LogOffsetMetadata logEndOffset,
            LogOffsetMetadata highWatermark) {
        this.logStartOffset = logStartOffset;
        this.localLogStartOffset = localLogStartOffset;
        this.logEndOffset = logEndOffset;
        this.highWatermark = highWatermark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogOffsetSnapshot that = (LogOffsetSnapshot) o;
        return logStartOffset == that.logStartOffset
                && localLogStartOffset == that.localLogStartOffset
                && logEndOffset.equals(that.logEndOffset)
                && highWatermark.equals(that.highWatermark);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(logStartOffset);
        result = 31 * result + Long.hashCode(localLogStartOffset);
        result = 31 * result + logEndOffset.hashCode();
        result = 31 * result + highWatermark.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LogOffsetSnapshot("
                + "logStartOffset="
                + logStartOffset
                + ", localLogStartOffset="
                + localLogStartOffset
                + ", logEndOffset="
                + logEndOffset
                + ", highWatermark="
                + highWatermark
                + ')';
    }
}
