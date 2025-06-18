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

package com.alibaba.fluss.server.log;

/**
 * Struct to hold various quantities we compute about each message set before appending to the log.
 */
public final class LogAppendInfo {

    /** The number of validated records. */
    private final int shallowCount;

    /** The total bytes of records after validate. */
    private final int validBytes;

    private final boolean offsetsMonotonic;
    private final String errorMessage;

    private long firstOffset;
    private long lastOffset;

    private long maxTimestamp;
    private long startOffsetOfMaxTimestamp;

    /** Whether the appended log data is duplicated. */
    private boolean duplicated;

    /** Creates an instance with the given params. */
    public LogAppendInfo(
            long firstOffset,
            long lastOffset,
            long maxTimestamp,
            long startOffsetOfMaxTimestamp,
            int shallowCount,
            int validBytes,
            boolean offsetsMonotonic) {
        this(
                firstOffset,
                lastOffset,
                maxTimestamp,
                startOffsetOfMaxTimestamp,
                shallowCount,
                validBytes,
                offsetsMonotonic,
                null);
    }

    /** Creates an instance with the given params. */
    public LogAppendInfo(
            long firstOffset,
            long lastOffset,
            long maxTimestamp,
            long startOffsetOfMaxTimestamp,
            int shallowCount,
            int validBytes,
            boolean offsetsMonotonic,
            String errorMessage) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.maxTimestamp = maxTimestamp;
        this.startOffsetOfMaxTimestamp = startOffsetOfMaxTimestamp;
        this.shallowCount = shallowCount;
        this.validBytes = validBytes;
        this.offsetsMonotonic = offsetsMonotonic;
        this.errorMessage = errorMessage;
        this.duplicated = false;
    }

    public long firstOffset() {
        return firstOffset;
    }

    public void setFirstOffset(long firstOffset) {
        this.firstOffset = firstOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

    /** Return the number of validated records. */
    public int shallowCount() {
        return shallowCount;
    }

    public int validBytes() {
        return validBytes;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public long startOffsetOfMaxTimestamp() {
        return startOffsetOfMaxTimestamp;
    }

    public void setStartOffsetOfMaxTimestamp(long startOffsetOfMaxTimestamp) {
        this.startOffsetOfMaxTimestamp = startOffsetOfMaxTimestamp;
    }

    public boolean duplicated() {
        return duplicated;
    }

    public void setDuplicated(boolean duplicated) {
        this.duplicated = duplicated;
    }

    public boolean offsetsMonotonic() {
        return offsetsMonotonic;
    }

    /**
     * Get the (maximum) number of messages described by LogAppendInfo.
     *
     * @return Maximum possible number of messages described by LogAppendInfo
     */
    public long numMessages() {
        if (firstOffset >= 0 && lastOffset >= 0) {
            return lastOffset - firstOffset + 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "LogAppendInfo("
                + "firstOffset="
                + firstOffset
                + ", lastOffset="
                + lastOffset
                + ", maxTimestamp="
                + maxTimestamp
                + ", startOffsetOfMaxTimestamp="
                + startOffsetOfMaxTimestamp
                + ", shallowCount="
                + shallowCount
                + ", validBytes="
                + validBytes
                + ", offsetsMonotonic="
                + offsetsMonotonic
                + ", duplicated="
                + duplicated
                + ", recordErrors="
                + errorMessage
                + ')';
    }
}
