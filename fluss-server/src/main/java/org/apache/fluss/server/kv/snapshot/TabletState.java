/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.server.kv.autoinc.AutoIncIDRange;

import javax.annotation.Nullable;

import java.util.List;

/**
 * TabletState represents the state of a kv tablet at a certain log offset. It contains the flushed
 * log offset, the row count of the tablet at that log offset, and the auto-increment ID ranges of
 * the tablet at that log offset. The row count and auto-increment ID ranges are optional, and may
 * be null if the information is not available or not needed for a particular use case.
 */
public class TabletState {

    private final long flushedLogOffset;
    @Nullable private final Long rowCount;
    @Nullable private final List<AutoIncIDRange> autoIncIDRanges;

    public TabletState(
            long flushedLogOffset,
            @Nullable Long rowCount,
            @Nullable List<AutoIncIDRange> autoIncIDRanges) {
        this.flushedLogOffset = flushedLogOffset;
        this.rowCount = rowCount;
        this.autoIncIDRanges = autoIncIDRanges;
    }

    public long getFlushedLogOffset() {
        return flushedLogOffset;
    }

    @Nullable
    public Long getRowCount() {
        return rowCount;
    }

    @Nullable
    public List<AutoIncIDRange> getAutoIncIDRanges() {
        return autoIncIDRanges;
    }

    @Override
    public String toString() {
        return "TabletState{"
                + "flushedLogOffset="
                + flushedLogOffset
                + ", rowCount="
                + rowCount
                + ", autoIncIDRanges="
                + autoIncIDRanges
                + '}';
    }
}
