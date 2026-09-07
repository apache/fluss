/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.log;

import org.apache.fluss.annotation.Internal;

/** Result of appending a batch of column-group rows to a {@link ColumnGroupLog}. */
@Internal
public final class ColumnGroupAppendInfo {
    private final long firstOffset;
    private final long lastOffset;
    private final int rowCount;
    private final boolean duplicated;

    public ColumnGroupAppendInfo(
            long firstOffset, long lastOffset, int rowCount, boolean duplicated) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.rowCount = rowCount;
        this.duplicated = duplicated;
    }

    public static ColumnGroupAppendInfo duplicated(long firstOffset, long lastOffset) {
        return new ColumnGroupAppendInfo(
                firstOffset, lastOffset, (int) (lastOffset - firstOffset + 1), true);
    }

    public long firstOffset() {
        return firstOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public int rowCount() {
        return rowCount;
    }

    /** True when every row of the batch was already filled and the append was skipped. */
    public boolean isDuplicated() {
        return duplicated;
    }

    @Override
    public String toString() {
        return "ColumnGroupAppendInfo("
                + "firstOffset="
                + firstOffset
                + ", lastOffset="
                + lastOffset
                + ", rowCount="
                + rowCount
                + ", duplicated="
                + duplicated
                + ')';
    }
}
