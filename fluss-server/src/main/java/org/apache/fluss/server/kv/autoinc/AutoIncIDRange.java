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

package org.apache.fluss.server.kv.autoinc;

import java.util.Objects;

/**
 * Represents a range of IDs allocated for auto-increment purposes. This class encapsulates the
 * auto-increment column id, the start and end of the ID range. The range is [start, end]. There is
 * possible that the start > end which means the range is empty.
 */
public class AutoIncIDRange {

    private final int columnId;
    private final long start;
    private final long end;

    public AutoIncIDRange(int columnId, long start, long end) {
        this.columnId = columnId;
        this.start = start;
        this.end = end;
    }

    /**
     * Returns the column ID of the auto-increment column that associated with this auto-increment
     * ID range.
     */
    public int getColumnId() {
        return columnId;
    }

    /** Returns the starting ID of the range (inclusive). */
    public long getStart() {
        return start;
    }

    /** Returns the ending ID of the range (inclusive). */
    public long getEnd() {
        return end;
    }

    /** Checks if the ID range is empty (i.e., start is greater than end). */
    public boolean isEmpty() {
        return start > end;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AutoIncIDRange autoIncIdRange = (AutoIncIDRange) o;
        return columnId == autoIncIdRange.getColumnId()
                && start == autoIncIdRange.start
                && end == autoIncIdRange.end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnId, start, end);
    }

    @Override
    public String toString() {
        return "IDRange{" + "columnId=" + columnId + ", start=" + start + ", end=" + end + '}';
    }
}
