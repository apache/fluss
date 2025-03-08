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

package com.alibaba.fluss.server.log;

/**
 * The mapping between a logical log offset and the physical position in some log file of the
 * beginning of the message set entry with the given offset.
 */
public final class OffsetPosition implements IndexEntry {
    private final long offset;
    private final int position;

    public OffsetPosition(long offset, int position) {
        this.offset = offset;
        this.position = position;
    }

    public long getOffset() {
        return offset;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public long indexKey() {
        return offset;
    }

    @Override
    public long indexValue() {
        return position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OffsetPosition that = (OffsetPosition) o;

        return offset == that.offset && position == that.position;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + position;
        return result;
    }

    @Override
    public String toString() {
        return "OffsetPosition(" + "offset=" + offset + ", position=" + position + ')';
    }
}
