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

package org.apache.fluss.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static io.airlift.slice.SizeOf.instanceSize;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Immutable range of Fluss log offsets. */
public final class FlussLogRange {
    private static final int INSTANCE_SIZE = instanceSize(FlussLogRange.class);

    private final long startOffset;
    private final long stoppingOffset;

    public FlussLogRange(long startOffset, long stoppingOffset) {
        checkArgument(startOffset >= 0, "startOffset must be non-negative");
        checkArgument(stoppingOffset >= startOffset, "stoppingOffset must not precede startOffset");
        this.startOffset = startOffset;
        this.stoppingOffset = stoppingOffset;
    }

    /** Creates a log offset range from JSON with both offsets required. */
    @JsonCreator
    public static FlussLogRange fromJson(
            @JsonProperty("startOffset") Long startOffset,
            @JsonProperty("stoppingOffset") Long stoppingOffset) {
        checkArgument(startOffset != null, "startOffset is required");
        checkArgument(stoppingOffset != null, "stoppingOffset is required");
        return new FlussLogRange(startOffset, stoppingOffset);
    }

    @JsonProperty
    public long getStartOffset() {
        return startOffset;
    }

    @JsonProperty
    public long getStoppingOffset() {
        return stoppingOffset;
    }

    boolean isEmpty() {
        return startOffset == stoppingOffset;
    }

    long getRetainedSizeInBytes() {
        return INSTANCE_SIZE;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlussLogRange that = (FlussLogRange) o;
        return startOffset == that.startOffset && stoppingOffset == that.stoppingOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startOffset, stoppingOffset);
    }

    @Override
    public String toString() {
        return "[" + startOffset + "," + stoppingOffset + ")";
    }
}
