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

package com.alibaba.fluss.flink.source.split;

/** State of the reader, essentially a mutable version of the {@link SourceSplitBase}. */
public abstract class SourceSplitState {

    protected final SourceSplitBase split;

    public SourceSplitState(SourceSplitBase split) {
        this.split = split;
    }

    /** Checks whether this split state is a hybrid snapshot log split state. */
    public final boolean isHybridSnapshotLogSplitState() {
        return getClass() == HybridSnapshotLogSplitState.class;
    }

    /** Checks whether this split state is a log split state. */
    public final boolean isLogSplitState() {
        return getClass() == LogSplitState.class;
    }

    /** Casts this split state into a {@link HybridSnapshotLogSplitState}. */
    public final HybridSnapshotLogSplitState asHybridSnapshotLogSplitState() {
        return (HybridSnapshotLogSplitState) this;
    }

    /** Casts this split state into a {@link LogSplitState}. */
    public final LogSplitState asLogSplitState() {
        return (LogSplitState) this;
    }

    public abstract SourceSplitBase toSourceSplit();

    public boolean isLakeSplit() {
        return split.isLakeSplit();
    }
}
