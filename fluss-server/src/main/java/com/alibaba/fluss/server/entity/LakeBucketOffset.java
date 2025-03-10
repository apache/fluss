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

package com.alibaba.fluss.server.entity;

import java.util.Optional;

/** The offset for a specific bucket in lake. */
public class LakeBucketOffset {
    private final long snapshotId;

    private final Long logStartOffset;
    private final Long logEndOffset;

    public LakeBucketOffset(long snapshotId, Long logStartOffset, Long logEndOffset) {
        this.snapshotId = snapshotId;
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public Optional<Long> getLogStartOffset() {
        return Optional.ofNullable(logStartOffset);
    }

    public Optional<Long> getLogEndOffset() {
        return Optional.ofNullable(logEndOffset);
    }
}
