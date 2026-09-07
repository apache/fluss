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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * The result of {@link AppendWriter#appendColumns}: the state of the column group on the bucket
 * after the rows were appended.
 *
 * @since 0.9
 */
@PublicEvolving
public final class AppendColumnsResult {
    private final long logEndOffset;
    private final long highWatermark;

    public AppendColumnsResult(long logEndOffset, long highWatermark) {
        this.logEndOffset = logEndOffset;
        this.highWatermark = highWatermark;
    }

    /**
     * The enrichment watermark of the column group on the bucket: the exclusive base-log offset up
     * to which the group is contiguously filled on the leader.
     */
    public long getLogEndOffset() {
        return logEndOffset;
    }

    /**
     * The committed enrichment watermark of the column group on the bucket: the exclusive base-log
     * offset up to which the group is filled on every in-sync replica. Readers projecting the group
     * never see rows at or beyond it.
     */
    public long getHighWatermark() {
        return highWatermark;
    }

    @Override
    public String toString() {
        return "AppendColumnsResult{"
                + "logEndOffset="
                + logEndOffset
                + ", highWatermark="
                + highWatermark
                + '}';
    }
}
