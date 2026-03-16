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

package org.apache.fluss.lake.committer;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Immutable statistics for a single completed tiering round of a lake table.
 *
 * <p>Fields use {@code null} to represent "unknown / not supported".
 */
public final class TieringStats implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * A {@code TieringStats} instance where every field is {@code null} (unknown/unsupported). Use
     * this as the default when no stats are available.
     */
    public static final TieringStats UNKNOWN = new TieringStats(null, null);

    // -----------------------------------------------------------------------------------------
    // Lake data stats (reported by the lake committer)
    // -----------------------------------------------------------------------------------------

    /** Cumulative total file size (bytes) of the lake table after this tiering round. */
    @Nullable private final Long fileSize;

    /** Cumulative total record count of the lake table after this tiering round. */
    @Nullable private final Long recordCount;

    public TieringStats(@Nullable Long fileSize, @Nullable Long recordCount) {
        this.fileSize = fileSize;
        this.recordCount = recordCount;
    }

    @Nullable
    public Long getFileSize() {
        return fileSize;
    }

    @Nullable
    public Long getRecordCount() {
        return recordCount;
    }

    /**
     * Returns {@code true} when at least one stat field is non-{@code null}, meaning actual data
     * was written during this tiering round.
     */
    public boolean isAvailableStats() {
        return fileSize != null || recordCount != null;
    }

    @Override
    public String toString() {
        return "TieringStats{" + "fileSize=" + fileSize + ", recordCount=" + recordCount + '}';
    }
}
