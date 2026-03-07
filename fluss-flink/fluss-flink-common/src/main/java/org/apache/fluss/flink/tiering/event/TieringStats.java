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

package org.apache.fluss.flink.tiering.event;

import java.io.Serializable;

/**
 * Immutable statistics for a single completed tiering round of a lake table.
 *
 * <p>All long fields use {@code -1} as the sentinel value meaning "unknown / not supported".
 */
public final class TieringStats implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * A {@code TieringStats} instance where every field is {@code -1} (unknown/unsupported). Use
     * this as the default when no stats are available.
     */
    public static final TieringStats UNKNOWN = new TieringStats(-1L, -1L);

    // -----------------------------------------------------------------------------------------
    // Lake data stats (reported by the lake committer)
    // -----------------------------------------------------------------------------------------

    /** Cumulative total file size (bytes) of the lake table after this tiering round. */
    private final long fileSize;

    /** Cumulative total record count of the lake table after this tiering round. */
    private final long recordCount;

    public TieringStats(long fileSize, long recordCount) {
        this.fileSize = fileSize;
        this.recordCount = recordCount;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getRecordCount() {
        return recordCount;
    }

    @Override
    public String toString() {
        return "TieringStats{" + "fileSize=" + fileSize + ", recordCount=" + recordCount + '}';
    }
}
