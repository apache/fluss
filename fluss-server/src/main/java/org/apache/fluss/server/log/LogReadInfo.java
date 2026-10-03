/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.log;

import org.apache.fluss.annotation.Internal;

/** Structure used for lower level reads. */
@Internal
public class LogReadInfo {
    public static final long NO_RESOLVED_EARLIEST_OFFSET = -1L;

    private final FetchDataInfo fetchedData;
    private final long highWatermark;
    private final long logEndOffset;
    private final long minRetainOffset;
    private final long resolvedEarliestOffset;

    public LogReadInfo(
            FetchDataInfo fetchedData,
            long highWatermark,
            long logEndOffset,
            long minRetainOffset) {
        this(
                fetchedData,
                highWatermark,
                logEndOffset,
                minRetainOffset,
                NO_RESOLVED_EARLIEST_OFFSET);
    }

    public LogReadInfo(
            FetchDataInfo fetchedData,
            long highWatermark,
            long logEndOffset,
            long minRetainOffset,
            long resolvedEarliestOffset) {
        this.fetchedData = fetchedData;
        this.highWatermark = highWatermark;
        this.logEndOffset = logEndOffset;
        this.minRetainOffset = minRetainOffset;
        this.resolvedEarliestOffset = resolvedEarliestOffset;
    }

    /**
     * Returns the physical log offset resolved from an EARLIEST fetch request, or {@link
     * #NO_RESOLVED_EARLIEST_OFFSET} if this read did not resolve EARLIEST.
     */
    public long getResolvedEarliestOffset() {
        return resolvedEarliestOffset;
    }

    public FetchDataInfo getFetchedData() {
        return fetchedData;
    }

    public long getHighWatermark() {
        return highWatermark;
    }

    public long getLogEndOffset() {
        return logEndOffset;
    }

    public boolean hasMinRetainOffset() {
        return minRetainOffset >= 0;
    }

    public long getMinRetainOffset() {
        return minRetainOffset;
    }

    @Override
    public String toString() {
        return "LogReadInfo("
                + "fetchedData="
                + fetchedData
                + ", highWatermark="
                + highWatermark
                + ", logEndOffset="
                + logEndOffset
                + ", minRetainOffset="
                + minRetainOffset
                + ", resolvedEarliestOffset="
                + resolvedEarliestOffset
                + ')';
    }
}
