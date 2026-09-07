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
import org.apache.fluss.rpc.entity.ColumnGroupFetchResult;

import java.util.Collections;
import java.util.Map;

/** Structure used for lower level reads. */
@Internal
public class LogReadInfo {

    private final FetchDataInfo fetchedData;
    private final long highWatermark;
    private final long logEndOffset;
    private final Map<String, ColumnGroupFetchResult> columnGroups;

    public LogReadInfo(FetchDataInfo fetchedData, long highWatermark, long logEndOffset) {
        this(fetchedData, highWatermark, logEndOffset, Collections.emptyMap());
    }

    public LogReadInfo(
            FetchDataInfo fetchedData,
            long highWatermark,
            long logEndOffset,
            Map<String, ColumnGroupFetchResult> columnGroups) {
        this.fetchedData = fetchedData;
        this.highWatermark = highWatermark;
        this.logEndOffset = logEndOffset;
        this.columnGroups = columnGroups;
    }

    /** Column-group records covering the fetched base range (FIP-45), keyed by group name. */
    public Map<String, ColumnGroupFetchResult> getColumnGroups() {
        return columnGroups;
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

    @Override
    public String toString() {
        return "LogReadInfo("
                + "fetchedData="
                + fetchedData
                + ", highWatermark="
                + highWatermark
                + ", logEndOffset="
                + logEndOffset
                + ')';
    }
}
