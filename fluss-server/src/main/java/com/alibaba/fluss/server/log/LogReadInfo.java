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

import com.alibaba.fluss.annotation.Internal;

/** Structure used for lower level reads. */
@Internal
public class LogReadInfo {

    private final FetchDataInfo fetchedData;
    private final long highWatermark;
    private final long logEndOffset;

    public LogReadInfo(FetchDataInfo fetchedData, long highWatermark, long logEndOffset) {
        this.fetchedData = fetchedData;
        this.highWatermark = highWatermark;
        this.logEndOffset = logEndOffset;
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
