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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.annotation.Internal;

/** Bucket scan status for log fetch. */
@Internal
class BucketScanStatus {
    private long offset; // last consumed position
    private long highWatermark; // the high watermark from last fetch
    // TODO add resetStrategy and nextAllowedRetryTimeMs.

    public BucketScanStatus() {
        this.offset = 0L;
    }

    public BucketScanStatus(Long position) {
        this.offset = position;
    }

    public long getOffset() {
        return offset;
    }

    public long getHighWatermark() {
        return highWatermark;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public void setHighWatermark(Long highWatermark) {
        this.highWatermark = highWatermark;
    }
}
