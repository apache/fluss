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

package org.apache.fluss.rpc.entity;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.record.LogRecords;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The records of one column group returned by a fetch (FIP-45). The batches are standard log record
 * batches whose base offsets are base-log offsets, covering at least the offset range of the base
 * records returned in the same fetch, so a reader can stitch them onto the base rows by offset.
 */
@Internal
public final class ColumnGroupFetchResult {
    private final String groupName;
    private final long highWatermark;
    private final LogRecords records;

    public ColumnGroupFetchResult(String groupName, long highWatermark, LogRecords records) {
        this.groupName = checkNotNull(groupName, "groupName");
        this.highWatermark = highWatermark;
        this.records = checkNotNull(records, "records");
    }

    public String getGroupName() {
        return groupName;
    }

    /** The column group's high watermark (committed enrichment watermark). */
    public long getHighWatermark() {
        return highWatermark;
    }

    public LogRecords getRecords() {
        return records;
    }
}
