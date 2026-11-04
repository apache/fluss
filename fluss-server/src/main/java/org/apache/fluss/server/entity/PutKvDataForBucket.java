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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.KvRecordBatch;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Put KV request data and historical partition context for one table bucket. */
public final class PutKvDataForBucket {

    private final TableBucket tableBucket;
    private final KvRecordBatch records;
    private final @Nullable String originalPartitionName;

    /** Creates decoded put-KV data for one table bucket. */
    public PutKvDataForBucket(
            TableBucket tableBucket,
            KvRecordBatch records,
            @Nullable String originalPartitionName) {
        this.tableBucket = checkNotNull(tableBucket, "tableBucket must not be null.");
        this.records = checkNotNull(records, "records must not be null.");
        this.originalPartitionName = originalPartitionName;
    }

    /** Returns the physical table bucket targeted by this request data. */
    public TableBucket tableBucket() {
        return tableBucket;
    }

    /** Returns the encoded KV records for this table bucket. */
    public KvRecordBatch records() {
        return records;
    }

    /** Returns the original partition name for a historical write, or null. */
    public @Nullable String originalPartitionName() {
        return originalPartitionName;
    }
}
