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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;

import javax.annotation.Nullable;

/**
 * The result of upserting a record ({@link UpsertWriter#upsert(InternalRow)}).
 *
 * @since 0.6
 */
@PublicEvolving
public class UpsertResult {

    private final @Nullable TableBucket bucket;
    private final long logEndOffset;

    /**
     * Creates a result with bucket and log end offset information.
     *
     * @param bucket the bucket this record was written to
     * @param logEndOffset the log end offset (LEO) after this write, i.e., the offset of the next
     *     record to be written
     */
    public UpsertResult(@Nullable TableBucket bucket, long logEndOffset) {
        this.bucket = bucket;
        this.logEndOffset = logEndOffset;
    }

    /**
     * Returns the bucket this record was written to.
     *
     * @return the bucket, or null if not available
     */
    @Nullable
    public TableBucket getBucket() {
        return bucket;
    }

    /**
     * Returns the log end offset (LEO) after this write. This is the offset of the next record to
     * be written to the changelog, not the offset of this specific record.
     *
     * <p>Note: When multiple records are batched together, all records in the same batch will
     * receive the same log end offset.
     *
     * @return the log end offset, or -1 if not available
     */
    public long getLogEndOffset() {
        return logEndOffset;
    }

    /** Creates an empty result (for testing purposes). */
    public static UpsertResult empty() {
        return new UpsertResult(null, -1);
    }
}
