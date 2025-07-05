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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.concurrent.TimeUnit;

/**
 * This class to keep replica offset and its state(fetching or delayed). This represents a bucket as
 * being either:
 *
 * <ul>
 *   <li>Delayed, for example due to an error, where we subsequently back off a bit.
 *   <li>ReadyForFetch, the is the active state where the thread is actively fetching data.
 * </ul>
 */
public class BucketFetchStatus {
    private final long tableId;
    private final TablePath tablePath;
    private final long fetchOffset;
    private final @Nullable DelayedItem delayedItem;

    public BucketFetchStatus(
            long tableId,
            TablePath tablePath,
            long fetchOffset,
            @Nullable DelayedItem delayedItem) {
        this.tableId = tableId;
        this.tablePath = tablePath;
        this.fetchOffset = fetchOffset;
        this.delayedItem = delayedItem;
    }

    public boolean isReadyForFetch() {
        return !isDelayed();
    }

    public boolean isDelayed() {
        return delayedItem != null && delayedItem.getDelay(TimeUnit.MILLISECONDS) > 0;
    }

    public long tableId() {
        return tableId;
    }

    public TablePath tablePath() {
        return tablePath;
    }

    public long fetchOffset() {
        return fetchOffset;
    }

    @Override
    public String toString() {
        return String.format(
                "BucketFetchStatus(tableId=%s, tablePath=%s, fetchOffset=%d, delay=%s ms)",
                tableId,
                tablePath,
                fetchOffset,
                delayedItem == null ? 0 : delayedItem.getDelayMs());
    }
}
