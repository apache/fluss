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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** Abstract Class to represent a lookup operation. */
@Internal
public abstract class AbstractLookupQuery<T> {

    private final TablePath tablePath;
    private final TableBucket tableBucket;
    private final byte[] key;

    /**
     * Null for normal and prefix lookups. Historical lookups use this to carry the original
     * partition name.
     */
    private final @Nullable String partitionName;

    private int retries;
    private long nextRetryTimeMs;

    public AbstractLookupQuery(TablePath tablePath, TableBucket tableBucket, byte[] key) {
        this(tablePath, tableBucket, key, null);
    }

    public AbstractLookupQuery(
            TablePath tablePath,
            TableBucket tableBucket,
            byte[] key,
            @Nullable String partitionName) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.key = key;
        this.partitionName = partitionName;
        this.retries = 0;
        this.nextRetryTimeMs = 0;
    }

    public byte[] key() {
        return key;
    }

    public TablePath tablePath() {
        return tablePath;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    public @Nullable String partitionName() {
        return partitionName;
    }

    public int retries() {
        return retries;
    }

    public void incrementRetries() {
        retries++;
    }

    public long nextRetryTimeMs() {
        return nextRetryTimeMs;
    }

    public void setNextRetryTimeMs(long nextRetryTimeMs) {
        this.nextRetryTimeMs = nextRetryTimeMs;
    }

    public abstract LookupType lookupType();

    public abstract CompletableFuture<T> future();
}
