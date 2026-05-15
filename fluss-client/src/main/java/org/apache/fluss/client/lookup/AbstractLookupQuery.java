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

    /** Whether this lookup targets a historical partition (lake fallback path). */
    private final boolean historical;

    /**
     * The original partition name for historical partition lookups, null for realtime lookups. Used
     * for composite key encoding and lake fallback on the server side.
     */
    @Nullable private final String partitionName;

    private int retries;

    public AbstractLookupQuery(TablePath tablePath, TableBucket tableBucket, byte[] key) {
        this(tablePath, tableBucket, key, false, null);
    }

    public AbstractLookupQuery(
            TablePath tablePath,
            TableBucket tableBucket,
            byte[] key,
            boolean historical,
            @Nullable String partitionName) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.key = key;
        this.historical = historical;
        this.partitionName = partitionName;
        this.retries = 0;
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

    public int retries() {
        return retries;
    }

    @Nullable
    public String partitionName() {
        return partitionName;
    }

    /** Returns true if this lookup targets a historical partition. */
    public boolean isHistorical() {
        return historical;
    }

    public void incrementRetries() {
        retries++;
    }

    public abstract LookupType lookupType();

    public abstract CompletableFuture<T> future();
}
