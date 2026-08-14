/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.server.kv.historical;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.server.kv.KvStateAccessor;
import org.apache.fluss.server.kv.KvStateLookupResult;
import org.apache.fluss.server.kv.KvStateLookupResult.Status;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A partition-scoped view over the shared local state of a historical KV tablet.
 *
 * <p>One historical tablet can contain writes for multiple original partitions. This accessor
 * namespaces every physical primary key with {@code originalPartitionName} before delegating to the
 * local accessor. For merge reads, the local state is authoritative when it contains either a value
 * or a tombstone; the external fallback is consulted only when the key is absent locally.
 */
@Internal
public final class HistoricalKvStateAccessor implements KvStateAccessor {

    private final KvStateAccessor localAccessor;
    private final String originalPartitionName;
    private final @Nullable HistoricalValueLookup fallbackLookup;

    /**
     * Creates an accessor with an optional external fallback for local misses.
     *
     * <p>The fallback receives the original, un-namespaced primary key expected by lake storage.
     */
    public HistoricalKvStateAccessor(
            KvStateAccessor localAccessor,
            String originalPartitionName,
            @Nullable HistoricalValueLookup fallbackLookup) {
        this.localAccessor = checkNotNull(localAccessor, "localAccessor must not be null");
        this.originalPartitionName =
                checkNotNull(originalPartitionName, "originalPartitionName must not be null");
        checkArgument(!originalPartitionName.isEmpty(), "originalPartitionName must not be empty");
        this.fallbackLookup = fallbackLookup;
    }

    @Override
    public Key encodeKey(byte[] primaryKey) {
        return Key.of(HistoricalKvKeyEncoder.encode(originalPartitionName, primaryKey));
    }

    /**
     * Looks up the local overlay first and falls back only when no local state exists.
     *
     * <p>A local tombstone is a definitive result. Falling back after a local delete could expose
     * the value that still exists in an older lake snapshot.
     */
    @Override
    public KvStateLookupResult lookup(Key encodedPrimaryKey) throws Exception {
        KvStateLookupResult localResult = localAccessor.lookup(encodedPrimaryKey);
        if (localResult.status() != Status.NOT_FOUND || fallbackLookup == null) {
            return localResult;
        }

        byte[] fallbackValue =
                fallbackLookup.lookup(
                        HistoricalKvKeyEncoder.extractOriginalPrimaryKey(encodedPrimaryKey.get()));
        return fallbackValue == null
                ? KvStateLookupResult.notFound()
                : KvStateLookupResult.present(fallbackValue);
    }

    @Override
    public void insert(Key key, byte[] value, long logOffset) {
        checkArgument(value.length > 0, "Historical KV insert value must not be empty");
        localAccessor.insert(key, value, logOffset);
    }

    @Override
    public void update(Key key, @Nullable byte[] value, long logOffset) {
        checkNotNull(value, "Historical KV update value must not be null");
        checkArgument(value.length > 0, "Historical KV update value must not be empty");
        localAccessor.update(key, value, logOffset);
    }

    @Override
    public void delete(Key key, long logOffset) {
        localAccessor.delete(key, logOffset);
    }

    @Override
    public void truncateTo(long logOffset, TruncateReason reason) {
        localAccessor.truncateTo(logOffset, reason);
    }
}
