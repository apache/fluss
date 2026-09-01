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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.server.kv.historical.HistoricalKvKeyEncoder;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Accesses a KV tablet's local prewrite buffer and RocksDB state. */
@Internal
public final class KvStateAccessor {

    /** Encoded RocksDB value marking a deleted key in historical KV state. */
    static final byte[] HISTORICAL_TOMBSTONE = new byte[0];

    private final KvPreWriteBuffer preWriteBuffer;
    private final RocksDBKv rocksDBKv;
    private final boolean historicalPartition;

    KvStateAccessor(
            KvPreWriteBuffer preWriteBuffer, RocksDBKv rocksDBKv, boolean historicalPartition) {
        this.preWriteBuffer = preWriteBuffer;
        this.rocksDBKv = rocksDBKv;
        this.historicalPartition = historicalPartition;
    }

    /**
     * Encodes the logical primary key into the physical key used by this state.
     *
     * <p>Normal KV state keeps the primary key unchanged. Historical KV state also encodes the
     * original partition context because multiple original partitions share one historical KV
     * tablet. {@code originalPartitionName} is null for normal state and must be present for
     * historical state.
     */
    public Key encodeKey(byte[] primaryKey, @Nullable String originalPartitionName) {
        if (!historicalPartition) {
            checkArgument(
                    originalPartitionName == null,
                    "A normal KV state key must not carry an original partition name");
            return Key.of(primaryKey);
        }

        String partitionName =
                checkNotNull(originalPartitionName, "originalPartitionName must not be null");
        checkArgument(!partitionName.isEmpty(), "originalPartitionName must not be empty");
        return Key.of(HistoricalKvKeyEncoder.encode(partitionName, primaryKey));
    }

    /** Looks up an encoded key from the local prewrite buffer and RocksDB state. */
    public KvStateLookupResult lookup(Key key) throws IOException {
        KvPreWriteBuffer.Value bufferedValue = preWriteBuffer.get(key);
        if (bufferedValue != null) {
            byte[] value = bufferedValue.get();
            return value == null
                    ? KvStateLookupResult.deleted()
                    : KvStateLookupResult.present(value);
        }

        byte[] value = rocksDBKv.get(key.get());
        if (value == null) {
            return KvStateLookupResult.notFound();
        }
        // Historical KV tablets persist deletes as empty values so that a local miss does not
        // expose a stale value from lake storage after the buffered delete has been flushed.
        return value.length == 0
                ? KvStateLookupResult.deleted()
                : KvStateLookupResult.present(value);
    }

    /** Adds an insert mutation to the prewrite buffer. */
    public void insert(Key key, byte[] value, long logOffset) {
        if (historicalPartition) {
            checkArgument(value.length > 0, "Historical KV insert value must not be empty");
        }
        preWriteBuffer.insert(key, value, logOffset);
    }

    /** Adds an update mutation to the prewrite buffer. */
    public void update(Key key, @Nullable byte[] value, long logOffset) {
        if (historicalPartition) {
            checkNotNull(value, "Historical KV update value must not be null");
            checkArgument(value.length > 0, "Historical KV update value must not be empty");
        }
        preWriteBuffer.update(key, value, logOffset);
    }

    /** Adds a delete mutation to the prewrite buffer. */
    public void delete(Key key, long logOffset) {
        preWriteBuffer.delete(key, logOffset);
    }

    /** Truncates pending mutations to the given log offset. */
    public void truncateTo(long logOffset, TruncateReason reason) {
        preWriteBuffer.truncateTo(logOffset, reason);
    }
}
