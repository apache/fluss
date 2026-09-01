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

package org.apache.fluss.server.kv.historical;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.server.kv.KvStateLookupResult;
import org.apache.fluss.utils.ByteArrayWrapper;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Request-scoped previous values used by a historical write batch.
 *
 * <p>A local hit is decoded and stored immediately. A local miss enters {@link #pendingLakeLookups}
 * and moves to {@link #resolvedValues} when its lake result arrives. Before apply, {@link
 * #ensureFullyResolved()} verifies that no lake lookup remains pending. Apply can then retrieve the
 * decoded previous value directly without probing local storage again.
 */
@Internal
@NotThreadSafe
public final class HistoricalWritePreviousValues {

    /** Decodes tagged values returned by local historical KV state. */
    private final ValueDecoder localValueDecoder;

    /** Decodes plain values returned by lake storage. */
    private final ValueDecoder lakeValueDecoder;

    /**
     * Decoded previous value for each completed lookup. {@link Optional#empty()} means the lookup
     * completed and confirmed that the key was absent.
     */
    private final Map<ByteArrayWrapper, Optional<BinaryValue>> resolvedValues = new HashMap<>();

    /**
     * Local misses still waiting for lake results. Insertion order defines the lake request order;
     * resolving a key removes it from this map, so an empty map also means resolution is complete.
     */
    private final Map<ByteArrayWrapper, byte[]> pendingLakeLookups = new LinkedHashMap<>();

    /** Creates previous-value storage with decoders for the local and lake layouts. */
    public HistoricalWritePreviousValues(
            ValueDecoder localValueDecoder, ValueDecoder lakeValueDecoder) {
        this.localValueDecoder = localValueDecoder;
        this.lakeValueDecoder = lakeValueDecoder;
    }

    /** Records the first local probe result for a key that requires its previous value. */
    public void addLocalResult(byte[] primaryKey, KvStateLookupResult localResult) {
        ByteArrayWrapper wrappedKey = new ByteArrayWrapper(primaryKey);
        checkState(
                !resolvedValues.containsKey(wrappedKey)
                        && !pendingLakeLookups.containsKey(wrappedKey),
                "Historical write key has already been probed");

        if (localResult.status() == KvStateLookupResult.Status.NOT_FOUND) {
            pendingLakeLookups.put(wrappedKey, primaryKey);
        } else {
            resolvedValues.put(wrappedKey, decode(localResult.value(), localValueDecoder));
        }
    }

    /** Returns a snapshot of pending local misses in lake request order. */
    public List<byte[]> keysRequiringLakeLookup() {
        return Collections.unmodifiableList(new ArrayList<>(pendingLakeLookups.values()));
    }

    /** Decodes and stores the lake result for one pending local miss. */
    public void resolveLakeValue(byte[] primaryKey, @Nullable byte[] lakeValue) {
        checkArgument(
                lakeValue == null || lakeValue.length > 0,
                "Historical lake value must not be empty");
        ByteArrayWrapper wrappedKey = new ByteArrayWrapper(primaryKey);
        checkState(
                pendingLakeLookups.remove(wrappedKey) != null,
                "Historical write key does not require lake lookup");
        resolvedValues.put(wrappedKey, decode(lakeValue, lakeValueDecoder));
    }

    /** Verifies that every local miss has been resolved before the write lock is acquired. */
    public void ensureFullyResolved() {
        checkState(
                pendingLakeLookups.isEmpty(),
                "Historical write has %s pending lake lookups",
                pendingLakeLookups.size());
    }

    /** Returns the decoded previous value, or null when the key was resolved as absent. */
    public @Nullable BinaryValue get(byte[] primaryKey) {
        Optional<BinaryValue> previousValue =
                checkNotNull(
                        resolvedValues.get(new ByteArrayWrapper(primaryKey)),
                        "No resolved previous value for a historical write key");
        return previousValue.orElse(null);
    }

    private static Optional<BinaryValue> decode(
            @Nullable byte[] encodedValue, ValueDecoder valueDecoder) {
        return encodedValue == null
                ? Optional.empty()
                : Optional.of(valueDecoder.decodeValue(encodedValue));
    }
}
