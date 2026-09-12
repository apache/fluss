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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Local lookup results for previous values required by a historical write request. */
@Internal
@NotThreadSafe
public final class LocalValueLookupResult {

    /** Decodes tagged values returned by local historical KV state. */
    private final ValueDecoder localValueDecoder;

    /** Decodes plain values returned by lake storage. */
    private final ValueDecoder lakeValueDecoder;

    /** Previous values found locally, including tombstones represented by an empty optional. */
    private final Map<ByteArrayWrapper, Optional<BinaryValue>> localValuesByKey = new HashMap<>();

    /** True local misses in lake request order. */
    private final Set<ByteArrayWrapper> keysMissingLocally = new LinkedHashSet<>();

    /** Creates a local lookup result with decoders for the local and lake value layouts. */
    public LocalValueLookupResult(ValueDecoder localValueDecoder, ValueDecoder lakeValueDecoder) {
        this.localValueDecoder = localValueDecoder;
        this.lakeValueDecoder = lakeValueDecoder;
    }

    /** Records the local result for one key whose previous value is required. */
    public void add(byte[] primaryKey, KvStateLookupResult localResult) {
        ByteArrayWrapper wrappedKey = new ByteArrayWrapper(primaryKey);
        checkState(
                !localValuesByKey.containsKey(wrappedKey)
                        && !keysMissingLocally.contains(wrappedKey),
                "Historical write key has already been probed");

        if (localResult.status() == KvStateLookupResult.Status.NOT_FOUND) {
            keysMissingLocally.add(wrappedKey);
        } else {
            localValuesByKey.put(wrappedKey, decode(localResult.value(), localValueDecoder));
        }
    }

    /** Returns a snapshot of true local misses in lake request order. */
    public List<byte[]> keysMissingLocally() {
        List<byte[]> primaryKeys = new ArrayList<>(keysMissingLocally.size());
        for (ByteArrayWrapper keyMissingLocally : keysMissingLocally) {
            primaryKeys.add(keyMissingLocally.getData());
        }
        return Collections.unmodifiableList(primaryKeys);
    }

    /**
     * Combines lake results with the local lookup results and creates an in-memory lookup for
     * apply.
     *
     * <p>Lake values must have the same order as {@link #keysMissingLocally()}.
     */
    public HistoricalValueLookup createValueLookup(List<byte[]> lakeValues) {
        checkNotNull(lakeValues, "Historical lake values must not be null");
        checkArgument(
                lakeValues.size() == keysMissingLocally.size(),
                "Expected %s historical lake values, but received %s",
                keysMissingLocally.size(),
                lakeValues.size());
        Map<ByteArrayWrapper, Optional<BinaryValue>> previousValuesByKey =
                new HashMap<>(localValuesByKey);
        Iterator<ByteArrayWrapper> missingKeyIterator = keysMissingLocally.iterator();
        for (byte[] lakeValue : lakeValues) {
            previousValuesByKey.put(missingKeyIterator.next(), decode(lakeValue, lakeValueDecoder));
        }
        return primaryKey ->
                checkNotNull(
                                previousValuesByKey.get(new ByteArrayWrapper(primaryKey)),
                                "No previous value for a historical write key")
                        .orElse(null);
    }

    private static Optional<BinaryValue> decode(
            @Nullable byte[] encodedValue, ValueDecoder valueDecoder) {
        return encodedValue == null
                ? Optional.empty()
                : Optional.of(valueDecoder.decodeValue(encodedValue));
    }
}
