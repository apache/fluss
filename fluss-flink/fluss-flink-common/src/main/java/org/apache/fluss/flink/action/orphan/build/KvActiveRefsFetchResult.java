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

package org.apache.fluss.flink.action.orphan.build;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Result of KV active-snapshot-dir fetch for one {@code (tableId, partitionId|null)} target.
 *
 * <p>Mirrors the per-target {@code listOk + listFailureReason} axis of {@link
 * LogActiveRefsFetchResult}. KV has no per-bucket failure dimension because the {@code
 * LIST_KV_SNAPSHOTS} RPC returns snapshot ids directly (no second-read of an external file), so the
 * per-bucket payload is just {@code Map<Integer, Set<String>>} of {@code snap-{id}} directory
 * names. Buckets absent from the map are treated by the consumer as "empty active set → skip".
 */
@Internal
public final class KvActiveRefsFetchResult {

    private final RpcListStatus list;
    private final Map<Integer, Set<String>> activeSnapDirsByBucket;

    private KvActiveRefsFetchResult(
            RpcListStatus list, Map<Integer, Set<String>> activeSnapDirsByBucket) {
        this.list = list;
        Map<Integer, Set<String>> copy = new HashMap<>();
        for (Map.Entry<Integer, Set<String>> e : activeSnapDirsByBucket.entrySet()) {
            copy.put(e.getKey(), Collections.unmodifiableSet(new HashSet<>(e.getValue())));
        }
        this.activeSnapDirsByBucket = Collections.unmodifiableMap(copy);
    }

    /** Result for a target whose {@code LIST_KV_SNAPSHOTS} RPC failed and exhausted retries. */
    public static KvActiveRefsFetchResult listFailed(String reason) {
        return new KvActiveRefsFetchResult(
                RpcListStatus.listFailed(reason), Collections.emptyMap());
    }

    /** Result for a target whose {@code LIST_KV_SNAPSHOTS} RPC succeeded. */
    static KvActiveRefsFetchResult ok(Map<Integer, Set<String>> activeSnapDirsByBucket) {
        return new KvActiveRefsFetchResult(RpcListStatus.ok(), activeSnapDirsByBucket);
    }

    /** Whether the per-target {@code LIST_KV_SNAPSHOTS} RPC succeeded. */
    public boolean listOk() {
        return list.isOk();
    }

    /** Reason the per-target RPC failed; {@code null} when {@link #listOk()} is true. */
    @Nullable
    public String listFailureReason() {
        return list.reason();
    }

    /**
     * Per-bucket active snapshot directory names ({@code snap-{id}}). Empty map when {@link
     * #listOk()} is false.
     *
     * <p><b>Bucket absent from the map means "the RPC returned no active-snapshot entries for this
     * bucket"</b>, which the consumer must treat as "cannot prove what is active here → skip KV
     * cleanup for this bucket and emit {@code skip_kv_bucket reason=empty_active_set}". Empty does
     * not mean "no active snapshots exist": the server enumerates buckets from ZK and that path can
     * transiently underreport (partial reads, znode creation lag, stale historical bucket counts),
     * so treating empty as no-op-skip is the only response compatible with the action's "may leak,
     * must not mis-delete" hard constraint.
     */
    public Map<Integer, Set<String>> activeSnapDirsByBucket() {
        return activeSnapDirsByBucket;
    }
}
