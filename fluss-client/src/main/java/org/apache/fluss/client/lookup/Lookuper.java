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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.exception.MvccReadOffsetExpiredException;
import org.apache.fluss.exception.MvccReadOffsetNotYetReadableException;
import org.apache.fluss.exception.TableNotVersionedException;
import org.apache.fluss.metadata.BucketLogOffset;
import org.apache.fluss.row.InternalRow;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.concurrent.CompletableFuture;

/**
 * A lookuper performs key-based lookups against a primary key table, using either the full primary
 * key or a prefix of the primary key (when configured via {@code Lookup#lookupBy}).
 *
 * <p>Usage examples:
 *
 * <pre>{@code
 * // Row-based key (InternalRow)
 * Lookuper lookuper = table.newLookup().createLookuper();
 * LookupResult res = lookuper.lookup(keyRow).get();
 * }</pre>
 *
 * @since 0.6
 */
@PublicEvolving
@NotThreadSafe
public interface Lookuper {

    /**
     * Looks up the latest state visible when the request executes.
     *
     * <p>The lookup key must be a primary key if the lookuper is a Primary Key Lookuper (created by
     * {@code table.newLookup().createLookuper()}), or be the prefix key if the lookuper is a Prefix
     * Key Lookuper (created by {@code table.newLookup().lookupBy(prefixKeys).createLookuper()}).
     *
     * @param lookupKey the lookup key
     * @return the result of lookup.
     */
    CompletableFuture<LookupResult> lookup(InternalRow lookupKey);

    /**
     * Looks up a primary key or prefix in a versioned table at the supplied bucket position.
     *
     * <p>The key must route to the physical bucket identified by the position. The result in the
     * lookuper's target schema reflects the committed changes with log offsets strictly below the
     * position's next offset. A key or prefix with no match returns an empty result.
     *
     * <p>The future fails with {@link TableNotVersionedException} for a table created without
     * version history, {@link MvccReadOffsetExpiredException} below the retained floor, or {@link
     * MvccReadOffsetNotYetReadableException} above the committed and applied end. The client
     * propagates position errors without automatically retrying. This overload is unavailable when
     * the lookuper is configured to insert missing rows.
     *
     * @param lookupKey the primary key or prefix used by this lookuper
     * @param position the exclusive log position in the bucket to which the key routes
     * @return the future containing the lookup result at the supplied position
     */
    default CompletableFuture<LookupResult> lookup(
            InternalRow lookupKey, BucketLogOffset position) {
        CompletableFuture<LookupResult> failed = new CompletableFuture<>();
        failed.completeExceptionally(
                new UnsupportedOperationException("Positioned lookup is unavailable."));
        return failed;
    }
}
