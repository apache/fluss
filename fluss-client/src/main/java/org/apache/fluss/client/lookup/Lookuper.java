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
     * Lookups certain row from the given lookup key.
     *
     * <p>The lookup key must be a primary key if the lookuper is a Primary Key Lookuper (created by
     * {@code table.newLookup().createLookuper()}), or be the prefix key if the lookuper is a Prefix
     * Key Lookuper (created by {@code table.newLookup().lookupBy(prefixKeys).createLookuper()}).
     *
     * @param lookupKey the lookup key
     * @return the result of lookup.
     */
    CompletableFuture<LookupResult> lookup(InternalRow lookupKey);
}
