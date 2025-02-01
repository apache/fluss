/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.lookup;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.Arrays;
import java.util.List;

/**
 * Used to configure and create a {@link Lookuper} to lookup rows of a primary key table. The built
 * Lookuper can be a primary key lookuper that lookups by the primary key, or a prefix key lookup
 * that lookups by the prefix key of the primary key.
 *
 * <p>{@link Lookup} objects are immutable and can be shared between threads. Refinement methods,
 * like {@link #lookupBy}, create new Lookup instances.
 *
 * <p>Example1: Create a Primary Key Lookuper. Given a table with primary key column [k STRING].
 *
 * <pre>{@code
 * Lookuper lookuper = table.newLookup().createLookuper();
 * CompletableFuture<LookupResult> resultFuture = lookuper.lookup(GenericRow.of("key1"));
 * resultFuture.get().getRows().forEach(row -> {
 *    System.out.println(row);
 * });
 * }</pre>
 *
 * <p>Example2: Create a Prefix Key Lookuper. Given a table with primary key column [a INT, b
 * STRING, c BIGINT] and bucket key [a, b].
 *
 * <pre>{@code
 * Lookuper lookuper = table.newLookup().lookupBy("a", "b").createLookuper();
 * CompletableFuture<LookupResult> resultFuture = lookuper.lookup(GenericRow.of(1, "b1"));
 * resultFuture.get().getRows().forEach(row -> {
 *   System.out.println(row);
 * });
 * }</pre>
 *
 * @since 0.6
 */
@PublicEvolving
public interface Lookup {

    /**
     * Returns a new Lookup instance with the given lookup columns. By default, the lookup columns
     * are the primary key columns of the table. The specified columns must be a prefix subset of
     * the physical primary key columns (i.e., the primary key columns exclude partition columns).
     *
     * <p>Note: Currently, if you want to lookup with specified lookup columns (i.e., prefix key
     * lookup), the table you created must both define the primary key and the bucket key, in
     * addition, the lookup columns needs to be equals with bucket key, and to be a part of the
     * primary key and must be a prefix of the primary key. For example, if a table has fields
     * [a,b,c,d], and the primary key is set to [a, b, c], with the bucket key set to [a, b], then
     * the schema of the lookup columns would also be [a, b]. This pattern can create a Prefix Key
     * Lookuper lookup rows by prefix of the primary key.
     *
     * <p>TODO: currently, the interface only support bucket key as the prefix key to lookup.
     * Generalize the prefix key lookup to support any prefix keys.
     *
     * <p>We also support prefix key lookup for partitioned tables. The schema of the lookup columns
     * should contain partition fields and bucket key. In addition, the schema of the lookup columns
     * excluded partition fields should be a prefix of primary key excluded partition fields.
     *
     * @param lookupColumnNames the specified columns to lookup by
     */
    Lookup lookupBy(List<String> lookupColumnNames);

    /**
     * @see #lookupBy(List) for more details.
     * @param lookupColumnNames the specified columns to lookup by
     */
    default Lookup lookupBy(String... lookupColumnNames) {
        return lookupBy(Arrays.asList(lookupColumnNames));
    }

    /**
     * Creates a {@link Lookuper} instance to lookup rows of a primary key table by the specified
     * lookup columns. By default, the lookup columns are the primary key columns, but can be
     * changed with ({@link #lookupBy(List)}) method.
     *
     * @return the lookuper
     */
    Lookuper createLookuper();
}
