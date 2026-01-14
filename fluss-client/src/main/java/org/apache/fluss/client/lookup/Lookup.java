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

import java.util.Arrays;
import java.util.List;

/**
 * Used to configure and create a {@link Lookuper} to lookup rows of a primary key table. The built
 * lookuper can lookup by the full primary key, or by a prefix of the primary key when configured
 * via {@link #lookupBy}.
 *
 * <p>{@link Lookup} objects are immutable and can be shared between threads. Refinement methods,
 * like {@link #lookupBy}, create new {@code Lookup} instances.
 *
 * <p>Examples
 *
 * <p>Example 1: Primary Key Lookuper using an InternalRow key. Given a table with primary key
 * column [k STRING]:
 *
 * <pre>{@code
 * Lookuper lookuper = table.newLookup().createLookuper();
 * CompletableFuture<LookupResult> resultFuture = lookuper.lookup(GenericRow.of("key1"));
 * resultFuture.get().getRowList().forEach(System.out::println);
 * }</pre>
 *
 * <p>Example 2: Prefix Key Lookuper using an InternalRow key. Given a table with primary key
 * columns [a INT, b STRING, c BIGINT] and bucket key [a, b]:
 *
 * <pre>{@code
 * Lookuper lookuper = table.newLookup().lookupBy("a", "b").createLookuper();
 * CompletableFuture<LookupResult> resultFuture = lookuper.lookup(GenericRow.of(1, "b1"));
 * resultFuture.get().getRowList().forEach(System.out::println);
 * }</pre>
 *
 * <p>Example 3: Using a POJO key (conversion handled internally):
 *
 * <pre>{@code
 * TypedLookuper<MyKeyPojo> lookuper = table.newLookup().createTypedLookuper(MyKeyPojo.class);
 * LookupResult result = lookuper.lookup(new MyKeyPojo(...)).get();
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

    /**
     * Creates a {@link TypedLookuper} instance to lookup rows of a primary key table using POJOs.
     *
     * @param pojoClass the class of the POJO
     * @param <T> the type of the POJO
     * @return the typed lookuper
     */
    <T> TypedLookuper<T> createTypedLookuper(Class<T> pojoClass);
}
