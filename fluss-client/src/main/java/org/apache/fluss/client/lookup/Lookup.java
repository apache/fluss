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
     * lookup), the table must define a bucket key that is a prefix of the primary key. The lookup
     * columns must be the bucket key, optionally followed by the next primary key columns, but not
     * the whole primary key. For example, with the primary key [a, b, c, d] and the bucket key [a],
     * the lookup columns can be [a], [a, b] or [a, b, c]. Lookups are routed by the bucket key and
     * return only the rows matching all lookup columns.
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
     * Returns a new lookup that accepts a range on the given column. Repeated calls replace the
     * range column. Supply bounds to {@link Lookuper#lookup(org.apache.fluss.row.InternalRow,
     * org.apache.fluss.utils.Range)} for each invocation.
     *
     * <p>Currently requires an ordered-key table and explicit lookup columns forming a physical
     * primary-key prefix, including all bucket and partition fields. The range column must
     * immediately follow that prefix. Validation happens when creating the lookuper.
     *
     * <p>Cannot be combined with insert-if-not-exists. The configured lookuper rejects calls
     * without bounds. Results contain all matching rows without an ordering guarantee.
     *
     * @param columnName the column to constrain by range
     */
    default Lookup rangeBy(String columnName) {
        throw new UnsupportedOperationException("Range lookup configuration is not supported.");
    }

    /**
     * Enables insert-if-not-exists behavior for the lookup operation. When enabled, if a lookup
     * does not find a matching row, a new row will be inserted with the lookup key values. This
     * feature cannot be used on tables that contain non-nullable columns other than the primary key
     * or auto-increment columns.
     *
     * @return a new Lookup instance with insert-if-not-exists enabled
     */
    Lookup enableInsertIfNotExists();

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
