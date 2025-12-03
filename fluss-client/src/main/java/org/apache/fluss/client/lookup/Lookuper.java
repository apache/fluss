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
 * The lookup-er is used to lookup row of a primary key table by primary key or prefix key. The
 * lookuper has retriable ability to handle transient errors during lookup operations which is
 * configured by {@link org.apache.fluss.config.ConfigOptions#CLIENT_LOOKUP_MAX_RETRIES}.
 *
 * <p>Note: Lookuper instances are not thread-safe.
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
     * @param lookupKey the lookup key.
     * @return the result of lookup.
     */
    CompletableFuture<LookupResult> lookup(InternalRow lookupKey);
}
