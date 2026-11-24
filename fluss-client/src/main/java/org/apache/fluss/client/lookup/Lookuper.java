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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.row.InternalRow;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.CompletableFuture;

/**
 * Looks up rows by primary key, primary-key prefix, or a declared secondary index.
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
@ThreadSafe
public interface Lookuper {

    /**
     * Looks up rows matching the given key. Supply the full primary key for a point lookup, the
     * configured prefix for a prefix lookup, or the index columns in declaration order for a
     * lookuper created by {@link Table#getSecondaryIndexLookuper(String)}.
     *
     * @param lookupKey the lookup key
     * @return the result of lookup.
     */
    CompletableFuture<LookupResult> lookup(InternalRow lookupKey);
}
