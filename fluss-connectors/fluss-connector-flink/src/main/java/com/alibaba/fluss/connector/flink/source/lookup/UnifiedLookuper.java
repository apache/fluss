/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.flink.source.lookup;

import com.alibaba.fluss.client.lookup.LookupType;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.lookup.PrefixLookup;
import com.alibaba.fluss.client.lookup.PrefixLookupResult;
import com.alibaba.fluss.client.lookup.PrefixLookuper;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A unified view of Fluss lookuper function to simplify the implementation of Flink lookup
 * connector.
 */
public abstract class UnifiedLookuper {

    /** Lookups matched rows by the given key. */
    public abstract CompletableFuture<List<InternalRow>> lookup(InternalRow key);

    /** Creates a unified lookup function to lookup rows by primary key or prefix key. */
    public static UnifiedLookuper of(
            LookupType lookupType, RowType lookupKeyRowType, Table flussTable) {
        if (lookupType == LookupType.LOOKUP) {
            return new PrimaryKeyLookuper(flussTable.getLookuper());
        } else {
            PrefixLookuper prefixLookuper =
                    flussTable.getPrefixLookuper(
                            new PrefixLookup(lookupKeyRowType.getFieldNames()));
            return new PrefixKeyLookuper(prefixLookuper);
        }
    }

    /** An implementation that lookup using primary key of the table. */
    private static class PrimaryKeyLookuper extends UnifiedLookuper {

        private final Lookuper lookuper;

        private PrimaryKeyLookuper(Lookuper lookuper) {
            this.lookuper = lookuper;
        }

        @Override
        public CompletableFuture<List<InternalRow>> lookup(InternalRow key) {
            return lookuper.lookup(key)
                    .thenApply(
                            result ->
                                    result == null
                                            ? Collections.emptyList()
                                            : Collections.singletonList(result.getRow()));
        }
    }

    /** An implementation that lookup using prefix of primary key of the table. */
    private static class PrefixKeyLookuper extends UnifiedLookuper {

        private final PrefixLookuper lookuper;

        private PrefixKeyLookuper(PrefixLookuper lookuper) {
            this.lookuper = lookuper;
        }

        @Override
        public CompletableFuture<List<InternalRow>> lookup(InternalRow key) {
            return lookuper.prefixLookup(key).thenApply(PrefixLookupResult::getRowList);
        }
    }
}
