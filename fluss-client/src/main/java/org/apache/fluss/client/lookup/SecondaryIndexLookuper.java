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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.concurrent.FutureUtils.doForward;

/**
 * Looks up candidate primary keys by index prefix, then rechecks current main rows to discard stale
 * entries. Partition recreation can produce multiple candidates for the same primary key.
 *
 * <p>Point lookups may block on partition metadata. Schedule them off the I/O thread that completes
 * the index lookup so that metadata responses can still be processed.
 */
@Internal
@ThreadSafe
public final class SecondaryIndexLookuper implements Lookuper {

    private final Lookuper indexTablePrefixLookuper;
    private final Lookuper mainTablePointLookuper;
    private final CompactedKeyEncoder indexKeyEncoder;
    private final InternalRow.FieldGetter[] idxColumnGettersInLookupKey;
    private final InternalRow.FieldGetter[] idxColumnGettersInMainRow;
    private final RowType lookupType;
    private final InternalRow.FieldGetter[] primaryKeyGetters;
    private final Consumer<LookupContinuation> submitContinuation;

    public static SecondaryIndexLookuper create(
            Schema mainSchema,
            List<String> idxCols,
            Table indexTable,
            Lookuper mainTablePointLookuper,
            LookupClient client) {
        Lookuper indexTablePrefixLookuper =
                indexTable
                        .newLookup()
                        .lookupBy(
                                Arrays.asList(
                                        IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN,
                                        IndexTableUtils.ROUTING_KEY_SYSTEM_COLUMN))
                        .createLookuper();
        return new SecondaryIndexLookuper(
                mainSchema,
                idxCols,
                indexTable.getTableInfo().getRowType(),
                indexTablePrefixLookuper,
                mainTablePointLookuper,
                client::executeContinuation);
    }

    SecondaryIndexLookuper(
            Schema mainSchema,
            List<String> indexColumns,
            RowType indexRowType,
            Lookuper indexTablePrefixLookuper,
            Lookuper mainTablePointLookuper,
            Consumer<LookupContinuation> submitContinuation) {
        this.indexTablePrefixLookuper = checkNotNull(indexTablePrefixLookuper);
        this.mainTablePointLookuper = checkNotNull(mainTablePointLookuper);
        this.submitContinuation = checkNotNull(submitContinuation);
        RowType mainRowType = mainSchema.getRowType();
        lookupType = mainRowType.project(indexColumns);
        indexKeyEncoder = new CompactedKeyEncoder(lookupType);
        idxColumnGettersInLookupKey = InternalRow.createFieldGetters(lookupType);
        idxColumnGettersInMainRow = gettersFor(mainRowType, indexColumns);
        primaryKeyGetters = gettersFor(indexRowType, mainSchema.getPrimaryKeyColumnNames());
    }

    private static InternalRow.FieldGetter[] gettersFor(RowType type, List<String> names) {
        InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[names.size()];
        for (int i = 0; i < getters.length; i++) {
            int position = type.getFieldIndex(names.get(i));
            getters[i] = InternalRow.createFieldGetter(type.getTypeAt(position), position);
        }
        return getters;
    }

    private GenericRow primaryKey(InternalRow indexRow) {
        GenericRow key = new GenericRow(primaryKeyGetters.length);
        for (int i = 0; i < primaryKeyGetters.length; i++) {
            key.setField(i, primaryKeyGetters[i].getFieldOrNull(indexRow));
        }
        return key;
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        for (int i = 0; i < idxColumnGettersInLookupKey.length; i++) {
            if (lookupKey.isNullAt(i)) {
                return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
            }
        }
        Object[] expectedIdxValues =
                AbstractLookuper.snapshotLookupKey(lookupKey, idxColumnGettersInLookupKey);
        return indexTablePrefixLookuper
                .lookup(
                        GenericRow.of(
                                IndexTableUtils.DATA_RECORD_KIND,
                                indexKeyEncoder.encodeKey(lookupKey)))
                .thenCompose(
                        hop1Result -> {
                            LookupContinuation continuation =
                                    new LookupContinuation(
                                            () -> doHop2(hop1Result, expectedIdxValues));
                            submitContinuation.accept(continuation);
                            return continuation;
                        });
    }

    // The queue entry owns the future, so immediate close can fail work that never ran.
    static final class LookupContinuation extends CompletableFuture<LookupResult>
            implements Runnable {
        private final Supplier<CompletableFuture<LookupResult>> lookup;

        LookupContinuation(Supplier<CompletableFuture<LookupResult>> lookup) {
            this.lookup = lookup;
        }

        @Override
        public void run() {
            try {
                lookup.get().whenComplete((result, failure) -> doForward(result, failure, this));
            } catch (Throwable failure) {
                completeExceptionally(failure);
            }
        }
    }

    private CompletableFuture<LookupResult> doHop2(
            LookupResult hop1Result, Object[] expectedIdxValues) {
        List<InternalRow> candidateIndexRows = hop1Result.getRowList();
        if (candidateIndexRows.isEmpty()) {
            return CompletableFuture.completedFuture(
                    new LookupResult(Collections.<InternalRow>emptyList()));
        }
        List<CompletableFuture<LookupResult>> mainFutures =
                new ArrayList<>(candidateIndexRows.size());
        Set<GenericRow> seenBasePrimaryKeys = new HashSet<>();
        for (InternalRow indexRow : candidateIndexRows) {
            GenericRow basePk = primaryKey(indexRow);
            if (seenBasePrimaryKeys.add(basePk)) {
                mainFutures.add(mainTablePointLookuper.lookup(basePk));
            }
        }
        return CompletableFuture.allOf(mainFutures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            List<InternalRow> aggregated = new ArrayList<>();
                            for (CompletableFuture<LookupResult> f : mainFutures) {
                                LookupResult r = f.join();
                                for (InternalRow mainRow : r.getRowList()) {
                                    if (idxColsMatch(expectedIdxValues, mainRow)) {
                                        aggregated.add(mainRow);
                                    }
                                }
                            }
                            return new LookupResult(aggregated);
                        });
    }

    private boolean idxColsMatch(Object[] expectedIdxValues, InternalRow mainRow) {
        for (int i = 0; i < expectedIdxValues.length; i++) {
            Object expected = expectedIdxValues[i];
            Object actual = idxColumnGettersInMainRow[i].getFieldOrNull(mainRow);
            if (expected == null
                    || actual == null
                    || !Equal.INSTANCE.test(lookupType.getTypeAt(i), actual, expected)) {
                return false;
            }
        }
        return true;
    }
}
