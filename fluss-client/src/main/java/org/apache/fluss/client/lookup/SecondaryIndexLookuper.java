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
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Two-hop {@link Lookuper} for a Global Secondary Index.
 *
 * <p>Hop 1: prefix-scan the Index Table by the provided {@code lookupKey} to obtain candidate Index
 * Table rows (each carrying the base table's primary key in trailing positions).
 *
 * <p>Hop 2: extract the logical basePK via {@code basePkExtractorFromIndexRow}, deduplicate
 * physical candidates by that key, and point-get the main table once per key. The deduplication
 * prevents old and new physical Index Table rows for a recreated partition from emitting the same
 * current main row twice. A main lookup returning an empty result list means the row was deleted
 * (stale Index Table pointer) and is skipped from the aggregated output.
 *
 * <p>Recheck: after Hop 2 returns, every surviving main row is re-validated against the user's
 * original {@code lookupKey}. Index columns are extracted from both sides using positional {@link
 * InternalRow.FieldGetter}s and compared according to their Fluss data types. Any row whose current
 * {@code idxCols} disagree with the lookup key is discarded as a stale index pointer (covers both
 * the async-visibility window and the natural lag during partition-tombstone cleanup).
 *
 * <p>Threading: Hop 2 is dispatched on {@code continuationExecutor} instead of the thread that
 * completes the Hop 1 future (a Netty I/O thread in production). Hop 2 submits point lookups
 * against the main table, which may synchronously block on a metadata refresh when partition
 * metadata is not cached yet; running that on an I/O thread would stall every RPC response served
 * by that thread and can even deadlock the metadata fetch itself.
 */
@Internal
@ThreadSafe
public final class SecondaryIndexLookuper implements Lookuper {

    private static final Logger LOG = LoggerFactory.getLogger(SecondaryIndexLookuper.class);
    private static final int LOW_SELECTIVITY_WARN_CANDIDATE_THRESHOLD = 1024;

    private final Lookuper indexTablePrefixLookuper;
    private final Lookuper mainTablePointLookuper;
    private final Function<InternalRow, InternalRow> indexLookupKeyEncoder;
    private final InternalRow.FieldGetter[] idxColumnGettersInLookupKey;
    private final InternalRow.FieldGetter[] idxColumnGettersInMainRow;
    private final DataType[] idxColumnTypes;
    private final Function<InternalRow, GenericRow> basePkExtractorFromIndexRow;
    private final Executor continuationExecutor;

    /**
     * Assembles a lookuper for one secondary index of {@code mainSchema}.
     *
     * <p>The wiring establishes the invariants this class depends on: the two getter arrays are
     * parallel to {@code idxCols}, and the base-PK extractor yields a row shaped like the main
     * table's primary key so {@code mainTablePointLookuper} can consume it directly.
     */
    public static SecondaryIndexLookuper create(
            Schema mainSchema,
            List<String> idxCols,
            Table indexTable,
            Lookuper mainTablePointLookuper,
            Executor continuationExecutor) {
        Lookuper indexTablePrefixLookuper =
                indexTable
                        .newLookup()
                        .lookupBy(
                                Arrays.asList(
                                        IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN,
                                        IndexTableUtils.ROUTING_KEY_SYSTEM_COLUMN))
                        .createLookuper();
        CompactedKeyEncoder logicalIndexKeyEncoder =
                new CompactedKeyEncoder(mainSchema.getRowType().project(idxCols));
        Function<InternalRow, InternalRow> indexLookupKeyEncoder =
                lookupKey ->
                        GenericRow.of(
                                IndexTableUtils.DATA_RECORD_KIND,
                                logicalIndexKeyEncoder.encodeKey(lookupKey));

        RowType mainRowType = mainSchema.getRowType();
        int idxColCount = idxCols.size();
        InternalRow.FieldGetter[] idxColumnGettersInLookupKey =
                new InternalRow.FieldGetter[idxColCount];
        InternalRow.FieldGetter[] idxColumnGettersInMainRow =
                new InternalRow.FieldGetter[idxColCount];
        DataType[] idxColumnTypes = new DataType[idxColCount];
        for (int i = 0; i < idxColCount; i++) {
            int posInMain = mainRowType.getFieldIndex(idxCols.get(i));
            DataType idxColumnType = mainRowType.getTypeAt(posInMain);
            idxColumnTypes[i] = idxColumnType;
            // The lookup-key row carries idx columns in declaration order at positions 0..n-1.
            idxColumnGettersInLookupKey[i] = InternalRow.createFieldGetter(idxColumnType, i);
            // The main row carries each idx column at its position in the main schema.
            idxColumnGettersInMainRow[i] = InternalRow.createFieldGetter(idxColumnType, posInMain);
        }

        // Index Table column ordering can dedup overlap between idx columns and PK columns, so each
        // base-PK position is resolved by name rather than assumed to be a trailing suffix.
        RowType indexRowType = indexTable.getTableInfo().getSchema().getRowType();
        List<String> basePk = mainSchema.getPrimaryKeyColumnNames();
        int pkSize = basePk.size();
        InternalRow.FieldGetter[] basePkGettersInIndexRow = new InternalRow.FieldGetter[pkSize];
        for (int i = 0; i < pkSize; i++) {
            int posInIndex = indexRowType.getFieldIndex(basePk.get(i));
            basePkGettersInIndexRow[i] =
                    InternalRow.createFieldGetter(indexRowType.getTypeAt(posInIndex), posInIndex);
        }
        Function<InternalRow, GenericRow> basePkExtractor =
                indexRow -> {
                    GenericRow pk = new GenericRow(pkSize);
                    for (int i = 0; i < pkSize; i++) {
                        pk.setField(i, basePkGettersInIndexRow[i].getFieldOrNull(indexRow));
                    }
                    return pk;
                };

        return new SecondaryIndexLookuper(
                indexTablePrefixLookuper,
                mainTablePointLookuper,
                indexLookupKeyEncoder,
                idxColumnGettersInLookupKey,
                idxColumnGettersInMainRow,
                idxColumnTypes,
                basePkExtractor,
                continuationExecutor);
    }

    SecondaryIndexLookuper(
            Lookuper indexTablePrefixLookuper,
            Lookuper mainTablePointLookuper,
            Function<InternalRow, InternalRow> indexLookupKeyEncoder,
            InternalRow.FieldGetter[] idxColumnGettersInLookupKey,
            InternalRow.FieldGetter[] idxColumnGettersInMainRow,
            DataType[] idxColumnTypes,
            Function<InternalRow, GenericRow> basePkExtractorFromIndexRow,
            Executor continuationExecutor) {
        this.indexTablePrefixLookuper =
                checkNotNull(indexTablePrefixLookuper, "indexTablePrefixLookuper");
        this.mainTablePointLookuper =
                checkNotNull(mainTablePointLookuper, "mainTablePointLookuper");
        this.indexLookupKeyEncoder = checkNotNull(indexLookupKeyEncoder, "indexLookupKeyEncoder");
        this.idxColumnGettersInLookupKey =
                checkNotNull(idxColumnGettersInLookupKey, "idxColumnGettersInLookupKey").clone();
        this.idxColumnGettersInMainRow =
                checkNotNull(idxColumnGettersInMainRow, "idxColumnGettersInMainRow").clone();
        this.idxColumnTypes = checkNotNull(idxColumnTypes, "idxColumnTypes").clone();
        checkArgument(
                this.idxColumnGettersInLookupKey.length == this.idxColumnGettersInMainRow.length
                        && this.idxColumnGettersInLookupKey.length == this.idxColumnTypes.length,
                "Index column getters and types must have the same length");
        this.basePkExtractorFromIndexRow =
                checkNotNull(basePkExtractorFromIndexRow, "basePkExtractorFromIndexRow");
        this.continuationExecutor = checkNotNull(continuationExecutor, "continuationExecutor");
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        Object[] expectedIdxValues = snapshotLookupKey(lookupKey);
        return indexTablePrefixLookuper
                .lookup(indexLookupKeyEncoder.apply(lookupKey))
                .thenComposeAsync(
                        hop1Result -> doHop2(hop1Result, expectedIdxValues), continuationExecutor);
    }

    private CompletableFuture<LookupResult> doHop2(
            LookupResult hop1Result, Object[] expectedIdxValues) {
        List<InternalRow> candidateIndexRows = hop1Result.getRowList();
        if (candidateIndexRows.size() >= LOW_SELECTIVITY_WARN_CANDIDATE_THRESHOLD) {
            LOG.warn(
                    "Secondary index lookup produced {} Hop1 candidate rows, reaching the low-selectivity warning threshold {}. This may indicate a low-selectivity secondary index or stale index entries; Hop2 will still process all candidates.",
                    candidateIndexRows.size(),
                    LOW_SELECTIVITY_WARN_CANDIDATE_THRESHOLD);
        }
        if (candidateIndexRows.isEmpty()) {
            return CompletableFuture.completedFuture(
                    new LookupResult(Collections.<InternalRow>emptyList()));
        }
        List<CompletableFuture<LookupResult>> mainFutures =
                new ArrayList<>(candidateIndexRows.size());
        Set<GenericRow> seenBasePrimaryKeys = new HashSet<>();
        for (InternalRow indexRow : candidateIndexRows) {
            GenericRow basePk = basePkExtractorFromIndexRow.apply(indexRow);
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

    private Object[] snapshotLookupKey(InternalRow lookupKey) {
        Object[] expectedIdxValues = new Object[idxColumnGettersInLookupKey.length];
        for (int i = 0; i < idxColumnGettersInLookupKey.length; i++) {
            expectedIdxValues[i] =
                    copyRecheckValue(idxColumnGettersInLookupKey[i].getFieldOrNull(lookupKey));
        }
        return expectedIdxValues;
    }

    private boolean idxColsMatch(Object[] expectedIdxValues, InternalRow mainRow) {
        for (int i = 0; i < expectedIdxValues.length; i++) {
            Object expected = expectedIdxValues[i];
            Object actual = idxColumnGettersInMainRow[i].getFieldOrNull(mainRow);
            if (expected == null
                    || actual == null
                    || !Equal.INSTANCE.test(idxColumnTypes[i], actual, expected)) {
                return false;
            }
        }
        return true;
    }

    private static Object copyRecheckValue(Object value) {
        if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        if (value instanceof BinaryString) {
            return ((BinaryString) value).copy();
        }
        return value;
    }
}
