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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.FieldRef;
import org.apache.fluss.predicate.FunctionVisitor;
import org.apache.fluss.predicate.LeafFunction;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.flink.types.Row;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** UT for {@link PaimonLakeSource}. */
class PaimonLakeSourceTest extends PaimonSourceTestBase {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", org.apache.paimon.types.DataTypes.INT())
                    .column("name", org.apache.paimon.types.DataTypes.STRING())
                    .column("__bucket", org.apache.paimon.types.DataTypes.INT())
                    .column("__offset", org.apache.paimon.types.DataTypes.BIGINT())
                    .column("__timestamp", org.apache.paimon.types.DataTypes.TIMESTAMP(6))
                    .primaryKey("id")
                    .option(CoreOptions.BUCKET.key(), "1")
                    .build();

    private static final PredicateBuilder FLUSS_BUILDER =
            new PredicateBuilder(RowType.of(DataTypes.BIGINT(), DataTypes.STRING()));

    @BeforeAll
    protected static void beforeAll() {
        PaimonSourceTestBase.beforeAll();
    }

    @Test
    void testEmptyFiltersDoNotAccessCatalog() {
        PaimonLakeSource lakeSource =
                new PaimonLakeSource(
                        new Configuration(), TablePath.of("missing_db", "missing_table"));

        LakeSource.FilterPushDownResult result = lakeSource.withFilters(Collections.emptyList());

        assertThat(result.acceptedPredicates()).isEmpty();
        assertThat(result.remainingPredicates()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testWithFilters(boolean useProvidedTable) throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, DEFAULT_TABLE);
        createTable(tablePath, SCHEMA);

        // write some rows
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            rows.add(
                    GenericRow.of(
                            i,
                            BinaryString.fromString("name" + i),
                            0,
                            (long) i,
                            Timestamp.fromEpochMillis(System.currentTimeMillis())));
        }
        writeRecord(tablePath, rows);

        // write some rows again
        rows = new ArrayList<>();
        for (int i = 10; i <= 14; i++) {
            rows.add(
                    GenericRow.of(
                            i,
                            BinaryString.fromString("name" + i),
                            0,
                            (long) i,
                            Timestamp.fromEpochMillis(System.currentTimeMillis())));
        }
        writeRecord(tablePath, rows);

        // test all filter can be accepted
        Predicate filter1 = FLUSS_BUILDER.greaterOrEqual(0, 2);
        Predicate filter2 = FLUSS_BUILDER.lessOrEqual(0, 3);
        Predicate filter3 =
                FLUSS_BUILDER.startsWith(1, org.apache.fluss.row.BinaryString.fromString("name"));
        List<Predicate> allFilters = Arrays.asList(filter1, filter2, filter3);

        LakeSource<PaimonSplit> lakeSource =
                useProvidedTable
                        ? new PaimonLakeSource((FileStoreTable) getTable(tablePath))
                        : lakeStorage.createLakeSource(tablePath);
        LakeSource.FilterPushDownResult filterPushDownResult = lakeSource.withFilters(allFilters);
        assertThat(filterPushDownResult.acceptedPredicates()).isEqualTo(allFilters);
        assertThat(filterPushDownResult.remainingPredicates()).isEmpty();
        LakeSource<PaimonSplit> copiedLakeSource = lakeSource.copy();

        // read data to verify the filters work
        List<PaimonSplit> paimonSplits = lakeSource.createPlanner(() -> 2).plan();
        assertThat(paimonSplits).hasSize(1);
        PaimonSplit paimonSplit = paimonSplits.get(0);
        // make sure we only have one data file after filter to check plan will make use
        // of filters
        assertThat(paimonSplit.dataSplit().dataFiles()).hasSize(1);

        // read data with filter to make sure the reader with filter works properly
        List<Row> actual = new ArrayList<>();
        org.apache.fluss.row.InternalRow.FieldGetter[] fieldGetters =
                org.apache.fluss.row.InternalRow.createFieldGetters(
                        RowType.of(new IntType(), new StringType()));
        RecordReader recordReader = lakeSource.createRecordReader(() -> paimonSplit);
        try (CloseableIterator<LogRecord> iterator = recordReader.read()) {
            actual.addAll(
                    convertToFlinkRow(
                            fieldGetters,
                            TransformingCloseableIterator.transform(iterator, LogRecord::getRow)));
        }
        assertThat(actual.toString()).isEqualTo("[+I[2, name2], +I[3, name3]]");

        // test mix one unaccepted filter
        Predicate nonConvertibleFilter =
                new LeafPredicate(
                        new UnSupportFilterFunction(),
                        DataTypes.INT(),
                        0,
                        "f1",
                        Collections.emptyList());
        allFilters = Arrays.asList(nonConvertibleFilter, filter1, filter2);

        filterPushDownResult = lakeSource.withFilters(allFilters);
        assertThat(filterPushDownResult.acceptedPredicates().toString())
                .isEqualTo(Arrays.asList(filter1, filter2).toString());
        assertThat(filterPushDownResult.remainingPredicates().toString())
                .isEqualTo(Collections.singleton(nonConvertibleFilter).toString());

        // test all are unaccepted filter
        allFilters = Arrays.asList(nonConvertibleFilter, nonConvertibleFilter);
        filterPushDownResult = lakeSource.withFilters(allFilters);
        assertThat(filterPushDownResult.acceptedPredicates()).isEmpty();
        assertThat(filterPushDownResult.remainingPredicates().toString())
                .isEqualTo(allFilters.toString());

        List<PaimonSplit> unfilteredSplits = lakeSource.createPlanner(() -> 2).plan();
        assertThat(unfilteredSplits).hasSize(1);
        assertThat(unfilteredSplits.get(0).dataSplit().dataFiles()).hasSize(2);

        List<PaimonSplit> copiedFilteredSplits = copiedLakeSource.createPlanner(() -> 2).plan();
        assertThat(copiedFilteredSplits).hasSize(1);
        assertThat(copiedFilteredSplits.get(0).dataSplit().dataFiles()).hasSize(1);
    }

    @Test
    void testNullProvidedTable() {
        assertThatThrownBy(() -> new PaimonLakeSource((FileStoreTable) null))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testProvidedTableCopyAndSerialization(boolean primaryKeyTable) throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, DEFAULT_TABLE);
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.INT())
                        .column("name", org.apache.paimon.types.DataTypes.STRING());
        if (primaryKeyTable) {
            schema.primaryKey("id").option(CoreOptions.BUCKET.key(), "1");
        }
        createTable(tablePath, schema.build());
        writeRecord(
                tablePath,
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("one")),
                        GenericRow.of(2, BinaryString.fromString("two"))));
        long snapshotId = getTable(tablePath).latestSnapshot().get().id();
        writeRecord(
                tablePath,
                Collections.singletonList(GenericRow.of(3, BinaryString.fromString("three"))));

        FileStoreTable table = (FileStoreTable) getTable(tablePath);
        long latestSnapshotId = table.latestSnapshot().get().id();
        Map<String, String> options = new HashMap<>(table.options());
        PaimonLakeSource source = new PaimonLakeSource(table);
        PredicateBuilder predicateBuilder =
                new PredicateBuilder(RowType.of(DataTypes.INT(), DataTypes.STRING()));
        source.withFilters(Collections.singletonList(predicateBuilder.greaterOrEqual(0, 2)));
        source.withProject(new int[][] {{1}});

        PaimonLakeSource copiedSource = source.copy();
        PaimonLakeSource serializedSource = InstantiationUtils.clone(source);
        source.withFilters(Collections.emptyList());
        source.withProject(new int[][] {{0}});

        assertThat(readRows(copiedSource, snapshotId, RowType.of(DataTypes.STRING())))
                .containsExactly(Row.of(org.apache.fluss.row.BinaryString.fromString("two")));
        assertThat(readRows(serializedSource, snapshotId, RowType.of(DataTypes.STRING())))
                .containsExactly(Row.of(org.apache.fluss.row.BinaryString.fromString("two")));
        assertThat(readRows(source, snapshotId, RowType.of(DataTypes.INT())))
                .containsExactlyInAnyOrder(Row.of(1), Row.of(2));
        assertThat(table.options()).isEqualTo(options);
        assertThat(table.newScan().plan().splits())
                .isNotEmpty()
                .allSatisfy(
                        split ->
                                assertThat(((DataSplit) split).snapshotId())
                                        .isEqualTo(latestSnapshotId));
    }

    private List<Row> readRows(LakeSource<PaimonSplit> source, long snapshotId, RowType rowType)
            throws Exception {
        List<Row> rows = new ArrayList<>();
        org.apache.fluss.row.InternalRow.FieldGetter[] fieldGetters =
                org.apache.fluss.row.InternalRow.createFieldGetters(rowType);
        for (PaimonSplit split : source.createPlanner(() -> snapshotId).plan()) {
            assertThat(split.dataSplit().snapshotId()).isEqualTo(snapshotId);
            try (CloseableIterator<LogRecord> iterator =
                    source.createRecordReader(() -> split).read()) {
                rows.addAll(
                        convertToFlinkRow(
                                fieldGetters,
                                TransformingCloseableIterator.transform(
                                        iterator, LogRecord::getRow)));
            }
        }
        return rows;
    }

    private static class UnSupportFilterFunction extends LeafFunction {

        @Override
        public boolean test(DataType type, Object field, List<Object> literals) {
            return false;
        }

        @Override
        public boolean test(
                DataType type,
                long rowCount,
                Object min,
                Object max,
                long nullCount,
                List<Object> literals) {
            return false;
        }

        @Override
        public Optional<LeafFunction> negate() {
            return Optional.empty();
        }

        @Override
        public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
            throw new UnsupportedOperationException(
                    "Unsupported filter function for test purpose.");
        }
    }
}
