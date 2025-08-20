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

package com.alibaba.fluss.lake.paimon.source;

import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.lake.source.RecordReader;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.predicate.FieldRef;
import com.alibaba.fluss.predicate.FunctionVisitor;
import com.alibaba.fluss.predicate.LeafFunction;
import com.alibaba.fluss.predicate.LeafPredicate;
import com.alibaba.fluss.predicate.Predicate;
import com.alibaba.fluss.predicate.PredicateBuilder;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.flink.types.Row;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.schema.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

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
    void testWithFilters() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_filters");
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
                FLUSS_BUILDER.startsWith(1, com.alibaba.fluss.row.BinaryString.fromString("name"));
        List<Predicate> allFilters = Arrays.asList(filter1, filter2, filter3);

        LakeSource<PaimonSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        LakeSource.FilterPushDownResult filterPushDownResult = lakeSource.withFilters(allFilters);
        assertThat(filterPushDownResult.acceptedPredicates()).isEqualTo(allFilters);
        assertThat(filterPushDownResult.remainingPredicates()).isEmpty();

        // read data to verify the filters work
        List<PaimonSplit> paimonSplits = lakeSource.createPlanner(() -> 2).plan();
        assertThat(paimonSplits).hasSize(1);
        PaimonSplit paimonSplit = paimonSplits.get(0);
        // make sure we only have one data file after filter to check plan will make use
        // of filters
        assertThat(paimonSplit.dataSplit().dataFiles()).hasSize(1);

        // read data with filter to mae sure the reader with filter works properly
        List<Row> actual = new ArrayList<>();
        com.alibaba.fluss.row.InternalRow.FieldGetter[] fieldGetters =
                com.alibaba.fluss.row.InternalRow.createFieldGetters(
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
                Long nullCount,
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
