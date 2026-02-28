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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.IcebergLakeStorage;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/** Test filter push down in {@link IcebergLakeSource}. */
class IcebergLakeSourceTest extends IcebergSourceTestBase {

    private static final Schema SCHEMA =
            new Schema(
                    required(1, "id", Types.IntegerType.get()),
                    optional(2, "name", Types.StringType.get()),
                    required(3, "__bucket", Types.IntegerType.get()),
                    required(4, "__offset", Types.LongType.get()),
                    required(5, "__timestamp", Types.TimestampType.withZone()));

    private static final PartitionSpec PARTITION_SPEC =
            PartitionSpec.builderFor(SCHEMA).bucket("id", DEFAULT_BUCKET_NUM).build();

    private static final PredicateBuilder FLUSS_BUILDER =
            new PredicateBuilder(RowType.of(DataTypes.BIGINT(), DataTypes.STRING()));

    @BeforeAll
    protected static void beforeAll() {
        IcebergSourceTestBase.beforeAll();
    }

    @Test
    void testWithFilters() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_filters");
        createTable(tablePath, SCHEMA, PARTITION_SPEC);

        // write some rows
        Table table = getTable(tablePath);
        List<Record> rows = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            rows.add(
                    createIcebergRecord(
                            SCHEMA,
                            i,
                            "name" + i,
                            0,
                            (long) i,
                            OffsetDateTime.now(ZoneOffset.UTC)));
        }
        writeRecord(table, rows, null, 0);

        // write some rows again
        table.refresh();
        rows = new ArrayList<>();
        for (int i = 14; i <= 16; i++) {
            rows.add(
                    createIcebergRecord(
                            SCHEMA,
                            i,
                            "name" + i,
                            0,
                            (long) i,
                            OffsetDateTime.now(ZoneOffset.UTC)));
        }
        writeRecord(table, rows, null, 0);
        table.refresh();

        // test all filter can be accepted
        Predicate filter1 = FLUSS_BUILDER.greaterOrEqual(0, 2);
        Predicate filter2 = FLUSS_BUILDER.lessOrEqual(0, 3);
        Predicate filter3 = FLUSS_BUILDER.startsWith(1, BinaryString.fromString("name"));
        List<Predicate> allFilters = Arrays.asList(filter1, filter2, filter3);

        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        LakeSource.FilterPushDownResult filterPushDownResult = lakeSource.withFilters(allFilters);
        assertThat(filterPushDownResult.acceptedPredicates()).isEqualTo(allFilters);
        assertThat(filterPushDownResult.remainingPredicates()).isEmpty();

        // read data to verify the filters work
        List<IcebergSplit> icebergSplits =
                lakeSource.createPlanner(() -> table.currentSnapshot().snapshotId()).plan();
        assertThat(icebergSplits).hasSize(1);
        IcebergSplit icebergSplit = icebergSplits.get(0);

        // read data with filter to make sure the reader with filter works properly
        List<Row> actual = new ArrayList<>();
        org.apache.fluss.row.InternalRow.FieldGetter[] fieldGetters =
                org.apache.fluss.row.InternalRow.createFieldGetters(
                        RowType.of(new IntType(), new StringType()));
        RecordReader recordReader = lakeSource.createRecordReader(() -> icebergSplit);
        try (CloseableIterator<LogRecord> iterator = recordReader.read()) {
            actual.addAll(
                    convertToFlinkRow(
                            fieldGetters,
                            TransformingCloseableIterator.transform(iterator, LogRecord::getRow)));
        }
        assertThat(actual.toString()).isEqualTo("[+I[2, name2], +I[3, name3]]");

        // test mix one unaccepted filter
        Predicate nonConvertibleFilter = FLUSS_BUILDER.endsWith(1, BinaryString.fromString("name"));
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

    @Test
    void testCreateRecordReaderReusesCachedTable() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_cache_reuse");
        createTable(tablePath, SCHEMA, PARTITION_SPEC);
        Table table = getTable(tablePath);
        List<Record> rows = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            rows.add(
                    createIcebergRecord(
                            SCHEMA, i, "n" + i, 0, (long) i, OffsetDateTime.now(ZoneOffset.UTC)));
        }
        writeRecord(table, rows, null, 0);
        table.refresh();

        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<IcebergSplit> splits =
                lakeSource.createPlanner(() -> table.currentSnapshot().snapshotId()).plan();
        assertThat(splits).isNotEmpty();

        org.apache.fluss.row.InternalRow.FieldGetter[] fieldGetters =
                org.apache.fluss.row.InternalRow.createFieldGetters(
                        RowType.of(new IntType(), new StringType()));
        List<Row> actual = new ArrayList<>();
        for (IcebergSplit split : splits) {
            RecordReader recordReader = lakeSource.createRecordReader(() -> split);
            try (CloseableIterator<LogRecord> it = recordReader.read()) {
                actual.addAll(
                        convertToFlinkRow(
                                fieldGetters,
                                TransformingCloseableIterator.transform(it, LogRecord::getRow)));
            }
        }
        assertThat(actual).hasSize(3);
        assertThat(actual.toString()).contains("n1", "n2", "n3");
    }

    @Test
    void testGetSchemaUsesCacheAfterCreateRecordReader() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_get_schema_cache");
        createTable(tablePath, SCHEMA, PARTITION_SPEC);
        Table table = getTable(tablePath);
        List<Record> rows = new ArrayList<>();
        rows.add(createIcebergRecord(SCHEMA, 1, "a", 0, 1L, OffsetDateTime.now(ZoneOffset.UTC)));
        writeRecord(table, rows, null, 0);
        table.refresh();

        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<IcebergSplit> splits =
                lakeSource.createPlanner(() -> table.currentSnapshot().snapshotId()).plan();
        RecordReader recordReader = lakeSource.createRecordReader(() -> splits.get(0));
        try (CloseableIterator<LogRecord> it = recordReader.read()) {
            assertThat(it.hasNext()).isTrue();
            it.next();
        }
        Predicate filter = FLUSS_BUILDER.greaterOrEqual(0, 0);
        LakeSource.FilterPushDownResult result =
                lakeSource.withFilters(Collections.singletonList(filter));
        assertThat(result.acceptedPredicates()).hasSize(1);
    }

    @Test
    void testTableCacheTtlZeroDisablesExpiry() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_ttl_zero");
        createTable(tablePath, SCHEMA, PARTITION_SPEC);
        Table table = getTable(tablePath);
        List<Record> rows = new ArrayList<>();
        rows.add(createIcebergRecord(SCHEMA, 1, "v1", 0, 1L, OffsetDateTime.now(ZoneOffset.UTC)));
        writeRecord(table, rows, null, 0);
        table.refresh();

        Configuration config = new Configuration();
        config.setString("warehouse", warehousePath);
        config.setString("type", "hadoop");
        config.setString("name", "fluss_test_catalog_ttl_zero");
        config.setString(IcebergLakeSource.TABLE_CACHE_TTL_MS_KEY, "0");
        IcebergLakeStorage storageWithTtl = new IcebergLakeStorage(config);
        LakeSource<IcebergSplit> lakeSource = storageWithTtl.createLakeSource(tablePath);

        List<IcebergSplit> splits =
                lakeSource.createPlanner(() -> table.currentSnapshot().snapshotId()).plan();
        org.apache.fluss.row.InternalRow.FieldGetter[] fieldGetters =
                org.apache.fluss.row.InternalRow.createFieldGetters(
                        RowType.of(new IntType(), new StringType()));
        List<Row> actual = new ArrayList<>();
        for (IcebergSplit split : splits) {
            RecordReader recordReader = lakeSource.createRecordReader(() -> split);
            try (CloseableIterator<LogRecord> it = recordReader.read()) {
                actual.addAll(
                        convertToFlinkRow(
                                fieldGetters,
                                TransformingCloseableIterator.transform(it, LogRecord::getRow)));
            }
        }
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getField(1)).isEqualTo(BinaryString.fromString("v1"));
    }

    @Test
    void testTableCacheTtlRefresh() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_ttl_refresh");
        createTable(tablePath, SCHEMA, PARTITION_SPEC);
        Table table = getTable(tablePath);
        List<Record> rows = new ArrayList<>();
        rows.add(createIcebergRecord(SCHEMA, 1, "v1", 0, 1L, OffsetDateTime.now(ZoneOffset.UTC)));
        writeRecord(table, rows, null, 0);
        table.refresh();

        Configuration config = new Configuration();
        config.setString("warehouse", warehousePath);
        config.setString("type", "hadoop");
        config.setString("name", "fluss_test_catalog_ttl_refresh");
        config.setString(IcebergLakeSource.TABLE_CACHE_TTL_MS_KEY, "50");
        IcebergLakeStorage storageWithTtl = new IcebergLakeStorage(config);
        LakeSource<IcebergSplit> lakeSource = storageWithTtl.createLakeSource(tablePath);

        List<IcebergSplit> splits =
                lakeSource.createPlanner(() -> table.currentSnapshot().snapshotId()).plan();
        org.apache.fluss.row.InternalRow.FieldGetter[] fieldGetters =
                org.apache.fluss.row.InternalRow.createFieldGetters(
                        RowType.of(new IntType(), new StringType()));

        RecordReader recordReader1 = lakeSource.createRecordReader(() -> splits.get(0));
        List<Row> first = new ArrayList<>();
        try (CloseableIterator<LogRecord> it = recordReader1.read()) {
            first.addAll(
                    convertToFlinkRow(
                            fieldGetters,
                            TransformingCloseableIterator.transform(it, LogRecord::getRow)));
        }
        assertThat(first).hasSize(1);

        Thread.sleep(60);

        RecordReader recordReader2 = lakeSource.createRecordReader(() -> splits.get(0));
        List<Row> second = new ArrayList<>();
        try (CloseableIterator<LogRecord> it = recordReader2.read()) {
            second.addAll(
                    convertToFlinkRow(
                            fieldGetters,
                            TransformingCloseableIterator.transform(it, LogRecord::getRow)));
        }
        assertThat(second).hasSize(1);
        assertThat(second.get(0).getField(1)).isEqualTo(BinaryString.fromString("v1"));
    }
}
