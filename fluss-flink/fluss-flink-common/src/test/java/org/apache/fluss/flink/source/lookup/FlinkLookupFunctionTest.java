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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.source.lookup.LookupNormalizer.createPrimaryKeyLookupNormalizer;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkLookupFunction} and {@link FlinkAsyncLookupFunction}. */
class FlinkLookupFunctionTest extends FlinkTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSyncLookupEval(boolean schemaNotMatch) throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "sync-lookup-table-" + schemaNotMatch);
        prepareData(tablePath, 3, schemaNotMatch);

        RowType flinkRowType =
                FlinkConversions.toFlinkRowType(DEFAULT_PK_TABLE_SCHEMA.getRowType());
        FlinkLookupFunction lookupFunction =
                new FlinkLookupFunction(
                        clientConf,
                        tablePath,
                        flinkRowType,
                        createPrimaryKeyLookupNormalizer(new int[] {0}, flinkRowType),
                        // no projection when job compiling, new column added after that.
                        null);

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);

        lookupFunction.open(null);

        // look up
        for (int i = 0; i < 4; i++) {
            lookupFunction.eval(i);
        }

        // collect the result and check
        List<String> result =
                collector.getOutputs().stream()
                        .map(RowData::toString)
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).containsExactly("+I(0,name0)", "+I(1,name1)", "+I(2,name2)");

        lookupFunction.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAsyncLookupEval(boolean schemaNotMatch) throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "async-lookup-table-" + schemaNotMatch);
        int rows = 3;
        prepareData(tablePath, rows, schemaNotMatch);

        RowType flinkRowType =
                FlinkConversions.toFlinkRowType(DEFAULT_PK_TABLE_SCHEMA.getRowType());
        AsyncLookupFunction asyncLookupFunction =
                new FlinkAsyncLookupFunction(
                        clientConf,
                        tablePath,
                        flinkRowType,
                        createPrimaryKeyLookupNormalizer(new int[] {0}, flinkRowType),
                        // no projection when job compiling, new column added after that.
                        null);
        asyncLookupFunction.open(null);

        int[] rowKeys = new int[] {0, 1, 2, 3, 4, 3, 0};
        CountDownLatch latch = new CountDownLatch(rowKeys.length);
        final List<String> result = new ArrayList<>();
        for (int rowKey : rowKeys) {
            CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
            asyncLookupFunction.eval(future, rowKey);
            future.whenComplete(
                    (rs, t) -> {
                        synchronized (result) {
                            if (rs.isEmpty()) {
                                result.add(rowKey + ": null");
                            } else {
                                rs.forEach(row -> result.add(rowKey + ": " + row.toString()));
                            }
                        }
                        latch.countDown();
                    });
        }

        // this verifies lookup calls are async
        assertThat(result.size()).isLessThan(rows);
        latch.await();
        asyncLookupFunction.close();
        Collections.sort(result);

        assertThat(result)
                .containsExactly(
                        "0: +I(0,name0)",
                        "0: +I(0,name0)",
                        "1: +I(1,name1)",
                        "2: +I(2,name2)",
                        "3: null",
                        "3: null",
                        "4: null");
    }

    @Test
    void testSchemaChange() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "sync-add-column");
        prepareData(tablePath, 3, false);

        RowType flinkRowType =
                FlinkConversions.toFlinkRowType(DEFAULT_PK_TABLE_SCHEMA.getRowType());
        FlinkLookupFunction lookupFunction =
                new FlinkLookupFunction(
                        clientConf,
                        tablePath,
                        flinkRowType,
                        createPrimaryKeyLookupNormalizer(new int[] {0}, flinkRowType),
                        new int[] {1, 0});

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);
        lookupFunction.open(null);

        // alter table after lookup function is created.
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "new_column",
                                        DataTypes.INT(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();
        FLUSS_CLUSTER_EXTENSION.waitAllSchemaSync(tablePath, 2);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            upsertWriter.upsert(row(3, "name3", 3));
            upsertWriter.flush();
        }

        // look up
        for (int i = 0; i < 5; i++) {
            lookupFunction.eval(i);
        }

        // collect the result and check
        List<String> result =
                collector.getOutputs().stream()
                        .map(RowData::toString)
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result)
                .containsExactly("+I(name0,0)", "+I(name1,1)", "+I(name2,2)", "+I(name3,3)");
        lookupFunction.close();

        // start lookup job after schema change.
        lookupFunction =
                new FlinkLookupFunction(
                        clientConf,
                        tablePath,
                        flinkRowType,
                        createPrimaryKeyLookupNormalizer(new int[] {0}, flinkRowType),
                        null);
        collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);
        lookupFunction.open(null);
        for (int i = 0; i < 5; i++) {
            lookupFunction.eval(i);
        }
        result =
                collector.getOutputs().stream()
                        .map(RowData::toString)
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result)
                .containsExactly("+I(0,name0)", "+I(1,name1)", "+I(2,name2)", "+I(3,name3)");
        lookupFunction.close();
    }

    private void prepareData(TablePath tablePath, int rows, boolean schemaNotMatch)
            throws Exception {
        TableDescriptor descriptor = DEFAULT_PK_TABLE_DESCRIPTOR;
        if (schemaNotMatch) {
            descriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .primaryKey("id")
                                            .column("id", DataTypes.INT())
                                            .column("name", DataTypes.STRING())
                                            // added an extra column 'age'
                                            .column("age", DataTypes.INT())
                                            .build())
                            .distributedBy(DEFAULT_BUCKET_NUM, "id")
                            .build();
        }

        createTable(tablePath, descriptor);

        // first write some data to the table
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 0; i < rows; i++) {
                upsertWriter.upsert(
                        schemaNotMatch ? row(i, "name" + i, i * 2) : row(i, "name" + i));
            }
            upsertWriter.flush();
        }
    }

    private static final class ListOutputCollector implements Collector<RowData> {

        private final List<RowData> output = new ArrayList<>();

        @Override
        public void collect(RowData row) {
            this.output.add(row);
        }

        @Override
        public void close() {}

        public List<RowData> getOutputs() {
            return output;
        }
    }
}
