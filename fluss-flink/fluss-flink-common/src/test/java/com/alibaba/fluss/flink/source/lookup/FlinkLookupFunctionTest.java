/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.source.lookup;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.flink.utils.FlinkConversions;
import com.alibaba.fluss.flink.utils.FlinkTestBase;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static com.alibaba.fluss.flink.source.lookup.LookupNormalizer.createPrimaryKeyLookupNormalizer;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkLookupFunction} and {@link FlinkAsyncLookupFunction}. */
class FlinkLookupFunctionTest extends FlinkTestBase {

    @Test
    void testSyncLookupEval() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "sync-lookup-table");
        prepareData(tablePath, 3);

        RowType flinkRowType =
                FlinkConversions.toFlinkRowType(DEFAULT_PK_TABLE_SCHEMA.getRowType());
        FlinkLookupFunction lookupFunction =
                new FlinkLookupFunction(
                        clientConf,
                        tablePath,
                        flinkRowType,
                        LookupOptions.MAX_RETRIES.defaultValue(),
                        createPrimaryKeyLookupNormalizer(new int[] {0}, flinkRowType),
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

    @Test
    void testAsyncLookupEval() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "async-lookup-table");
        int rows = 3;
        prepareData(tablePath, rows);

        RowType flinkRowType =
                FlinkConversions.toFlinkRowType(DEFAULT_PK_TABLE_SCHEMA.getRowType());
        AsyncLookupFunction asyncLookupFunction =
                new FlinkAsyncLookupFunction(
                        clientConf,
                        tablePath,
                        flinkRowType,
                        LookupOptions.MAX_RETRIES.defaultValue(),
                        createPrimaryKeyLookupNormalizer(new int[] {0}, flinkRowType),
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

    private void prepareData(TablePath tablePath, int rows) throws Exception {
        createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);

        // first write some data to the table
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 0; i < rows; i++) {
                upsertWriter.upsert(row(i, "name" + i));
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
