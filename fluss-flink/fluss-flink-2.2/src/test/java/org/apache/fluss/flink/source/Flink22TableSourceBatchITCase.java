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

package org.apache.fluss.flink.source;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for batch source in Flink 2.2. */
public class Flink22TableSourceBatchITCase extends FlinkTableSourceBatchITCase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @Override
    void testCountPushDown(boolean partitionTable) throws Exception {
        String tableName = partitionTable ? preparePartitionedLogTable() : prepareLogTable();
        int expectedRows = partitionTable ? 10 : 5;
        // normal scan
        String query = String.format("SELECT COUNT(*) FROM %s", tableName);
        assertThat(tEnv.explainSql(query))
                .contains(
                        String.format(
                                "TableSourceScan(table=[[testcatalog, defaultdb, %s, "
                                        + "aggregates=[grouping=[], aggFunctions=[Count1AggFunction()]]]], "
                                        + "fields=[count1$0])",
                                tableName));
        CloseableIterator<Row> iterRows = tEnv.executeSql(query).collect();
        List<String> collected = collectRowsWithTimeout(iterRows, 1);
        List<String> expected = Collections.singletonList(String.format("+I[%s]", expectedRows));
        assertThat(collected).isEqualTo(expected);

        // test not push down grouping count.
        assertThatThrownBy(
                        () ->
                                tEnv.explainSql(
                                                String.format(
                                                        "SELECT COUNT(*) FROM %s group by id",
                                                        tableName))
                                        .wait())
                .hasMessageContaining(
                        "Currently, Fluss only support queries on table with datalake enabled or point queries on primary key when it's in batch execution mode.");

        // test not support primary key now
        String primaryTableName = prepareSourceTable(new String[] {"id"}, null);
        assertThatThrownBy(
                        () ->
                                tEnv.explainSql(
                                                String.format(
                                                        "SELECT COUNT(*) FROM %s ",
                                                        primaryTableName))
                                        .wait())
                .hasMessageContaining(
                        "Currently, Fluss only support queries on table with datalake enabled or point queries on primary key when it's in batch execution mode.");
    }
}
