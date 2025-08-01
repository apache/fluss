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

package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.lance.LanceConfig;
import com.alibaba.fluss.lake.lance.testutils.FlinkLanceTieringTestBase;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for tiering tables to lance. */
public class LanceTieringITCase extends FlinkLanceTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";
    private static StreamExecutionEnvironment execEnv;
    private static Configuration lanceConf;

    @BeforeAll
    protected static void beforeAll() {
        FlinkLanceTieringTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
        lanceConf = Configuration.fromMap(getLanceCatalogConf());
    }

    @Test
    void testTiering() throws Exception {
        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));

        // create log table
        TablePath t1 = TablePath.of(DEFAULT_DB, "logTable");
        long t1Id = createLogTable(t1);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);
        List<InternalRow> flussRows = new ArrayList<>();
        // write records
        for (int i = 0; i < 10; i++) {
            rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
            flussRows.addAll(rows);
            // write records
            writeRows(t1, rows, true);
        }

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        // check the status of replica after synced;
        // note: we can't update log start offset for unaware bucket mode log table
        assertReplicaStatus(t1Bucket, 30);

        // check data in lance
        checkDataInLanceAppendOnlyTable(t1, flussRows, 0);

        jobClient.cancel().get();
    }

    private void checkDataInLanceAppendOnlyTable(
            TablePath tablePath, List<InternalRow> expectedRows, long startingOffset)
            throws Exception {
        LanceConfig config =
                LanceConfig.from(
                        lanceConf.toMap(), tablePath.getDatabaseName(), tablePath.getTableName());
        ArrowReader reader = LanceDatasetAdapter.getArrowReader(config);
        VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        //        while (reader.loadNextBatch()) {
        //            System.out.print(readerRoot.contentToTSVString());
        //        }
        reader.loadNextBatch();
        Iterator<InternalRow> flussRowIterator = expectedRows.iterator();
        int rowCount = readerRoot.getRowCount();
        for (int i = 0; i < rowCount; i++) {
            InternalRow flussRow = flussRowIterator.next();
            assertThat((int) (readerRoot.getVector(0).getObject(i))).isEqualTo(flussRow.getInt(0));
            assertThat(((VarCharVector) readerRoot.getVector(1)).getObject(i).toString())
                    .isEqualTo(flussRow.getString(1).toString());
            // the idx 2 is __bucket, so use 3
            assertThat((long) (readerRoot.getVector(3).getObject(i))).isEqualTo(startingOffset++);
        }
        assertThat(reader.loadNextBatch()).isFalse();
        assertThat(flussRowIterator.hasNext()).isFalse();
    }
}
