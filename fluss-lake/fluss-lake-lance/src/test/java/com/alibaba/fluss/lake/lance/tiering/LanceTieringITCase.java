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
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.Transaction;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static com.alibaba.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for tiering tables to lance. */
class LanceTieringITCase extends FlinkLanceTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";
    private static StreamExecutionEnvironment execEnv;
    private static Configuration lanceConf;
    private static final RootAllocator allocator = new RootAllocator();

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
        // create log table
        TablePath t1 = TablePath.of(DEFAULT_DB, "logTable");
        long t1Id = createLogTable(t1);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);
        List<InternalRow> flussRows = new ArrayList<>();
        // write records
        for (int i = 0; i < 10; i++) {
            List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
            flussRows.addAll(rows);
            // write records
            writeRows(t1, rows, true);
        }

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        // check the status of replica after synced;
        // note: we can't update log start offset for unaware bucket mode log table
        assertReplicaStatus(t1Bucket, 30);

        LanceConfig config =
                LanceConfig.from(
                        lanceConf.toMap(),
                        Collections.emptyMap(),
                        t1.getDatabaseName(),
                        t1.getTableName());

        // check data in lance
        checkDataInLanceAppendOnlyTable(config, flussRows);
        // check snapshot property in lance
        Map<String, String> properties =
                new HashMap<String, String>() {
                    {
                        put(
                                FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                "[{\"bucket_id\":0,\"log_offset\":30}]");
                        put("commit-user", FLUSS_LAKE_TIERING_COMMIT_USER);
                    }
                };
        checkSnapshotPropertyInLance(config, properties);

        jobClient.cancel().get();
    }

    private void checkSnapshotPropertyInLance(
            LanceConfig config, Map<String, String> expectedProperties) throws Exception {
        ReadOptions.Builder builder = new ReadOptions.Builder();
        builder.setStorageOptions(LanceConfig.genStorageOptions(config));
        try (Dataset dataset = Dataset.open(allocator, config.getDatasetUri(), builder.build())) {
            Transaction transaction = dataset.readTransaction().orElse(null);
            assertThat(transaction).isNotNull();
            assertThat(transaction.transactionProperties()).isEqualTo(expectedProperties);
        }
    }

    private void checkDataInLanceAppendOnlyTable(LanceConfig config, List<InternalRow> expectedRows)
            throws Exception {
        try (Dataset dataset =
                Dataset.open(
                        allocator,
                        config.getDatasetUri(),
                        LanceConfig.genReadOptionFromConfig(config))) {
            ArrowReader reader = dataset.newScan().scanBatches();
            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
            reader.loadNextBatch();
            Iterator<InternalRow> flussRowIterator = expectedRows.iterator();
            int rowCount = readerRoot.getRowCount();
            for (int i = 0; i < rowCount; i++) {
                InternalRow flussRow = flussRowIterator.next();
                assertThat((int) (readerRoot.getVector(0).getObject(i)))
                        .isEqualTo(flussRow.getInt(0));
                assertThat(((VarCharVector) readerRoot.getVector(1)).getObject(i).toString())
                        .isEqualTo(flussRow.getString(1).toString());
            }
            assertThat(reader.loadNextBatch()).isFalse();
            assertThat(flussRowIterator.hasNext()).isFalse();
        }
    }
}
