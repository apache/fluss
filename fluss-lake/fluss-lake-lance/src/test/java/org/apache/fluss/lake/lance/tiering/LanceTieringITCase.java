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

package org.apache.fluss.lake.lance.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.lance.LanceConfig;
import org.apache.fluss.lake.lance.testutils.FlinkLanceTieringTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.server.zk.data.lake.LakeTable;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.Transaction;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.testutils.DataTestUtils.row;
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
        // Test 1: Basic log table with INT and STRING columns
        TablePath t1 = TablePath.of(DEFAULT_DB, "logTable");
        long t1Id = createLogTable(t1);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);

        // write records
        for (int i = 0; i < 10; i++) {
            writeRows(t1, Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3")), true);
        }

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        // check the status of replica after synced
        assertReplicaStatus(t1Bucket, 30);

        LanceConfig config1 =
                LanceConfig.from(
                        lanceConf.toMap(),
                        Collections.emptyMap(),
                        t1.getDatabaseName(),
                        t1.getTableName());

        // check data in lance using TSV string comparison
        String expectedTsv1 = buildExpectedTsvForBasicTable(30);
        checkDataInLance(config1, expectedTsv1);
        checkSnapshotPropertyInLance(config1, Collections.singletonMap(t1Bucket, 30L));

        // Test 2: Log table with multiple array type columns (STRING, INT, FLOAT)
        TablePath t2 = TablePath.of(DEFAULT_DB, "logTableWithArrays");
        long t2Id = createLogTableWithAllArrayTypes(t2);
        TableBucket t2Bucket = new TableBucket(t2Id, 0);

        // write records with various array types
        for (int i = 0; i < 10; i++) {
            writeRows(
                    t2,
                    Arrays.asList(
                            row(
                                    1,
                                    "v1",
                                    new String[] {"tag1", "tag2"},
                                    new int[] {10, 20},
                                    new GenericArray(new float[] {0.1f, 0.2f, 0.3f, 0.4f})),
                            row(
                                    2,
                                    "v2",
                                    new String[] {"tag3"},
                                    new int[] {30, 40, 50},
                                    new GenericArray(new float[] {0.5f, 0.6f, 0.7f, 0.8f})),
                            row(
                                    3,
                                    "v3",
                                    new String[] {"tag4", "tag5", "tag6"},
                                    new int[] {60},
                                    new GenericArray(new float[] {0.9f, 1.0f, 1.1f, 1.2f}))),
                    true);
        }

        // check the status of replica after synced
        assertReplicaStatus(t2Bucket, 30);

        LanceConfig config2 =
                LanceConfig.from(
                        lanceConf.toMap(),
                        Collections.emptyMap(),
                        t2.getDatabaseName(),
                        t2.getTableName());

        // check data in lance using TSV string comparison
        String expectedTsv2 = buildExpectedTsvForArrayTable(30);
        checkDataInLance(config2, expectedTsv2);
        checkSnapshotPropertyInLance(config2, Collections.singletonMap(t2Bucket, 30L));

        jobClient.cancel().get();
    }

    private void checkSnapshotPropertyInLance(
            LanceConfig config, Map<TableBucket, Long> expectedOffsets) throws Exception {
        ReadOptions.Builder builder = new ReadOptions.Builder();
        builder.setStorageOptions(LanceConfig.genStorageOptions(config));
        try (Dataset dataset = Dataset.open(allocator, config.getDatasetUri(), builder.build())) {
            Transaction transaction = dataset.readTransaction().orElse(null);
            assertThat(transaction).isNotNull();
            String offsetFile =
                    transaction.transactionProperties().get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
            Map<TableBucket, Long> recordedOffsets =
                    new LakeTable(
                                    new LakeTable.LakeSnapshotMetadata(
                                            // don't care about snapshot id
                                            -1, new FsPath(offsetFile), null))
                            .getOrReadLatestTableSnapshot()
                            .getBucketLogEndOffset();
            assertThat(recordedOffsets).isEqualTo(expectedOffsets);
        }
    }

    private void checkDataInLance(LanceConfig config, String expectedTsv) throws Exception {
        try (Dataset dataset =
                Dataset.open(
                        allocator,
                        config.getDatasetUri(),
                        LanceConfig.genReadOptionFromConfig(config))) {
            ArrowReader reader = dataset.newScan().scanBatches();
            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
            StringBuilder actualTsvBuilder = new StringBuilder();
            boolean isFirstBatch = true;
            while (reader.loadNextBatch()) {
                String batchTsv = readerRoot.contentToTSVString();
                if (isFirstBatch) {
                    actualTsvBuilder.append(batchTsv);
                    isFirstBatch = false;
                } else {
                    // Skip header line for subsequent batches
                    int firstNewline = batchTsv.indexOf('\n');
                    if (firstNewline >= 0 && firstNewline < batchTsv.length() - 1) {
                        actualTsvBuilder.append(batchTsv.substring(firstNewline + 1));
                    }
                }
            }
            assertThat(actualTsvBuilder.toString()).isEqualTo(expectedTsv);
        }
    }

    private String buildExpectedTsvForBasicTable(int rowCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("a\tb\n");
        for (int i = 0; i < rowCount / 3; i++) {
            sb.append("1\tv1\n");
            sb.append("2\tv2\n");
            sb.append("3\tv3\n");
        }
        return sb.toString();
    }

    private String buildExpectedTsvForArrayTable(int rowCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("a\tb\ttags\tscores\tembedding\n");
        for (int i = 0; i < rowCount / 3; i++) {
            sb.append("1\tv1\t[\"tag1\",\"tag2\"]\t[10,20]\t[0.1,0.2,0.3,0.4]\n");
            sb.append("2\tv2\t[\"tag3\"]\t[30,40,50]\t[0.5,0.6,0.7,0.8]\n");
            sb.append("3\tv3\t[\"tag4\",\"tag5\",\"tag6\"]\t[60]\t[0.9,1.0,1.1,1.2]\n");
        }
        return sb.toString();
    }
}
