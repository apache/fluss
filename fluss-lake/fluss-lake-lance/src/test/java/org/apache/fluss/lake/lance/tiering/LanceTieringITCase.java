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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.zk.data.lake.LakeTable;

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
import java.util.Iterator;
import java.util.List;
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
        // create log table with array columns
        TablePath t1 = TablePath.of(DEFAULT_DB, "logTable");
        long t1Id = createLogTableWithArrayColumns(t1);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);
        List<InternalRow> flussRows = new ArrayList<>();
        // write records with array data
        for (int i = 0; i < 10; i++) {
            List<InternalRow> rows =
                    Arrays.asList(
                            row(1, "v1", new String[] {"tag1", "tag2"}, new int[] {10, 20}),
                            row(2, "v2", new String[] {"tag3"}, new int[] {30, 40, 50}),
                            row(3, "v3", new String[] {"tag4", "tag5", "tag6"}, new int[] {60}));
            flussRows.addAll(rows);
            // write records
            writeRows(t1, rows, true);
        }

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        // check the status of replica after synced
        assertReplicaStatus(t1Bucket, 30);

        LanceConfig config =
                LanceConfig.from(
                        lanceConf.toMap(),
                        Collections.emptyMap(),
                        t1.getDatabaseName(),
                        t1.getTableName());

        // check data in lance, including array columns
        checkDataInLanceAppendOnlyTableWithArrays(config, flussRows);
        checkSnapshotPropertyInLance(config, Collections.singletonMap(t1Bucket, 30L));

        jobClient.cancel().get();
    }

    @Test
    void testTieringWithFloatArrayForVectorEmbeddings() throws Exception {
        // create log table with float array column for vector embeddings
        TablePath t1 = TablePath.of(DEFAULT_DB, "vectorTable");
        long t1Id = createLogTableWithVectorColumn(t1);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);
        List<InternalRow> flussRows = new ArrayList<>();
        // write records with float array data representing vector embeddings
        for (int i = 0; i < 10; i++) {
            List<InternalRow> rows =
                    Arrays.asList(
                            row(1, "doc1", new float[] {0.1f, 0.2f, 0.3f, 0.4f}),
                            row(2, "doc2", new float[] {0.5f, 0.6f, 0.7f, 0.8f}),
                            row(3, "doc3", new float[] {0.9f, 1.0f, 1.1f, 1.2f}));
            flussRows.addAll(rows);
            writeRows(t1, rows, true);
        }

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        // check the status of replica after synced
        assertReplicaStatus(t1Bucket, 30);

        LanceConfig config =
                LanceConfig.from(
                        lanceConf.toMap(),
                        Collections.emptyMap(),
                        t1.getDatabaseName(),
                        t1.getTableName());

        // check vector data in lance
        checkVectorDataInLance(config, flussRows);
        checkSnapshotPropertyInLance(config, Collections.singletonMap(t1Bucket, 30L));

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

    private void checkDataInLanceAppendOnlyTableWithArrays(
            LanceConfig config, List<InternalRow> expectedRows) throws Exception {
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

                org.apache.arrow.vector.complex.ListVector tagsVector =
                        (org.apache.arrow.vector.complex.ListVector) readerRoot.getVector(2);
                java.util.List<?> tagsFromLance = (java.util.List<?>) tagsVector.getObject(i);
                assertThat(tagsFromLance).isNotNull();
                assertThat(tagsFromLance.size()).isEqualTo(flussRow.getArray(2).size());

                org.apache.arrow.vector.complex.ListVector scoresVector =
                        (org.apache.arrow.vector.complex.ListVector) readerRoot.getVector(3);
                java.util.List<?> scoresFromLance = (java.util.List<?>) scoresVector.getObject(i);
                assertThat(scoresFromLance).isNotNull();
                assertThat(scoresFromLance.size()).isEqualTo(flussRow.getArray(3).size());
            }
            assertThat(reader.loadNextBatch()).isFalse();
            assertThat(flussRowIterator.hasNext()).isFalse();
        }
    }

    private void checkVectorDataInLance(LanceConfig config, List<InternalRow> expectedRows)
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

                org.apache.arrow.vector.complex.ListVector embeddingVector =
                        (org.apache.arrow.vector.complex.ListVector) readerRoot.getVector(2);
                java.util.List<?> embeddingFromLance =
                        (java.util.List<?>) embeddingVector.getObject(i);
                assertThat(embeddingFromLance).isNotNull();

                float[] expectedEmbedding = flussRow.getArray(2).toFloatArray();
                assertThat(embeddingFromLance.size()).isEqualTo(expectedEmbedding.length);

                for (int j = 0; j < expectedEmbedding.length; j++) {
                    assertThat(((Number) embeddingFromLance.get(j)).floatValue())
                            .isCloseTo(
                                    expectedEmbedding[j],
                                    org.assertj.core.data.Offset.offset(0.001f));
                }
            }
            assertThat(reader.loadNextBatch()).isFalse();
            assertThat(flussRowIterator.hasNext()).isFalse();
        }
    }
}
