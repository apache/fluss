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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lance.LanceConfig;
import org.apache.fluss.lake.lance.testutils.FlinkLanceTieringTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import com.lancedb.lance.Dataset;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for tiering Fluss {@code VECTOR(n)} columns to Lance.
 *
 * <p>Verifies that VECTOR columns are written as Arrow {@code FixedSizeList<Float32>(n)} in Lance,
 * with correct float values and null handling.
 */
class LanceVectorTieringITCase extends FlinkLanceTieringTestBase {

    private static final int DIMENSION = 4;
    private static final String DEFAULT_DB = "fluss";

    private static Configuration lanceConf;
    private static final RootAllocator allocator = new RootAllocator();

    @BeforeAll
    protected static void beforeAll() {
        FlinkLanceTieringTestBase.beforeAll();
        lanceConf = Configuration.fromMap(getLanceCatalogConf());
    }

    /**
     * Creates a log table with schema {@code (id BIGINT NOT NULL, embedding VECTOR(4))} backed by
     * Lance, then writes 5 rows with known float values, triggers tiering, opens the resulting
     * Lance dataset, and asserts:
     *
     * <ul>
     *   <li>The {@code embedding} column schema is {@code FixedSizeList<Float32>(4)}.
     *   <li>All 5 rows have the expected float values (bit-exact comparison).
     * </ul>
     */
    @Test
    void testVectorTiering() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "vectorTable");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("embedding", DataTypes.VECTOR(DIMENSION))
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "id")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .build();

        long tableId = createTable(tablePath, descriptor);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        // Write 5 rows: embedding = [i*1.0, i*1.1, i*1.2, i*1.3]
        float[][] expectedData = {
            {1.0f, 1.1f, 1.2f, 1.3f},
            {2.0f, 2.1f, 2.2f, 2.3f},
            {3.0f, 3.1f, 3.2f, 3.3f},
            {4.0f, 4.1f, 4.2f, 4.3f},
            {5.0f, 5.1f, 5.2f, 5.3f}
        };

        List<InternalRow> rows =
                Arrays.<InternalRow>asList(
                        buildVectorRow(0L, expectedData[0]),
                        buildVectorRow(1L, expectedData[1]),
                        buildVectorRow(2L, expectedData[2]),
                        buildVectorRow(3L, expectedData[3]),
                        buildVectorRow(4L, expectedData[4]));

        writeRows(tablePath, rows, true);

        // Start tiering job and wait for replication
        JobClient jobClient = buildTieringJob(execEnv);
        assertReplicaStatus(tableBucket, 5);

        LanceConfig config =
                LanceConfig.from(
                        lanceConf.toMap(),
                        Collections.emptyMap(),
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());

        try (Dataset dataset =
                Dataset.open(
                        allocator,
                        config.getDatasetUri(),
                        LanceConfig.genReadOptionFromConfig(config))) {

            // Assert schema: embedding column must be FixedSizeList<Float32>(DIMENSION)
            org.apache.arrow.vector.types.pojo.Field embeddingField =
                    dataset.getSchema().findField("embedding");
            assertThat(embeddingField).isNotNull();
            assertThat(embeddingField.getType()).isInstanceOf(ArrowType.FixedSizeList.class);
            assertThat(((ArrowType.FixedSizeList) embeddingField.getType()).getListSize())
                    .isEqualTo(DIMENSION);

            // Assert child is Float32
            assertThat(embeddingField.getChildren()).hasSize(1);
            assertThat(embeddingField.getChildren().get(0).getType())
                    .isInstanceOf(ArrowType.FloatingPoint.class);

            // Read and assert data values
            ArrowReader reader = dataset.newScan().scanBatches();
            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
            assertThat(reader.loadNextBatch()).isTrue();

            assertThat(readerRoot.getRowCount()).isEqualTo(5);
            FixedSizeListVector embeddingVector =
                    (FixedSizeListVector) readerRoot.getVector("embedding");
            assertThat(embeddingVector.getListSize()).isEqualTo(DIMENSION);

            for (int i = 0; i < 5; i++) {
                assertThat(embeddingVector.isNull(i)).isFalse();
                List<?> values = embeddingVector.getObject(i);
                assertThat(values).hasSize(DIMENSION);
                for (int j = 0; j < DIMENSION; j++) {
                    assertThat((Float) values.get(j))
                            .as("Row %d, element %d", i, j)
                            .isEqualTo(expectedData[i][j]);
                }
            }
        }

        jobClient.cancel().get();
    }

    /**
     * Creates a log table with a VECTOR(4) column, writes rows where some have null embeddings,
     * triggers tiering, and asserts that the Lance dataset correctly represents null FixedSizeList
     * rows (isNull returns true).
     */
    @Test
    void testNullVectorTiering() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "nullVectorTable");

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("embedding", DataTypes.VECTOR(DIMENSION))
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "id")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .build();

        long tableId = createTable(tablePath, descriptor);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        // Write 3 rows: non-null, null, non-null
        GenericRow row0 = buildVectorRow(0L, new float[] {1.0f, 2.0f, 3.0f, 4.0f});
        GenericRow row1 = new GenericRow(2); // id=1, embedding=null
        row1.setField(0, 1L);
        row1.setField(1, null);
        GenericRow row2 = buildVectorRow(2L, new float[] {5.0f, 6.0f, 7.0f, 8.0f});

        writeRows(tablePath, Arrays.<InternalRow>asList(row0, row1, row2), true);

        // Start tiering job and wait for replication
        JobClient jobClient = buildTieringJob(execEnv);
        assertReplicaStatus(tableBucket, 3);

        LanceConfig config =
                LanceConfig.from(
                        lanceConf.toMap(),
                        Collections.emptyMap(),
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());

        try (Dataset dataset =
                Dataset.open(
                        allocator,
                        config.getDatasetUri(),
                        LanceConfig.genReadOptionFromConfig(config))) {

            ArrowReader reader = dataset.newScan().scanBatches();
            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
            assertThat(reader.loadNextBatch()).isTrue();

            assertThat(readerRoot.getRowCount()).isEqualTo(3);
            FixedSizeListVector embeddingVector =
                    (FixedSizeListVector) readerRoot.getVector("embedding");

            // Row 0: non-null, [1.0, 2.0, 3.0, 4.0]
            assertThat(embeddingVector.isNull(0)).isFalse();
            List<?> v0 = embeddingVector.getObject(0);
            assertThat((Float) v0.get(0)).isEqualTo(1.0f);
            assertThat((Float) v0.get(1)).isEqualTo(2.0f);
            assertThat((Float) v0.get(2)).isEqualTo(3.0f);
            assertThat((Float) v0.get(3)).isEqualTo(4.0f);

            // Row 1: null embedding
            assertThat(embeddingVector.isNull(1)).isTrue();

            // Row 2: non-null, [5.0, 6.0, 7.0, 8.0]
            assertThat(embeddingVector.isNull(2)).isFalse();
            List<?> v2 = embeddingVector.getObject(2);
            assertThat((Float) v2.get(0)).isEqualTo(5.0f);
            assertThat((Float) v2.get(1)).isEqualTo(6.0f);
            assertThat((Float) v2.get(2)).isEqualTo(7.0f);
            assertThat((Float) v2.get(3)).isEqualTo(8.0f);
        }

        jobClient.cancel().get();
    }

    private static GenericRow buildVectorRow(long id, float[] embedding) {
        GenericRow row = new GenericRow(2);
        row.setField(0, id);
        row.setField(1, new GenericArray(embedding));
        return row;
    }
}
