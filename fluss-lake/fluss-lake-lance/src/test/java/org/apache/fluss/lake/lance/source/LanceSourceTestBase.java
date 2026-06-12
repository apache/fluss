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

package org.apache.fluss.lake.lance.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lance.LanceConfig;
import org.apache.fluss.lake.lance.utils.LanceDatasetAdapter;
import org.apache.fluss.metadata.TablePath;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** Base utilities for Lance source tests. */
class LanceSourceTestBase {

    protected static final String DEFAULT_DB = "fluss_lance";
    protected static final String DEFAULT_TABLE = "source_test_table";

    @TempDir protected File tempWarehouseDir;

    protected Configuration config;
    protected TablePath tablePath;
    protected Schema schema;
    protected LanceConfig lanceConfig;

    @BeforeEach
    void setupBase() {
        Assumptions.assumeTrue(
                isLanceJniAvailable(),
                "Skip Lance source tests because Lance JNI is unavailable for current runtime.");

        this.config = new Configuration();
        this.config.setString("warehouse", tempWarehouseDir.getAbsolutePath());

        this.tablePath = TablePath.of(DEFAULT_DB, DEFAULT_TABLE);
        this.lanceConfig =
                LanceConfig.from(
                        config.toMap(),
                        Collections.emptyMap(),
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());

        this.schema =
                new Schema(
                        Arrays.asList(
                                new Field(
                                        "id",
                                        FieldType.nullable(new ArrowType.Int(32, true)),
                                        null),
                                new Field(
                                        "name",
                                        FieldType.nullable(ArrowType.Utf8.INSTANCE),
                                        null),
                                new Field(
                                        BUCKET_COLUMN_NAME,
                                        FieldType.nullable(new ArrowType.Int(32, true)),
                                        null),
                                new Field(
                                        OFFSET_COLUMN_NAME,
                                        FieldType.nullable(new ArrowType.Int(64, true)),
                                        null),
                                new Field(
                                        TIMESTAMP_COLUMN_NAME,
                                        FieldType.nullable(new ArrowType.Int(64, true)),
                                        null)));
    }

    private static boolean isLanceJniAvailable() {
        try {
            Class.forName("com.lancedb.lance.Dataset", true, LanceSourceTestBase.class.getClassLoader());
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    protected void createEmptyDataset() {
        LanceDatasetAdapter.createDataset(
                lanceConfig.getDatasetUri(), schema, new WriteParams.Builder().build());
    }

    protected long appendRows(List<RowData> rows) {
        WriteParams writeParams = new WriteParams.Builder().build();

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector idVector = (IntVector) root.getVector("id");
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            IntVector bucketVector = (IntVector) root.getVector(BUCKET_COLUMN_NAME);
            BigIntVector offsetVector = (BigIntVector) root.getVector(OFFSET_COLUMN_NAME);
            BigIntVector tsVector = (BigIntVector) root.getVector(TIMESTAMP_COLUMN_NAME);

            for (int i = 0; i < rows.size(); i++) {
                RowData row = rows.get(i);
                idVector.setSafe(i, row.id);
                nameVector.setSafe(i, row.name.getBytes(StandardCharsets.UTF_8));
                bucketVector.setSafe(i, row.bucket);
                offsetVector.setSafe(i, row.offset);
                tsVector.setSafe(i, row.timestamp);
            }
            root.setRowCount(rows.size());

            List<FragmentMetadata> fragments =
                    Fragment.create(lanceConfig.getDatasetUri(), allocator, root, writeParams);
            return LanceDatasetAdapter.commitAppend(lanceConfig, fragments, Collections.emptyMap());
        }
    }

    protected Map<Integer, Integer> currentFragmentRowCount() {
        ReadOptions readOptions = LanceConfig.genReadOptionFromConfig(lanceConfig);
        Map<Integer, Integer> result = new HashMap<>();
        try (Dataset dataset = Dataset.open(lanceConfig.getDatasetUri(), readOptions)) {
            for (Fragment fragment : dataset.getFragments()) {
                result.put(fragment.getId(), fragment.countRows());
            }
        }
        return result;
    }

    protected void assertDatasetHasAtLeastFragments(int minFragmentCount) {
        ReadOptions readOptions = LanceConfig.genReadOptionFromConfig(lanceConfig);
        try (Dataset dataset = Dataset.open(lanceConfig.getDatasetUri(), readOptions)) {
            assertThat(dataset.getFragments().size()).isGreaterThanOrEqualTo(minFragmentCount);
        }
    }

    protected static RowData row(int id, String name, int bucket, long offset, long timestamp) {
        return new RowData(id, name, bucket, offset, timestamp);
    }

    protected static class RowData {
        private final int id;
        private final String name;
        private final int bucket;
        private final long offset;
        private final long timestamp;

        private RowData(int id, String name, int bucket, long offset, long timestamp) {
            this.id = id;
            this.name = name;
            this.bucket = bucket;
            this.offset = offset;
            this.timestamp = timestamp;
        }
    }
}
