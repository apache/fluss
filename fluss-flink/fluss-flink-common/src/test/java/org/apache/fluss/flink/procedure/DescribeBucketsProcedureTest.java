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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.flink.sink.testutils.TestAdminAdapter;
import org.apache.fluss.metadata.BucketInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DescribeBucketsProcedure}. */
class DescribeBucketsProcedureTest {

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");

    @Test
    void testDescribeAllBucketsAndConvertNullableFields() throws Exception {
        BucketInfo bucketInfo =
                new BucketInfo(
                        TABLE_PATH,
                        10L,
                        null,
                        null,
                        0,
                        null,
                        null,
                        null,
                        Arrays.asList(1, 2, 3),
                        Collections.emptyList());
        TestingAdmin admin = new TestingAdmin(Collections.singletonList(bucketInfo));
        DescribeBucketsProcedure procedure = newProcedure(admin);

        Row[] rows = procedure.call(null, " test_db.test_table ");

        assertThat(admin.requestedTablePath).isEqualTo(TABLE_PATH);
        assertThat(admin.requestedPartitionSpec).isNull();
        assertThat(rows).hasSize(1);
        Row row = rows[0];
        assertThat(row.getArity()).isEqualTo(10);
        assertThat(row.getField(0)).isEqualTo("test_db.test_table");
        assertThat(row.getField(1)).isEqualTo(10L);
        assertThat(row.getField(2)).isNull();
        assertThat(row.getField(3)).isNull();
        assertThat(row.getField(4)).isEqualTo(0);
        assertThat(row.getField(5)).isNull();
        assertThat(row.getField(6)).isNull();
        assertThat(row.getField(7)).isNull();
        assertThat((Integer[]) row.getField(8)).containsExactly(1, 2, 3);
        assertThat((Integer[]) row.getField(9)).isEmpty();
    }

    @Test
    void testDescribeBucketsWithPartitionSpec() throws Exception {
        BucketInfo bucketInfo =
                new BucketInfo(
                        TABLE_PATH,
                        10L,
                        100L,
                        "cn$2026-09-16",
                        1,
                        2,
                        3,
                        -1,
                        Arrays.asList(1, 2, 3),
                        Arrays.asList(2, 3));
        TestingAdmin admin = new TestingAdmin(Collections.singletonList(bucketInfo));
        DescribeBucketsProcedure procedure = newProcedure(admin);

        Row[] rows = procedure.call(null, "test_db.test_table", "region=cn/dt=2026-09-16");

        assertThat(admin.requestedTablePath).isEqualTo(TABLE_PATH);
        assertThat(admin.requestedPartitionSpec).isNotNull();
        assertThat(admin.requestedPartitionSpec.getSpecMap())
                .containsEntry("region", "cn")
                .containsEntry("dt", "2026-09-16");
        assertThat(rows).hasSize(1);
        Row row = rows[0];
        assertThat(row.getField(2)).isEqualTo(100L);
        assertThat(row.getField(3)).isEqualTo("cn$2026-09-16");
        assertThat(row.getField(4)).isEqualTo(1);
        assertThat(row.getField(5)).isEqualTo(2);
        assertThat(row.getField(6)).isEqualTo(3);
        assertThat(row.getField(7)).isEqualTo(-1);
        assertThat((Integer[]) row.getField(8)).containsExactly(1, 2, 3);
        assertThat((Integer[]) row.getField(9)).containsExactly(2, 3);
    }

    @Test
    void testEmptyAdminResult() throws Exception {
        DescribeBucketsProcedure procedure =
                newProcedure(new TestingAdmin(Collections.emptyList()));

        assertThat(procedure.call(null, "test_db.test_table")).isEmpty();
    }

    @Test
    void testInvalidTablePath() {
        DescribeBucketsProcedure procedure =
                newProcedure(new TestingAdmin(Collections.emptyList()));

        for (String tablePath :
                Arrays.asList(null, "", " ", "test_db", ".test_table", "test_db.", "a.b.c")) {
            assertThatThrownBy(() -> procedure.call(null, tablePath))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("table_path");
        }
    }

    @Test
    void testInvalidPartitionSpec() {
        DescribeBucketsProcedure procedure =
                newProcedure(new TestingAdmin(Collections.emptyList()));

        for (String partitionSpec :
                Arrays.asList(null, "", " ", "region", "=cn", "region=cn/", "region=cn//dt=d1")) {
            assertThatThrownBy(() -> procedure.call(null, "test_db.test_table", partitionSpec))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("partition_spec");
        }

        assertThatThrownBy(() -> procedure.call(null, "test_db.test_table", "region=cn/region=us"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate partition key 'region'");
    }

    private static DescribeBucketsProcedure newProcedure(TestingAdmin admin) {
        DescribeBucketsProcedure procedure = new DescribeBucketsProcedure();
        procedure.withAdmin(admin);
        return procedure;
    }

    private static class TestingAdmin extends TestAdminAdapter {
        private final List<BucketInfo> bucketInfos;
        private @Nullable TablePath requestedTablePath;
        private @Nullable PartitionSpec requestedPartitionSpec;

        private TestingAdmin(List<BucketInfo> bucketInfos) {
            this.bucketInfos = bucketInfos;
        }

        @Override
        public CompletableFuture<List<BucketInfo>> describeBuckets(TablePath tablePath) {
            requestedTablePath = tablePath;
            requestedPartitionSpec = null;
            return CompletableFuture.completedFuture(bucketInfos);
        }

        @Override
        public CompletableFuture<List<BucketInfo>> describeBuckets(
                TablePath tablePath, PartitionSpec partitionSpec) {
            requestedTablePath = tablePath;
            requestedPartitionSpec = partitionSpec;
            return CompletableFuture.completedFuture(bucketInfos);
        }
    }
}
