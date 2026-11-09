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

package org.apache.fluss.client.table;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies that actual writer routing agrees with BucketCalculator. */
class BucketCalculatorITCase {

    private static final int BUCKET_COUNT = 7;

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    private Connection connection;
    private Admin admin;

    @BeforeEach
    void setUp() {
        connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
        admin = connection.getAdmin();
    }

    @AfterEach
    void tearDown() throws Exception {
        IOUtils.closeAll(admin, connection);
    }

    @Test
    void testAppendRoutesToCalculatedBucket() throws Exception {
        TablePath path = TablePath.of("fluss", "calculator_append");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .build();
        TableInfo tableInfo =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(BUCKET_COUNT, "id")
                                .build());
        BucketCalculator calculator = BucketCalculator.forBucketKeyRow(tableInfo);
        int expectedBucket =
                calculator.getBucketId(calculator.encodeKey(GenericRow.of(42)), BUCKET_COUNT);
        Map<Integer, Long> before = latestOffsets(tableInfo);
        try (Table table = connection.getTable(path)) {
            AppendWriter writer = table.newAppend().createWriter();
            writer.append(GenericRow.of(42, BinaryString.fromString("value")))
                    .get(30, TimeUnit.SECONDS);
        }
        Map<Integer, Long> after = latestOffsets(tableInfo);

        assertOnlyBucketChanged(before, after, expectedBucket);
    }

    @Test
    void testUpsertAndDeleteRouteToCalculatedBucket() throws Exception {
        for (int version : new int[] {1, 2}) {
            TablePath path = TablePath.of("fluss", "calculator_pk_v" + version);
            Schema schema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("payload", DataTypes.STRING())
                            .primaryKey("id")
                            .build();
            TableInfo tableInfo =
                    createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(schema)
                                    .distributedBy(BUCKET_COUNT)
                                    .property(ConfigOptions.TABLE_KV_FORMAT_VERSION, version)
                                    .build());
            BucketCalculator calculator = BucketCalculator.forBucketKeyRow(tableInfo);
            int expectedBucket =
                    calculator.getBucketId(
                            calculator.encodeKey(GenericRow.of(42)), tableInfo.getNumBuckets());
            TableBucket expectedTableBucket =
                    new TableBucket(tableInfo.getTableId(), expectedBucket);
            try (Table table = connection.getTable(path)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                GenericRow row = GenericRow.of(42, BinaryString.fromString("value"));
                assertThat(writer.upsert(row).get(30, TimeUnit.SECONDS).getBucket())
                        .isEqualTo(expectedTableBucket);
                assertThat(writer.delete(row).get(30, TimeUnit.SECONDS).getBucket())
                        .isEqualTo(expectedTableBucket);
            }
        }
    }

    @Test
    void testAppendWithoutBucketKeyPreservesRouting() throws Exception {
        TablePath path = TablePath.of("fluss", "calculator_no_bucket_key");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .build();
        TableInfo tableInfo =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(BUCKET_COUNT)
                                .build());
        assertThat(tableInfo.hasBucketKey()).isFalse();
        Map<Integer, Long> before = latestOffsets(tableInfo);
        try (Table table = connection.getTable(path)) {
            AppendWriter writer = table.newAppend().createWriter();
            writer.append(GenericRow.of(42, BinaryString.fromString("value")))
                    .get(30, TimeUnit.SECONDS);
        }
        Map<Integer, Long> after = latestOffsets(tableInfo);
        long totalIncrease = 0;
        for (Map.Entry<Integer, Long> entry : before.entrySet()) {
            int bucketId = entry.getKey();
            long delta = after.get(bucketId) - entry.getValue();
            assertThat(delta).isGreaterThanOrEqualTo(0);
            totalIncrease += delta;
        }
        // A log table without bucket keys must still use the existing non-hash bucket assigner.
        assertThat(totalIncrease).isEqualTo(1L);
    }

    @Test
    void testExplicitPrimaryKeyBucketRoutesToCalculatedBucket() throws Exception {
        TablePath path = TablePath.of("fluss", "calculator_explicit_pk");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("tenant", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("id", "tenant")
                        .build();
        TableInfo tableInfo =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(BUCKET_COUNT, "id")
                                .property(ConfigOptions.TABLE_KV_FORMAT_VERSION, 2)
                                .build());

        assertThat(tableInfo.isDefaultBucketKey()).isFalse();
        assertThat(tableInfo.getBucketKeys()).containsExactly("id");

        BucketCalculator calculator = BucketCalculator.forBucketKeyRow(tableInfo);
        int expectedBucket =
                calculator.getBucketId(
                        calculator.encodeKey(GenericRow.of(42)), tableInfo.getNumBuckets());
        TableBucket expectedTableBucket = new TableBucket(tableInfo.getTableId(), expectedBucket);
        try (Table table = connection.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            GenericRow row = GenericRow.of(42, 73, BinaryString.fromString("value"));

            assertThat(writer.upsert(row).get(30, TimeUnit.SECONDS).getBucket())
                    .isEqualTo(expectedTableBucket);
            assertThat(writer.delete(row).get(30, TimeUnit.SECONDS).getBucket())
                    .isEqualTo(expectedTableBucket);
        }
    }

    private TableInfo createTable(TablePath path, TableDescriptor descriptor) throws Exception {
        admin.createTable(path, descriptor, false).get(30, TimeUnit.SECONDS);
        TableInfo tableInfo = admin.getTableInfo(path).get(30, TimeUnit.SECONDS);
        for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) {
            CLUSTER.waitUntilAllReplicaReady(new TableBucket(tableInfo.getTableId(), bucketId));
        }
        return tableInfo;
    }

    private Map<Integer, Long> latestOffsets(TableInfo tableInfo) throws Exception {
        List<Integer> bucketIds =
                IntStream.range(0, tableInfo.getNumBuckets()).boxed().collect(Collectors.toList());
        return admin.listOffsets(tableInfo.getTablePath(), bucketIds, new OffsetSpec.LatestSpec())
                .all()
                .get(30, TimeUnit.SECONDS);
    }

    private static void assertOnlyBucketChanged(
            Map<Integer, Long> before, Map<Integer, Long> after, int expectedBucket) {
        assertThat(after.keySet()).containsExactlyInAnyOrderElementsOf(before.keySet());
        for (Map.Entry<Integer, Long> entry : before.entrySet()) {
            int bucketId = entry.getKey();
            long delta = after.get(bucketId) - entry.getValue();
            assertThat(delta)
                    .as("Offset increase in bucket %s", bucketId)
                    .isEqualTo(bucketId == expectedBucket ? 1L : 0L);
        }
    }
}
