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

package org.apache.fluss.lake.delta.tiering;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.delta.DeltaConfig;
import org.apache.fluss.lake.delta.testutils.DeltaTestUtils;
import org.apache.fluss.lake.delta.testutils.FlinkDeltaTieringTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.engine.Engine;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for tiering Fluss log tables to Delta Lake. */
class DeltaTieringITCase extends FlinkDeltaTieringTestBase {

    private static StreamExecutionEnvironment execEnv;
    private static Engine engine;

    @BeforeAll
    protected static void beforeAll() {
        FlinkDeltaTieringTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
        engine = DeltaConfig.createEngine(deltaConf());
    }

    @Test
    void testTieringLogTable() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "log_table");
        long tableId = createLogTable(tablePath, 1, false);
        TableBucket bucket = new TableBucket(tableId, 0);

        List<String> expectedRows = new ArrayList<>();
        writeRows(tablePath, genRows(0, 30, null, expectedRows));

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(bucket, 30);
            String deltaPath = deltaTablePath(tablePath);
            assertThat(DeltaTestUtils.readRows(engine, deltaPath))
                    .containsExactlyInAnyOrderElementsOf(expectedRows);
            assertOffsetsInDelta(deltaPath, Collections.singletonMap(bucket, 30L));

            // the next tiering round appends to the same Delta table
            writeRows(tablePath, genRows(30, 30, null, expectedRows));
            assertReplicaStatus(bucket, 60);
            assertThat(DeltaTestUtils.readRows(engine, deltaPath))
                    .containsExactlyInAnyOrderElementsOf(expectedRows);
            assertOffsetsInDelta(deltaPath, Collections.singletonMap(bucket, 60L));
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testTieringPartitionedLogTable() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "partitioned_log_table");
        long tableId = createLogTable(tablePath, 1, true);
        Map<String, Long> partitions =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);

        List<String> expectedRows = new ArrayList<>();
        Map<TableBucket, Long> expectedOffsets = new HashMap<>();
        for (Map.Entry<String, Long> partition : partitions.entrySet()) {
            writeRows(tablePath, genRows(0, 10, partition.getKey(), expectedRows));
            expectedOffsets.put(new TableBucket(tableId, partition.getValue(), 0), 10L);
        }

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            for (TableBucket bucket : expectedOffsets.keySet()) {
                assertReplicaStatus(bucket, 10);
            }
            String deltaPath = deltaTablePath(tablePath);
            Snapshot snapshot = TableManager.loadSnapshot(deltaPath).build(engine);
            assertThat(snapshot.getPartitionColumnNames()).containsExactly("c");
            assertThat(DeltaTestUtils.readRows(engine, deltaPath))
                    .containsExactlyInAnyOrderElementsOf(expectedRows);
            assertOffsetsInDelta(deltaPath, expectedOffsets);
        } finally {
            jobClient.cancel().get();
        }
    }

    private static List<InternalRow> genRows(
            int start, int count, @Nullable String partition, List<String> expectedRows) {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = start; i < start + count; i++) {
            GenericRow address = GenericRow.of(BinaryString.fromString("city" + i), 10000 + i);
            GenericArray tags =
                    new GenericArray(
                            new Object[] {
                                BinaryString.fromString("t" + i), BinaryString.fromString("x")
                            });
            TimestampLtz ts = TimestampLtz.fromEpochMillis(1_700_000_000_000L + i);
            TimestampNtz tsNtz = TimestampNtz.fromMicros(1_600_000_000_000_000L + i);
            String expected =
                    String.format(
                            "+I[%d, v%d, [t%d, x], (city%d, %d), %d, %d",
                            i,
                            i,
                            i,
                            i,
                            10000 + i,
                            (1_700_000_000_000L + i) * 1000,
                            1_600_000_000_000_000L + i);
            if (partition == null) {
                rows.add(
                        GenericRow.of(
                                i, BinaryString.fromString("v" + i), tags, address, ts, tsNtz));
                expectedRows.add(expected + "]");
            } else {
                rows.add(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("v" + i),
                                tags,
                                address,
                                ts,
                                tsNtz,
                                BinaryString.fromString(partition)));
                expectedRows.add(expected + ", " + partition + "]");
            }
        }
        return rows;
    }

    private static void assertOffsetsInDelta(
            String deltaPath, Map<TableBucket, Long> expectedOffsets) throws Exception {
        Snapshot snapshot = TableManager.loadSnapshot(deltaPath).build(engine);
        String domainMetadata =
                snapshot.getDomainMetadata(DeltaLakeCommitter.FLUSS_DOMAIN).orElse(null);
        assertThat(domainMetadata).isNotNull();
        Map<String, String> properties =
                new ObjectMapper()
                        .readValue(domainMetadata, new TypeReference<Map<String, String>>() {});
        String offsetFile = properties.get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
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
