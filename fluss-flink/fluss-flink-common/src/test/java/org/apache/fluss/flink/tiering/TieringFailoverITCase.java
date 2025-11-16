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

package org.apache.fluss.flink.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.tiering.committer.CommittableMessageTypeInfo;
import org.apache.fluss.flink.tiering.committer.TieringCommitOperatorFactory;
import org.apache.fluss.flink.tiering.source.TableBucketWriteResultTypeInfo;
import org.apache.fluss.flink.tiering.source.TieringSource;
import org.apache.fluss.lake.values.ValuesLake;
import org.apache.fluss.lake.values.tiering.ValuesLakeTieringFactory;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.TypeUtils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.tiering.source.TieringSource.TIERING_SOURCE_TRANSFORMATION_UID;
import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static org.apache.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test tiering failover.
 */
class TieringFailoverITCase extends FlinkValuesTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;

    private static final Schema pkSchema =
            Schema.newBuilder()
                    .column("f_int", DataTypes.INT())
                    .column("f_string", DataTypes.STRING())
                    .primaryKey("f_int")
                    .build();

    @BeforeAll
    protected static void beforeAll() {
        FlinkValuesTieringTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
    }

    @Test
    void testTiering() throws Exception {
        // create a pk table, write some records and wait until snapshot finished
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTable");
        long t1Id = createPkTable(t1, 1, false, pkSchema);
        TableBucket t1Bucket = new TableBucket(t1Id, 0);
        // write records
        List<InternalRow> rows =
                Arrays.asList(
                        row(1, "v1"),
                        row(2, "v1"),
                        row(3, "v1"));
        List<InternalRow> expectedRows = new ArrayList<>(rows);
        writeRows(t1, rows);
        waitUntilSnapshot(t1Id, 1, 0);

        // fail the first write to the pk table
        ValuesLake.failWhen(t1.toString()).failWriteOnce();

        // then start tiering job
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            // check the status of replica after synced
            assertReplicaStatus(t1Bucket, 3);

            checkDataInValuesPrimaryKeyTable(t1, rows);
            // check snapshot property in values lake
            Map<String, String> properties =
                    new HashMap<String, String>() {
                        {
                            put(
                                    FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                    "[{\"bucket\":0,\"offset\":3}]");
                        }
                    };
            checkSnapshotPropertyInValues(t1, properties);

            // then write data to the pk tables
            // write records
            rows =
                    Arrays.asList(
                            row(1, "v111"),
                            row(2, "v222"),
                            row(3, "v333"));
            expectedRows.addAll(rows);
            // write records
            writeRows(t1, rows);

            // check the status of replica of t1 after synced
            // not check start offset since we won't
            // update start log offset for primary key table
            assertReplicaStatus(t1Bucket, expectedRows.size());

            checkDataInValuesPrimaryKeyTable(t1, expectedRows);
        } finally {
            jobClient.cancel().get();
        }
    }

    protected JobClient buildTieringJob(StreamExecutionEnvironment execEnv) throws Exception {
        Configuration flussConfig = new Configuration(clientConf);
        flussConfig.set(POLL_TIERING_TABLE_INTERVAL, Duration.ofMillis(500L));

        LakeTieringFactory lakeTieringFactory = new ValuesLakeTieringFactory();

        // build tiering source
        TieringSource.Builder<?> tieringSourceBuilder =
                new TieringSource.Builder<>(flussConfig, lakeTieringFactory);
        if (flussConfig.get(POLL_TIERING_TABLE_INTERVAL) != null) {
            tieringSourceBuilder.withPollTieringTableIntervalMs(
                    flussConfig.get(POLL_TIERING_TABLE_INTERVAL).toMillis());
        }
        TieringSource<?> tieringSource = tieringSourceBuilder.build();
        DataStreamSource<?> source =
                execEnv.fromSource(
                        tieringSource,
                        WatermarkStrategy.noWatermarks(),
                        "TieringSource",
                        TableBucketWriteResultTypeInfo.of(
                                () -> lakeTieringFactory.getWriteResultSerializer()));

        source.getTransformation().setUid(TIERING_SOURCE_TRANSFORMATION_UID);

        source.transform(
                        "TieringCommitter",
                        CommittableMessageTypeInfo.of(
                                () -> lakeTieringFactory.getCommittableSerializer()),
                        new TieringCommitOperatorFactory(flussConfig, lakeTieringFactory))
                .setParallelism(1)
                .setMaxParallelism(1)
                .sinkTo(new DiscardingSink())
                .name("end")
                .setParallelism(1);
        String jobName =
                execEnv.getConfiguration()
                        .getOptional(PipelineOptions.NAME)
                        .orElse("Fluss Lake Tiering FailOver IT Test.");

        return execEnv.executeAsync(jobName);
    }

    private void checkDataInValuesPrimaryKeyTable(
            TablePath tablePath, List<InternalRow> expectedRows) throws Exception {
        Iterator<InternalRow> actualIterator = getValuesRecords(tablePath).iterator();
        Iterator<InternalRow> iterator = expectedRows.iterator();
        while (iterator.hasNext() && actualIterator.hasNext()) {
            InternalRow row = iterator.next();
            InternalRow record = actualIterator.next();
            assertThat(record.getInt(3)).isEqualTo(row.getInt(3));
            assertThat(record.getString(7)).isEqualTo(row.getString(7));
        }
        assertThat(actualIterator.hasNext()).isFalse();
        assertThat(iterator.hasNext()).isFalse();
    }
}
