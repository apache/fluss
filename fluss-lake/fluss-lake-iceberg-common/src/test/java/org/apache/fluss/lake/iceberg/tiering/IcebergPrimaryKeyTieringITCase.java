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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.lake.iceberg.testutils.IcebergTestUtils;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Regression tests for primary-key updates across Iceberg tiering rounds. */
class IcebergPrimaryKeyTieringITCase extends FlinkIcebergTieringTestBase {

    @ParameterizedTest(name = "Iceberg format={0}, legacySchema={1}")
    @CsvSource({"2,false", "3,false", "2,true", "3,true"})
    void testSameKeyUpdates(int formatVersion, boolean legacySchema) throws Exception {
        TablePath tablePath =
                TablePath.of("fluss", "v3_repro_pk_v" + formatVersion + "_" + legacySchema);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        long tableId = createPkTable(tablePath, 1, false, schema);
        TableBucket bucket = new TableBucket(tableId, 0);
        if (legacySchema) {
            IcebergTestUtils.adjustToLegacyV1Table(icebergCatalog, toIceberg(tablePath));
        }
        Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        table.updateProperties()
                .set(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion))
                .commit();
        table.refresh();
        assertThat(((BaseTable) table).operations().current().formatVersion())
                .isEqualTo(formatVersion);
        execEnv.enableCheckpointing(500);
        JobClient job = null;
        try {
            writeRows(tablePath, Collections.singletonList(row(1, "initial")), false);
            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);
            job = buildTieringJob(execEnv);
            assertReplicaStatus(bucket, 1L);
            assertPayload(tablePath, "initial");
            stopJob(job);
            job = null;

            // A single update across commits creates an equality delete, without an intra-file
            // delete.
            writeRows(tablePath, Collections.singletonList(row(1, "across_commit")), false);
            job = buildTieringJob(execEnv);
            assertReplicaStatus(bucket, 3L);
            assertPayload(tablePath, "across_commit");
            stopJob(job);
            job = null;

            // Stop the tiering job while producing both updates so they are consumed in one round.
            List<InternalRow> updates = Arrays.asList(row(1, "in_batch_1"), row(1, "in_batch_2"));
            writeRows(tablePath, updates, false);
            job = buildTieringJob(execEnv);
            assertReplicaStatus(bucket, 7L);
            assertPayload(tablePath, "in_batch_2");
        } finally {
            if (job != null) {
                stopJob(job);
            }
            admin.dropTable(tablePath, true).get();
        }
    }

    private void assertPayload(TablePath tablePath, String expected) throws Exception {
        List<Record> records = getIcebergRecords(tablePath);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getField("id")).isEqualTo(1);
        assertThat(records.get(0).getField("payload")).isEqualTo(expected);
    }

    private void stopJob(JobClient job) throws Exception {
        job.cancel().get(30, TimeUnit.SECONDS);
    }
}
