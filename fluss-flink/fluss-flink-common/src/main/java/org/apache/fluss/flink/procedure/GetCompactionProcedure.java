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

import org.apache.fluss.cluster.compaction.CompactionInfo;
import org.apache.fluss.cluster.compaction.CompactionProgress;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Procedure to query manual compaction counts and current executions by operation ID. */
public class GetCompactionProcedure extends ProcedureBase {

    private static final JsonSerializer<List<CompactionProgress.Execution>> EXECUTIONS_SERIALIZER =
            (executions, generator) -> {
                generator.writeStartArray();
                for (CompactionProgress.Execution execution : executions) {
                    generator.writeStartObject();
                    generator.writeNumberField("server_id", execution.serverId());
                    generator.writeNumberField("table_id", execution.bucket().getTableId());
                    if (execution.bucket().getPartitionId() == null) {
                        generator.writeNullField("partition_id");
                    } else {
                        generator.writeNumberField(
                                "partition_id", execution.bucket().getPartitionId());
                    }
                    generator.writeNumberField("bucket_id", execution.bucket().getBucket());
                    generator.writeNumberField("leader_epoch", execution.leaderEpoch());
                    generator.writeStringField("phase", execution.phase());
                    generator.writeNumberField("phase_start_time", execution.phaseStartTime());
                    if (execution.snapshotId().isPresent()) {
                        generator.writeNumberField("snapshot_id", execution.snapshotId().get());
                    } else {
                        generator.writeNullField("snapshot_id");
                    }
                    generator.writeStringField("reason", execution.reason().orElse(null));
                    generator.writeEndObject();
                }
                generator.writeEndArray();
            };

    /** Returns the operation summary, progress counts, times, error, and current executions. */
    @ProcedureHint(
            argument = {@ArgumentHint(name = "operation_id", type = @DataTypeHint("STRING"))},
            output =
                    @DataTypeHint(
                            "ROW<operation_id STRING, compaction_type STRING, compaction_status STRING, database_name STRING, table_name STRING, partition_name STRING, create_time BIGINT, total BIGINT, pending BIGINT, running BIGINT, succeeded BIGINT, failed BIGINT, cancelled BIGINT, last_progress_time BIGINT, observed_time BIGINT, persisted_time BIGINT, first_error STRING, current_executions STRING>"))
    public Row[] call(ProcedureContext context, String operationId) throws Exception {
        CompactionProgress progress = admin.getCompaction(operationId).get();
        CompactionInfo info = progress.info();
        return new Row[] {
            Row.of(
                    info.operationId(),
                    info.type().name(),
                    info.status().name(),
                    info.databaseName().orElse(null),
                    info.tablePath().map(TablePath::getTableName).orElse(null),
                    info.partitionName().orElse(null),
                    info.createTime(),
                    progress.total(),
                    progress.pending(),
                    progress.running(),
                    progress.succeeded(),
                    progress.failed(),
                    progress.cancelled(),
                    progress.lastProgressTime(),
                    progress.observedTime(),
                    progress.persistedTime(),
                    progress.firstError().orElse(null),
                    new String(
                            JsonSerdeUtils.writeValueAsBytes(
                                    progress.currentExecutions(), EXECUTIONS_SERIALIZER),
                            StandardCharsets.UTF_8))
        };
    }
}
