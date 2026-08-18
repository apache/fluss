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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.cluster.rebalance.RebalanceInfo;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceProgressJsonSerializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Procedure to list rebalance progress.
 *
 * <p>This procedure allows querying rebalance progress. See {@link
 * Admin#listRebalanceProgress(String)} and {@link Admin#listRebalances()} for more details.
 *
 * <p>Usage examples:
 *
 * <pre>
 * -- List the current rebalance and the retained history of finished rebalances
 * CALL sys.list_rebalance();
 *
 * -- List the rebalance progress with rebalance id
 * CALL sys.list_rebalance('xxx_xxx_xxx');
 * </pre>
 */
public class ListRebalanceProcessProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {
                @ArgumentHint(
                        name = "rebalanceId",
                        type = @DataTypeHint("STRING"),
                        isOptional = true)
            },
            output =
                    @DataTypeHint(
                            "ROW<rebalance_id STRING, rebalance_status STRING, rebalance_progress STRING, rebalance_plan STRING, started_at TIMESTAMP_LTZ(3), completed_at TIMESTAMP_LTZ(3)>"))
    public Row[] call(ProcedureContext context, @Nullable String rebalanceId) throws Exception {
        if (rebalanceId != null) {
            Optional<RebalanceProgress> progressOpt =
                    admin.listRebalanceProgress(rebalanceId).get();
            return progressOpt.map(progress -> new Row[] {toRow(progress)}).orElse(new Row[0]);
        }

        // Without an id, list one row per known rebalance (the current one plus the retained
        // history), ordered from most recent to oldest. Only the current rebalance carries
        // per-bucket progress and plan detail. The two RPCs are separate round-trips, so a
        // rebalance that starts or finishes between them can leave one row briefly out of
        // date until the next call.
        Optional<RebalanceProgress> currentOpt = admin.listRebalanceProgress(null).get();
        List<RebalanceInfo> rebalanceInfos = admin.listRebalances().get();
        List<Row> rows = new ArrayList<>(rebalanceInfos.size());
        for (RebalanceInfo info : rebalanceInfos) {
            if (currentOpt.isPresent()
                    && currentOpt.get().rebalanceId().equals(info.rebalanceId())) {
                rows.add(toRow(currentOpt.get()));
            } else {
                rows.add(
                        Row.of(
                                info.rebalanceId(),
                                info.status().toString(),
                                null,
                                null,
                                toInstant(info.startedAtMs()),
                                toInstant(info.completedAtMs())));
            }
        }
        return rows.toArray(new Row[0]);
    }

    private static Row toRow(RebalanceProgress progress) {
        return Row.of(
                progress.rebalanceId(),
                progress.status().toString(),
                progress.formatAsPercentage(),
                new String(
                        JsonSerdeUtils.writeValueAsBytes(
                                progress, RebalanceProgressJsonSerializer.INSTANCE),
                        StandardCharsets.UTF_8),
                toInstant(progress.startedAtMs()),
                toInstant(progress.completedAtMs()));
    }

    private static @Nullable Instant toInstant(long epochMs) {
        return epochMs < 0 ? null : Instant.ofEpochMilli(epochMs);
    }
}
