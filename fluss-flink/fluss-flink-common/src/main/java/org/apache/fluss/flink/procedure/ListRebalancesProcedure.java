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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.List;

/**
 * Procedure to list all known rebalances as summaries.
 *
 * <p>This procedure lists the current rebalance (if any) followed by the retained history of
 * finished rebalances, newest first. See {@link Admin#listRebalances()} for more details. To query
 * the detailed progress of one rebalance, see {@link ListRebalanceProcessProcedure}.
 *
 * <p>Usage examples:
 *
 * <pre>
 * -- List all known rebalances
 * CALL sys.list_rebalances();
 * </pre>
 */
public class ListRebalancesProcedure extends ProcedureBase {

    @ProcedureHint(
            output =
                    @DataTypeHint(
                            "ROW<rebalance_id STRING, rebalance_status STRING, started_at TIMESTAMP_LTZ(3), completed_at TIMESTAMP_LTZ(3)>"))
    public Row[] call(ProcedureContext context) throws Exception {
        List<RebalanceInfo> rebalanceInfos = admin.listRebalances().get();
        Row[] rows = new Row[rebalanceInfos.size()];
        for (int i = 0; i < rows.length; i++) {
            RebalanceInfo info = rebalanceInfos.get(i);
            rows[i] =
                    Row.of(
                            info.rebalanceId(),
                            info.status().toString(),
                            toInstant(info.startedAtMs()),
                            toInstant(info.completedAtMs()));
        }
        return rows;
    }

    private static @Nullable Instant toInstant(long epochMs) {
        return epochMs < 0 ? null : Instant.ofEpochMilli(epochMs);
    }
}
