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
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.RebalancePlanForBucketJsonSerde;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * Procedure to list rebalance progress.
 *
 * <p>This procedure allows querying rebalance progress. See {@link
 * Admin#listRebalanceProgress(String)} for more details.
 *
 * <p>Usage examples:
 *
 * <pre>
 * -- List the rebalance progress without rebalance id
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
                            "ROW<rebalance_id STRING, rebalance_status STRING, rebalance_progress STRING, rebalance_plan STRING>"))
    public Row[] call(ProcedureContext context, @Nullable String rebalanceId) throws Exception {
        Optional<RebalanceProgress> progressOpt = admin.listRebalanceProgress(rebalanceId).get();

        if (!progressOpt.isPresent()) {
            return new Row[] {Row.of("No rebalance progress found.")};
        }

        return progressToString(progressOpt.get());
    }

    private static Row[] progressToString(RebalanceProgress progress) {
        double rebalanceProgress = progress.progress();
        Map<TableBucket, RebalanceResultForBucket> bucketMap = progress.progressForBucketMap();

        StringJoiner planResult = new StringJoiner(", ");

        for (RebalanceResultForBucket resultForBucket : bucketMap.values()) {
            RebalancePlanForBucket plan = resultForBucket.plan();
            planResult.add(
                    new String(
                            JsonSerdeUtils.writeValueAsBytes(
                                    plan, RebalancePlanForBucketJsonSerde.INSTANCE),
                            StandardCharsets.UTF_8));
        }
        return new Row[] {
            Row.of(
                    progress.rebalanceId(),
                    progress.status(),
                    formatAsPercentage(rebalanceProgress),
                    planResult.toString())
        };
    }

    public static String formatAsPercentage(double value) {
        if (value < 0) {
            return "NONE";
        }
        NumberFormat pctFormat = NumberFormat.getPercentInstance();
        pctFormat.setMaximumFractionDigits(2);
        return pctFormat.format(value);
    }
}
