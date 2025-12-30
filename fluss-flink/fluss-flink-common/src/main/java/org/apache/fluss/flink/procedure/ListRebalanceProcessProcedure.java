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

import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.text.NumberFormat;
import java.util.Map;

/** Procedure to list rebalance process. */
public class ListRebalanceProcessProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {
                @ArgumentHint(
                        name = "rebalanceId",
                        type = @DataTypeHint("STRING"),
                        isOptional = true)
            })
    public String[] call(ProcedureContext context, @Nullable String rebalanceId) throws Exception {
        RebalanceProgress progress = admin.listRebalanceProgress(rebalanceId).get();
        return progressToString(progress);
    }

    private static String[] progressToString(RebalanceProgress progress) {
        RebalanceStatus status = progress.status();
        double rebalanceProgress = progress.progress();
        Map<TableBucket, RebalanceResultForBucket> bucketMap = progress.progressForBucketMap();

        String[] result = new String[bucketMap.size() + 3];
        result[0] = "Reblance total status: " + status;
        result[1] = "Rebalance progress: " + formatAsPercentage(rebalanceProgress);
        result[2] = "Rebalance detail progress for bucket:";
        int i = 3;
        for (RebalanceResultForBucket resultForBucket : bucketMap.values()) {
            result[i++] = resultForBucket.toString();
        }
        return result;
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
