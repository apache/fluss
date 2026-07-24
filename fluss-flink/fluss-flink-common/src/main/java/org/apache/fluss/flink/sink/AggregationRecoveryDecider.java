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

package org.apache.fluss.flink.sink;

import org.apache.fluss.flink.sink.undo.RecoveryAction;
import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.Schema;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Decides the failover correction action for an aggregation sink. */
final class AggregationRecoveryDecider {

    private AggregationRecoveryDecider() {}

    static RecoveryAction decide(
            Schema tableSchema,
            @Nullable int[] targetColumnIndexes,
            DeleteBehavior deleteBehavior,
            boolean ignoreDelete,
            boolean inputKnownUpsertOnly) {
        checkNotNull(deleteBehavior, "deleteBehavior must not be null.");
        checkNotNull(tableSchema, "tableSchema must not be null.");

        List<Schema.Column> columns = tableSchema.getColumns();
        int[] effectiveTargets =
                targetColumnIndexes == null
                        ? IntStream.range(0, columns.size()).toArray()
                        : targetColumnIndexes;
        for (int targetIndex : effectiveTargets) {
            checkArgument(
                    targetIndex >= 0 && targetIndex < columns.size(),
                    "Invalid target column index %s for schema with %s columns.",
                    targetIndex,
                    columns.size());
        }

        if (!ignoreDelete && deleteBehavior == DeleteBehavior.ALLOW && !inputKnownUpsertOnly) {
            return RecoveryAction.UNDO;
        }

        Set<Integer> primaryKeyIndexes = new HashSet<>();
        for (int primaryKeyIndex : tableSchema.getPrimaryKeyIndexes()) {
            primaryKeyIndexes.add(primaryKeyIndex);
        }
        for (int targetIndex : effectiveTargets) {
            if (primaryKeyIndexes.contains(targetIndex)) {
                continue;
            }

            AggFunctionType functionType =
                    columns.get(targetIndex)
                            .getAggFunction()
                            .map(function -> function.getType())
                            .orElse(AggFunctionType.LAST_VALUE_IGNORE_NULLS);
            if (!functionType.isIdempotent()) {
                return RecoveryAction.UNDO;
            }
        }
        return RecoveryAction.NO_OP;
    }
}
