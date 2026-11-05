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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.InternalRow;

import java.util.concurrent.CompletableFuture;

/**
 * The writer to write data to the primary key table.
 *
 * @since 0.2
 */
@PublicEvolving
public interface UpsertWriter extends TableWriter {

    /**
     * Inserts a record into Fluss table if it does not already exist, or updates it if it does.
     *
     * @param record the record to upsert.
     * @return A {@link CompletableFuture} that always returns upsert result when complete normally.
     */
    CompletableFuture<UpsertResult> upsert(InternalRow record);

    /**
     * Applies field-level aggregation retraction to the supplied input contribution. The row must
     * contain the primary key fields and the aggregation values to retract.
     *
     * <p>Each target field applies its aggregate function's retract rule. SUM subtracts the input
     * (or negates it when the accumulator is null). PRODUCT divides using the numeric type's
     * division rules; a null accumulator remains null. COLLECT removes matching occurrences, while
     * MERGE_MAP removes entries by key without restoring overwritten values. Their best-effort
     * rules do not remember retracts received before the contribution. LAST_VALUE clears the field;
     * it does not restore earlier values. Retraction also applies to keys without a current row.
     * Zero and all-null aggregate fields do not delete the record. Primary keys and non-target
     * fields are preserved. Use {@link #delete(InternalRow)} to request deletion instead. Fields
     * configured with {@code fields.<column>.ignore-retract=true} keep their accumulators. The
     * table's changelog DELETE policy and Flink's sink opt-in do not change this explicit API.
     *
     * <p>When removing a previously added contribution, use its original primary key and input
     * values for the target fields being retracted. Upstream lookup joins and non-deterministic
     * expressions must preserve those values for retraction. A key-changing update retracts the old
     * key's contribution and upserts the new key's contribution.
     *
     * <p>Each call is processed in order without inferring an update pair from adjacency or client
     * batching. Atomicity is not provided across separate requests.
     *
     * <p>An arithmetic failure, such as integer or decimal division by zero or non-terminating
     * decimal division, rejects the write batch with {@link
     * org.apache.fluss.exception.InvalidRecordException}. The error identifies the field,
     * aggregation function, table bucket and encoded primary key.
     *
     * <p><b>Retract is NOT idempotent.</b> Applying the same retract twice subtracts the
     * contribution twice (e.g., for SUM). Correctness under retries relies on the server-side
     * writer batch deduplication, therefore retract requires {@code
     * client.writer.enable-idempotence} to be enabled (the default) and implementations must reject
     * retract when it is disabled. Note that deduplication is scoped to a single writer session: if
     * the application itself resends a retract after a restart (a new writer id), the server cannot
     * detect the duplicate. Callers that need exactly-once semantics across restarts must handle it
     * externally (e.g., the Flink connector restores checkpoint state via undo recovery before
     * replaying).
     *
     * @param row the input contribution to retract, including primary key fields
     * @return A {@link CompletableFuture} that returns retract result when complete normally.
     */
    default CompletableFuture<RetractResult> retract(InternalRow row) {
        throw new UnsupportedOperationException(
                "retract() is not supported by this UpsertWriter implementation.");
    }

    /**
     * Delete a certain record from the Fluss table. The input must contain the primary key fields.
     * Full writes request deletion of the record; partial writes clear the target value fields. The
     * configured table delete behavior determines whether deletion is allowed. Non-key values in
     * the input are not used as aggregation contributions; use {@link #retract(InternalRow)} for
     * field-level retraction.
     *
     * @param record the record to delete.
     * @return A {@link CompletableFuture} that always delete result when complete normally.
     */
    CompletableFuture<DeleteResult> delete(InternalRow record);
}
