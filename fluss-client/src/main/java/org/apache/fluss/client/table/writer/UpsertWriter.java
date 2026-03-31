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
     * Retracts a previous aggregation contribution for the given row. The row must contain the
     * primary key fields and the old aggregation values to retract.
     *
     * <p>This is used when a Flink aggregate operator emits UPDATE_BEFORE (retract old value) for a
     * key. The retract record is sent independently with {@link
     * org.apache.fluss.record.MutationType#RETRACT} mutation type.
     *
     * @param row the old aggregation value to retract (UPDATE_BEFORE).
     * @return A {@link CompletableFuture} that returns upsert result when complete normally.
     */
    default CompletableFuture<UpsertResult> retract(InternalRow row) {
        throw new UnsupportedOperationException(
                "retract() is not supported by this UpsertWriter implementation.");
    }

    /**
     * Delete a certain record from the Fluss table. The input must contain the primary key fields.
     *
     * @param record the record to delete.
     * @return A {@link CompletableFuture} that always delete result when complete normally.
     */
    CompletableFuture<DeleteResult> delete(InternalRow record);
}
