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

import java.util.concurrent.CompletableFuture;

/**
 * The typed writer to write data to the primary key table using POJOs.
 *
 * @param <T> the type of the record
 * @since 0.6
 */
@PublicEvolving
public interface TypedUpsertWriter<T> extends TableWriter {

    /**
     * Inserts a record into Fluss table if it does not already exist, or updates it if it does.
     *
     * @param record the record to upsert.
     * @return A {@link CompletableFuture} that always returns upsert result when complete normally.
     */
    CompletableFuture<UpsertResult> upsert(T record);

    /**
     * Delete a certain record from the Fluss table. The input must contain the primary key fields.
     *
     * @param record the record to delete.
     * @return A {@link CompletableFuture} that always delete result when complete normally.
     */
    CompletableFuture<DeleteResult> delete(T record);
}
