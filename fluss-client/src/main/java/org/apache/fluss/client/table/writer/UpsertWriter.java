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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The writer to write data to the primary key table.
 *
 * @since 0.2
 */
@PublicEvolving
public interface UpsertWriter extends TableWriter, AutoCloseable {

    /**
     * Inserts row into Fluss table if they do not already exist, or updates them if they do exist.
     *
     * @param row the row to upsert.
     * @return A {@link CompletableFuture} that always returns upsert result when complete normally.
     */
    CompletableFuture<UpsertResult> upsert(InternalRow row);

    /**
     * Delete certain row by the input row in Fluss table, the input row must contain the primary
     * key.
     *
     * @param row the row to delete.
     * @return A {@link CompletableFuture} that always delete result when complete normally.
     */
    CompletableFuture<DeleteResult> delete(InternalRow row);

    /**
     * Get the acknowledged status for all buckets that have been written by this writer.
     *
     * <p>This method returns a map from TableBucket to the latest acknowledged offset for each
     * bucket. The offsets represent the last successfully acknowledged write offset for each bucket
     * that this writer has written to.
     *
     * <p>The returned map only contains buckets that have been written by this writer instance. If
     * no writes have been performed, the map will be empty.
     *
     * @return a map from TableBucket to the latest acknowledged offset for each bucket
     */
    Map<TableBucket, Long> bucketsAckStatus();
}
