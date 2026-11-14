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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.row.InternalRow;

import java.util.concurrent.CompletableFuture;

/**
 * The typed writer to write data to the log table using POJOs.
 *
 * @param <T> the type of the record
 * @since 0.6
 */
@PublicEvolving
public interface TypedAppendWriter<T> extends TableWriter {

    /**
     * Append a record into a Log Table.
     *
     * @param record the record to append.
     * @return A {@link CompletableFuture} that always returns append result when complete normally.
     */
    CompletableFuture<AppendResult> append(T record);

    /**
     * Append a record in the specified bucket of its target table or partition.
     *
     * <p>For tables with bucket keys, the bucket must match the key's normal hash routing.
     *
     * <p>This call may block while resolving the target table or partition's bucket metadata.
     *
     * @param record the record to append
     * @param bucketId the zero-based target bucket ID, less than the target bucket count
     * @return the append result
     * @throws IllegalArgumentException if the bucket ID is negative
     * @throws FlussRuntimeException if the bucket ID is out of range or does not match the bucket
     *     key
     * @see AppendWriter#append(InternalRow, int)
     */
    CompletableFuture<AppendResult> append(T record, int bucketId);
}
