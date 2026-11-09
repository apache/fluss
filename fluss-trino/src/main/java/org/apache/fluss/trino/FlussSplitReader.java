/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.row.InternalRow;

import java.time.Duration;

/**
 * Reads rows for one physical Fluss split.
 *
 * <p>A reader owns the Fluss scanner and any buffered input belonging to the current scanner batch.
 * Polling has three outcomes:
 *
 * <ul>
 *   <li>{@link PollResult#AVAILABLE}: at least one row can be consumed;
 *   <li>{@link PollResult#YIELD}: no row is currently available, but the split is not finished;
 *   <li>{@link PollResult#FINISHED}: the bounded split has been fully consumed.
 * </ul>
 *
 * <p>The caller consumes the complete buffered batch before polling again.
 */
interface FlussSplitReader extends AutoCloseable {

    enum PollResult {
        AVAILABLE,
        YIELD,
        FINISHED
    }

    /**
     * Polls the underlying Fluss scanner at most once.
     *
     * <p>This method must only be called when {@link #hasNext()} is false.
     */
    PollResult poll(Duration timeout);

    /** Returns whether a buffered row can be consumed without another scanner poll. */
    boolean hasNext();

    /** Returns the next buffered row. */
    InternalRow next();

    /**
     * Returns whether the bounded split is fully consumed.
     *
     * <p>A finished reader must not have buffered rows.
     */
    boolean isFinished();

    /**
     * Returns the currently retained input memory known to the reader.
     *
     * <p>This excludes memory whose retained size is not exposed by the Fluss client.
     */
    long getRetainedSizeInBytes();

    /**
     * Returns encoded input bytes consumed so far, or zero when the underlying Fluss API does not
     * expose a useful byte count.
     */
    long getCompletedBytes();

    @Override
    void close() throws Exception;
}
