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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Receives the result of an asynchronous consumer offset commit on the thread calling the scanner.
 *
 * <p>Built-in scanners invoke ready callbacks during a later poll, after any required lookup of
 * committed offsets, or before submitting a subsequent synchronous or asynchronous commit. A commit
 * call can proceed while an earlier asynchronous commit is still in flight.
 *
 * <p>Closing the scanner may also invoke callbacks while it waits for outstanding commits. The
 * configured {@code client.request-timeout} limits this wait. Running callbacks can extend close
 * beyond that timeout. A callback may remain undelivered even if its commit has already completed.
 */
@PublicEvolving
@FunctionalInterface
public interface OffsetCommitCallback {

    /**
     * Handles completion of an asynchronous offset commit.
     *
     * @param offsets the caller-supplied offset map, or a read-only snapshot of the scanner's
     *     positions if the caller omitted the map
     * @param exception the commit failure, including {@link
     *     org.apache.fluss.exception.TimeoutException} if the request timed out, or {@code null} on
     *     success. A timeout leaves the server-side outcome unknown.
     */
    void onComplete(Map<TableBucket, OffsetMetadata> offsets, @Nullable Exception exception);
}
