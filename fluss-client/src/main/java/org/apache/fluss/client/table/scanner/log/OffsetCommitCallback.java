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
 * Callback invoked asynchronously after a consumer-offset commit completes, times out, or is
 * cancelled on scanner close. It does not require another scanner operation to run, and may run on
 * a different thread from the scanner operation that initiated the commit. A timeout or
 * cancellation does not guarantee the server has not applied the commit.
 */
@PublicEvolving
@FunctionalInterface
public interface OffsetCommitCallback {

    /**
     * Handles completion of an asynchronous offset commit.
     *
     * @param offsets immutable snapshot of the offsets submitted for commit
     * @param exception commit failure (including {@link
     *     org.apache.fluss.exception.TimeoutException} if the request times out), {@link
     *     java.util.concurrent.CancellationException} if the scanner closed before the commit
     *     completed, or {@code null} on success
     */
    void onComplete(Map<TableBucket, OffsetMetadata> offsets, @Nullable Exception exception);
}
