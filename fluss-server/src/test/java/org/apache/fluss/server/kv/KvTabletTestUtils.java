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

package org.apache.fluss.server.kv;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;

/** Test utilities for {@link KvTablet}. */
public class KvTabletTestUtils {

    private KvTabletTestUtils() {}

    /**
     * Requests an asynchronous flush of the pre-write buffer up to the given offset and blocks
     * until {@link KvTablet#getFlushedLogOffset()} has reached it or the flush failed.
     *
     * <p>{@code Long.MAX_VALUE} means "flush everything" and is resolved to the current local log
     * end offset so that {@code flushedLogOffset} is never poisoned with an unreachable target.
     */
    public static void flushAndWait(KvTablet tablet, long exclusiveUpToLogOffset) {
        long targetOffset =
                exclusiveUpToLogOffset == Long.MAX_VALUE
                        ? tablet.localLogEndOffset()
                        : exclusiveUpToLogOffset;

        AtomicReference<Throwable> failure = new AtomicReference<>();
        tablet.requestFlush(targetOffset, failure::set);

        waitUntil(
                () -> failure.get() != null || tablet.getFlushedLogOffset() >= targetOffset,
                Duration.ofSeconds(30),
                String.format(
                        "KV flush did not reach offset %s for %s",
                        targetOffset, tablet.getTableBucket()));

        Throwable cause = failure.get();
        if (cause != null) {
            throw new AssertionError("KV flush failed for " + tablet.getTableBucket(), cause);
        }
    }
}
