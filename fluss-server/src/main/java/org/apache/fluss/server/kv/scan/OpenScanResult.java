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

package org.apache.fluss.server.kv.scan;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

/**
 * Result of opening a KV full-scan session: an optional {@link ScannerContext} plus the log offset
 * captured at snapshot time. The offset must reach the client even on the empty-bucket fast path so
 * a snapshot-to-log handoff cannot miss or duplicate records.
 */
@Internal
public final class OpenScanResult {

    @Nullable private final ScannerContext context;
    private final long logOffset;

    public OpenScanResult(@Nullable ScannerContext context, long logOffset) {
        this.context = context;
        this.logOffset = logOffset;
    }

    /** {@code null} if the bucket was empty at snapshot time. */
    @Nullable
    public ScannerContext getContext() {
        return context;
    }

    public long getLogOffset() {
        return logOffset;
    }
}
