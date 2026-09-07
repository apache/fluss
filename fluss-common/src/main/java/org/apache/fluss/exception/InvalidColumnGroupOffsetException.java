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

package org.apache.fluss.exception;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Thrown when an {@code appendColumns} write violates the strict-from-EWM ordering invariant — the
 * supplied {@code source_offset} does not equal the next slot after the current per-bucket
 * Enrichment Watermark (EWM). The client typically reaches this state when its EWM cache is stale
 * relative to the leader; on receiving this error, the client batching layer should refresh the EWM
 * and drop any in-flight batches whose first offset is no longer the leader's expected next slot
 * rather than blindly retrying.
 *
 * @since 0.10
 */
@PublicEvolving
public class InvalidColumnGroupOffsetException extends ApiException {
    private static final long serialVersionUID = 1L;

    /** The source offset the server expected, or -1 when unknown. */
    private final long expectedSourceOffset;

    public InvalidColumnGroupOffsetException(String message, long expectedSourceOffset) {
        super(message);
        this.expectedSourceOffset = expectedSourceOffset;
    }

    public InvalidColumnGroupOffsetException(String message, Throwable cause) {
        super(message, cause);
        this.expectedSourceOffset = -1L;
    }

    public long getExpectedSourceOffset() {
        return expectedSourceOffset;
    }

    public InvalidColumnGroupOffsetException(String message) {
        super(message);
        this.expectedSourceOffset = -1L;
    }

    public InvalidColumnGroupOffsetException(Throwable cause) {
        super(cause);
        this.expectedSourceOffset = -1L;
    }
}
