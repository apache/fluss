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
import org.apache.fluss.metadata.BucketLogOffset;

import java.util.Objects;

/** The requested position is ahead of committed and applied KV progress. */
@PublicEvolving
public class MvccReadOffsetNotYetReadableException extends ApiException {
    private static final long serialVersionUID = 1L;

    private final BucketLogOffset latestPosition;

    /** Creates an error with the latest position observed at the serving bucket. */
    public MvccReadOffsetNotYetReadableException(BucketLogOffset latestPosition) {
        super(
                "The requested position is above the readable history. Latest readable position: "
                        + Objects.requireNonNull(latestPosition)
                        + ".");
        this.latestPosition = latestPosition;
    }

    /** Returns the latest readable position observed when validating the request. */
    public BucketLogOffset getLatestReadableOffset() {
        return latestPosition;
    }
}
