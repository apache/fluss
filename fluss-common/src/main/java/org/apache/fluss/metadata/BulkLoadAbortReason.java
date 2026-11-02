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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

/** The reason why a BulkLoad transaction was aborted. */
@PublicEvolving
public enum BulkLoadAbortReason {
    TARGET_NOT_EMPTY(0),
    BUILD_DEADLINE_EXCEEDED(1),
    COMMIT_DECISION_DEADLINE_EXCEEDED(2),
    ABORTED_BY_CALLER(3);

    private final int code;

    BulkLoadAbortReason(int code) {
        this.code = code;
    }

    /** Returns the stable protocol code for this abort reason. */
    public int getCode() {
        return code;
    }

    /**
     * Returns the abort reason represented by the stable protocol code.
     *
     * @throws IllegalArgumentException if the code is unknown
     */
    public static BulkLoadAbortReason fromCode(int code) {
        for (BulkLoadAbortReason reason : values()) {
            if (reason.code == code) {
                return reason;
            }
        }
        throw new IllegalArgumentException("Unknown BulkLoad abort reason code " + code + ".");
    }
}
