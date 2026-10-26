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

package org.apache.fluss.server.zk.data.bulkload;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Stable target access state persisted in table and partition registrations. */
@Internal
public enum BulkLoadDataState {
    ACTIVE(0),
    LOADING(1);

    private final int code;

    BulkLoadDataState(int code) {
        this.code = code;
    }

    /** Returns the stable persisted code. */
    public int getCode() {
        return code;
    }

    /** Validates the transaction ownership carried by a registration in this access state. */
    public void validateBulkLoadId(@Nullable String bulkLoadId) {
        checkArgument(
                this != LOADING || bulkLoadId != null,
                "LOADING registration must have a BulkLoad ID.");
        if (bulkLoadId == null) {
            return;
        }
        try {
            checkArgument(
                    UUID.fromString(bulkLoadId).toString().equals(bulkLoadId),
                    "BulkLoad ID must be a canonical lowercase UUID.");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "BulkLoad ID must be a canonical lowercase UUID: " + bulkLoadId, e);
        }
    }

    /** Returns the state for a stable persisted code. */
    public static BulkLoadDataState fromCode(int code) {
        for (BulkLoadDataState state : values()) {
            if (state.code == code) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown BulkLoad data state code " + code + ".");
    }
}
