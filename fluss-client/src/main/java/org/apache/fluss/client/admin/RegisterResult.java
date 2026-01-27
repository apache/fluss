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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Result of producer offset registration.
 *
 * <p>This enum indicates whether a producer offset snapshot was newly created or already existed
 * when calling registerProducerOffsets API.
 *
 * @since 0.9
 */
@PublicEvolving
public enum RegisterResult {
    /**
     * Snapshot was newly created.
     *
     * <p>This indicates a first startup scenario where no previous snapshot existed. The caller
     * does not need to perform undo recovery.
     */
    CREATED(0),

    /**
     * Snapshot already existed and was not overwritten.
     *
     * <p>This indicates a failover scenario where a previous snapshot exists. The caller should
     * perform undo recovery using the existing snapshot offsets.
     */
    ALREADY_EXISTS(1);

    /** The code used in RPC messages. */
    private final int code;

    RegisterResult(int code) {
        this.code = code;
    }

    /** Returns the code used in RPC messages. */
    public int getCode() {
        return code;
    }

    /** Returns the RegisterResult for the given code. */
    public static RegisterResult fromCode(int code) {
        for (RegisterResult result : values()) {
            if (result.code == code) {
                return result;
            }
        }
        throw new IllegalArgumentException("Unknown RegisterResult code: " + code);
    }
}
