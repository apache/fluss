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

package org.apache.fluss.record;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * The mutation type of a {@link KvRecord} in V2 format.
 *
 * <p>This is a per-record field inside the KvRecord body, independent of the batch/request-level
 * {@link org.apache.fluss.rpc.protocol.MergeMode}.
 *
 * @since 0.10
 */
@PublicEvolving
public enum MutationType {
    /** Normal upsert record. Row must not be null. */
    UPSERT((byte) 0),

    /** Explicit delete record. Row must be null. */
    DELETE((byte) 1),

    /** Retract record carrying the old value to be retracted. Row must not be null. */
    RETRACT((byte) 2);

    private final byte value;

    MutationType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static MutationType fromValue(byte value) {
        switch (value) {
            case 0:
                return UPSERT;
            case 1:
                return DELETE;
            case 2:
                return RETRACT;
            default:
                throw new IllegalArgumentException("Unknown MutationType value: " + value);
        }
    }
}
