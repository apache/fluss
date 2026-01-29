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

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Aggregation mode for write operations.
 *
 * <p>This enum controls how the server handles data aggregation when writing to tables with
 * aggregation merge engine.
 *
 * @since 0.9
 */
@PublicEvolving
public enum AggMode {

    /**
     * Aggregate mode (default): Data is aggregated through server-side merge engine.
     *
     * <p>This is the normal mode for aggregation tables. When writing to a table with aggregation
     * merge engine, the server will apply the configured aggregation functions (e.g., SUM, MAX,
     * MIN) to merge the new values with existing values.
     */
    AGGREGATE(0),

    /**
     * Overwrite mode: Data directly overwrites target values, bypassing merge engine.
     *
     * <p>This mode is used for undo recovery operations to restore exact historical values. When in
     * overwrite mode, the server will not apply any aggregation functions and will directly replace
     * the existing values with the new values.
     *
     * <p>This is typically used internally by the Flink connector during failover recovery to
     * restore the state to a previous checkpoint.
     */
    OVERWRITE(1),

    /**
     * Client-side local aggregation mode (future): Data is pre-aggregated on client side before
     * being sent to server for final aggregation.
     *
     * <p>This mode reduces write amplification and network traffic by aggregating multiple updates
     * to the same key on the client side before sending to the server.
     *
     * <p>Note: This mode is reserved for future implementation.
     */
    LOCAL_AGGREGATE(2);

    private final int value;

    AggMode(int value) {
        this.value = value;
    }

    /**
     * Returns the integer value for this aggregation mode.
     *
     * <p>This value matches the agg_mode field values in the proto definition.
     *
     * @return the integer value
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns the proto value for this aggregation mode.
     *
     * <p>This is an alias for {@link #getValue()} for clarity when working with proto messages.
     *
     * @return the proto value
     */
    public int getProtoValue() {
        return value;
    }

    /**
     * Converts an integer value to an AggMode enum.
     *
     * @param value the integer value
     * @return the corresponding AggMode, or AGGREGATE if the value is invalid
     */
    public static AggMode fromValue(int value) {
        switch (value) {
            case 0:
                return AGGREGATE;
            case 1:
                return OVERWRITE;
            case 2:
                return LOCAL_AGGREGATE;
            default:
                return AGGREGATE;
        }
    }

    /**
     * Converts a proto value to an AggMode enum.
     *
     * <p>This is an alias for {@link #fromValue(int)} for clarity when working with proto messages.
     *
     * @param protoValue the proto value
     * @return the corresponding AggMode, or AGGREGATE if the value is invalid
     */
    public static AggMode fromProtoValue(int protoValue) {
        return fromValue(protoValue);
    }
}
