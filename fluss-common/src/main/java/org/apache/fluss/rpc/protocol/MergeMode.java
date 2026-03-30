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
 * Merge mode for write operations on tables with merge engines.
 *
 * <p>This enum controls how the server handles data merging when writing to tables with merge
 * engines. It applies uniformly to all merge engine types (aggregation, first-row, versioned).
 *
 * @since 0.9
 */
@PublicEvolving
public enum MergeMode {

    /**
     * Default merge mode: Data is merged through the server-side merge engine.
     *
     * <p>This is the normal mode for tables with merge engines. The behavior depends on the
     * configured merge engine type:
     *
     * <ul>
     *   <li>For aggregation merge engine: applies configured aggregation functions (e.g., SUM, MAX,
     *       MIN) to merge new values with existing values.
     *   <li>For first-row merge engine: retains the first observed row for each primary key.
     *   <li>For versioned merge engine: keeps the row with the highest version for each primary
     *       key.
     * </ul>
     */
    DEFAULT(0),

    /**
     * Overwrite mode: Data directly overwrites target values, bypassing the merge engine.
     *
     * <p>This mode is used for undo/recovery operations to restore exact historical values. When in
     * overwrite mode, the server will not apply any merge logic and will directly replace the
     * existing values with the new values.
     *
     * <p>This is similar to an "OVERWRITE INTO" statement and is typically used internally by the
     * Flink connector during failover recovery to restore the state to a previous checkpoint.
     *
     * <p>This mode applies to all merge engine types.
     */
    OVERWRITE(1);

    private final int value;

    MergeMode(int value) {
        this.value = value;
    }

    /**
     * Returns the integer value for this merge mode.
     *
     * <p>This value matches the merge_mode field values in the proto definition.
     *
     * @return the integer value
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns the proto value for this merge mode.
     *
     * <p>This is an alias for {@link #getValue()} for clarity when working with proto messages.
     *
     * @return the proto value
     */
    public int getProtoValue() {
        return value;
    }

    /**
     * Converts an integer value to a MergeMode enum.
     *
     * @param value the integer value
     * @return the corresponding MergeMode, or DEFAULT if the value is invalid
     */
    public static MergeMode fromValue(int value) {
        switch (value) {
            case 0:
                return DEFAULT;
            case 1:
                return OVERWRITE;
            default:
                return DEFAULT;
        }
    }

    /**
     * Converts a proto value to a MergeMode enum.
     *
     * <p>This is an alias for {@link #fromValue(int)} for clarity when working with proto messages.
     *
     * @param protoValue the proto value
     * @return the corresponding MergeMode, or DEFAULT if the value is invalid
     */
    public static MergeMode fromProtoValue(int protoValue) {
        return fromValue(protoValue);
    }
}
