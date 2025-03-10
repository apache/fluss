/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import com.alibaba.fluss.annotation.PublicStable;

/**
 * Lists all kinds of changes that a row can describe in an append-only log or changelog.
 *
 * @since 0.1
 */
@PublicStable
public enum ChangeType {

    // Note: Enums have no stable hash code across different JVMs, use toByteValue() for
    // this purpose.

    /** Append only operation. */
    APPEND_ONLY("+A", (byte) 0),

    /** Insertion operation. */
    INSERT("+I", (byte) 1),

    /**
     * Update operation with the previous content of the updated row.
     *
     * <p>This kind SHOULD occur together with {@link #UPDATE_AFTER} for modelling an update that
     * needs to retract the previous row first. It is useful in cases of a non-idempotent update,
     * i.e., an update of a row that is not uniquely identifiable by a key.
     */
    UPDATE_BEFORE("-U", (byte) 2),

    /**
     * Update operation with new content of the updated row.
     *
     * <p>This kind CAN occur together with {@link #UPDATE_BEFORE} for modelling an update that
     * needs to retract the previous row first. OR it describes an idempotent update, i.e., an
     * update of a row that is uniquely identifiable by a key.
     */
    UPDATE_AFTER("+U", (byte) 3),

    /** Deletion operation. */
    DELETE("-D", (byte) 4);

    private final String shortString;

    private final byte value;

    /**
     * Creates a {@link ChangeType} enum with the given short string and byte value representation
     * of the {@link ChangeType}.
     */
    ChangeType(String shortString, byte value) {
        this.shortString = shortString;
        this.value = value;
    }

    /**
     * Returns a short string representation of this {@link ChangeType}.
     *
     * <ul>
     *   <li>"+A" represents {@link #APPEND_ONLY}.
     *   <li>"+I" represents {@link #INSERT}.
     *   <li>"-U" represents {@link #UPDATE_BEFORE}.
     *   <li>"+U" represents {@link #UPDATE_AFTER}.
     *   <li>"-D" represents {@link #DELETE}.
     * </ul>
     */
    public String shortString() {
        return shortString;
    }

    /**
     * Returns the byte value representation of this {@link ChangeType}. The byte value is used for
     * serialization and deserialization.
     *
     * <ul>
     *   <li>"0" represents {@link #APPEND_ONLY}.
     *   <li>"1" represents {@link #INSERT}.
     *   <li>"2" represents {@link #UPDATE_BEFORE}.
     *   <li>"3" represents {@link #UPDATE_AFTER}.
     *   <li>"4" represents {@link #DELETE}.
     * </ul>
     */
    public byte toByteValue() {
        return value;
    }

    /**
     * Creates a {@link ChangeType} from the given byte value. Each {@link ChangeType} has a byte
     * value representation.
     *
     * @see #toByteValue() for mapping of byte value and {@link ChangeType}.
     */
    public static ChangeType fromByteValue(byte value) {
        switch (value) {
            case 0:
                return APPEND_ONLY;
            case 1:
                return INSERT;
            case 2:
                return UPDATE_BEFORE;
            case 3:
                return UPDATE_AFTER;
            case 4:
                return DELETE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for change type.");
        }
    }
}
