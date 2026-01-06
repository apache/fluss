/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.record.BinaryValue;

/** A updater to auto increment column . */
public interface AutoIncrementUpdater {

    /**
     * Updates the auto-increment column in the given row by replacing its value with a new sequence
     * number.
     *
     * <p>This method may return a new {@link BinaryValue} instance or the same instance if no
     * update is needed (e.g., in a no-op implementation).
     *
     * @param rowValue the input row in binary form, must not be {@code null}
     * @return a {@link BinaryValue} representing the updated row; never {@code null}
     */
    BinaryValue updateAutoIncrementColumns(BinaryValue rowValue);

    /**
     * Returns whether this updater actually performs auto-increment logic.
     *
     * @return {@code true} if auto-increment is active; {@code false} otherwise.
     */
    default boolean hasAutoIncrement() {
        return false;
    }
}
