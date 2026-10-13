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

package org.apache.fluss.server.kv;

import org.apache.fluss.metadata.Schema;

import java.util.BitSet;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Helpers for interpreting KV write {@code targetColumns}. */
public final class TargetColumns {

    private TargetColumns() {}

    /**
     * Returns {@code true} when {@code targetColumns} specifies every row field index of {@code
     * schema} (each index in {@code [0, fieldCount)} appears at least once, with no index outside
     * that range).
     *
     * <p>In that case the write is equivalent to a full-row upsert for merge-engine purposes even
     * if the client passed an explicit column array instead of {@code null}.
     */
    public static boolean specifiesAllSchemaFieldIndexes(Schema schema, int[] targetColumns) {
        checkNotNull(schema, "schema");
        checkNotNull(targetColumns, "targetColumns");
        int fieldCount = schema.getRowType().getFieldCount();
        if (fieldCount == 0) {
            return targetColumns.length == 0;
        }
        if (targetColumns.length < fieldCount) {
            return false;
        }
        BitSet covered = new BitSet(fieldCount);
        for (int col : targetColumns) {
            if (col < 0 || col >= fieldCount) {
                return false;
            }
            covered.set(col);
        }
        return covered.nextClearBit(0) >= fieldCount;
    }
}
