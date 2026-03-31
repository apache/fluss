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

package org.apache.fluss.flink.sink.serializer;

import java.io.Serializable;

/**
 * Encapsulates the operation mode configuration for a Flink sink, replacing multiple boolean
 * constructor parameters with a self-documenting, validated configuration object.
 *
 * <p>Semantic precedence for UPDATE_BEFORE handling (evaluated in order):
 *
 * <ol>
 *   <li>{@code ignoreDelete} — if true, DELETE and UPDATE_BEFORE are silently dropped
 *   <li>{@code appendOnly} — if true, all operations map to APPEND
 *   <li>{@code supportsRetract} — if true, UPDATE_BEFORE maps to RETRACT
 *   <li>{@code aggregationTable} — if true and retract not supported, UPDATE_BEFORE fails fast
 * </ol>
 */
public final class SinkOperationMode implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean appendOnly;
    private final boolean ignoreDelete;
    private final boolean aggregationTable;
    private final boolean supportsRetract;

    private SinkOperationMode(
            boolean appendOnly,
            boolean ignoreDelete,
            boolean aggregationTable,
            boolean supportsRetract) {
        if (supportsRetract && !aggregationTable) {
            throw new IllegalArgumentException(
                    "supportsRetract requires aggregationTable to be true.");
        }
        this.appendOnly = appendOnly;
        this.ignoreDelete = ignoreDelete;
        this.aggregationTable = aggregationTable;
        this.supportsRetract = supportsRetract;
    }

    /** Mode for append-only (log) tables. */
    public static SinkOperationMode appendOnly(boolean ignoreDelete) {
        return new SinkOperationMode(true, ignoreDelete, false, false);
    }

    /** Mode for upsert (primary key) tables without aggregation. */
    public static SinkOperationMode upsert(boolean ignoreDelete) {
        return new SinkOperationMode(false, ignoreDelete, false, false);
    }

    /** Mode for aggregation tables. */
    public static SinkOperationMode aggregation(boolean ignoreDelete, boolean supportsRetract) {
        return new SinkOperationMode(false, ignoreDelete, true, supportsRetract);
    }

    public boolean isAppendOnly() {
        return appendOnly;
    }

    public boolean isIgnoreDelete() {
        return ignoreDelete;
    }

    public boolean isAggregationTable() {
        return aggregationTable;
    }

    public boolean supportsRetract() {
        return supportsRetract;
    }

    @Override
    public String toString() {
        return String.format(
                "SinkOperationMode{appendOnly=%s, ignoreDelete=%s, aggregationTable=%s, supportsRetract=%s}",
                appendOnly, ignoreDelete, aggregationTable, supportsRetract);
    }
}
