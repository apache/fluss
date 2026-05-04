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

package org.apache.fluss.config;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Configuration for statistics columns collection with three states:
 *
 * <ul>
 *   <li>{@link Mode#DISABLED}: Statistics collection is disabled (config not set).
 *   <li>{@link Mode#ALL}: Collect statistics for all supported columns ("*" configuration).
 *   <li>{@link Mode#SPECIFIED}: Collect statistics for specific columns only.
 * </ul>
 *
 * @since 1.0
 */
@PublicEvolving
public class StatisticsColumnsConfig {

    /** The mode of statistics columns collection. */
    public enum Mode {
        /** Statistics collection is disabled. */
        DISABLED,
        /** Collect statistics for all supported columns. */
        ALL,
        /** Collect statistics for specified columns only. */
        SPECIFIED
    }

    private static final StatisticsColumnsConfig DISABLED_INSTANCE =
            new StatisticsColumnsConfig(Mode.DISABLED, Collections.emptyList());

    private static final StatisticsColumnsConfig ALL_INSTANCE =
            new StatisticsColumnsConfig(Mode.ALL, Collections.emptyList());

    private final Mode mode;
    private final List<String> columns;

    private StatisticsColumnsConfig(Mode mode, List<String> columns) {
        this.mode = mode;
        this.columns = columns;
    }

    /** Creates a disabled statistics columns configuration. */
    public static StatisticsColumnsConfig disabled() {
        return DISABLED_INSTANCE;
    }

    /** Creates a configuration that collects statistics for all supported columns. */
    public static StatisticsColumnsConfig all() {
        return ALL_INSTANCE;
    }

    /** Creates a configuration that collects statistics for the specified columns. */
    public static StatisticsColumnsConfig of(List<String> columns) {
        checkNotNull(columns, "columns must not be null");
        return new StatisticsColumnsConfig(Mode.SPECIFIED, Collections.unmodifiableList(columns));
    }

    /** Returns the mode of statistics columns collection. */
    public Mode getMode() {
        return mode;
    }

    /** Returns the specified columns. Only meaningful when mode is {@link Mode#SPECIFIED}. */
    public List<String> getColumns() {
        return columns;
    }

    /** Returns whether statistics collection is enabled (mode is not DISABLED). */
    public boolean isEnabled() {
        return mode != Mode.DISABLED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StatisticsColumnsConfig that = (StatisticsColumnsConfig) o;
        return mode == that.mode && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, columns);
    }

    @Override
    public String toString() {
        switch (mode) {
            case DISABLED:
                return "StatisticsColumnsConfig{DISABLED}";
            case ALL:
                return "StatisticsColumnsConfig{ALL}";
            case SPECIFIED:
                return "StatisticsColumnsConfig{SPECIFIED: " + columns + "}";
            default:
                return "StatisticsColumnsConfig{UNKNOWN}";
        }
    }
}
