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

package com.alibaba.fluss.metrics.groups;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.metrics.Meter;
import com.alibaba.fluss.metrics.Metric;

import java.util.Map;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A MetricGroup is a named container for {@link Metric Metrics} and further metric subgroups.
 *
 * <p>Instances of this class can be used to register new metrics with Fluss and to create a nested
 * hierarchy based on the group names.
 *
 * <p>A MetricGroup is uniquely identified by it's place in the hierarchy and name.
 *
 * @since 0.2
 */
@PublicEvolving
public interface MetricGroup {

    /**
     * Creates and registers a new {@link Counter} with Fluss.
     *
     * @param name name of the counter
     * @return the created counter
     */
    Counter counter(String name);

    /**
     * Registers a {@link Counter} with Fluss.
     *
     * @param name name of the counter
     * @param counter counter to register
     * @param <C> counter type
     * @return the given counter
     */
    <C extends Counter> C counter(String name, C counter);

    /**
     * Registers a new {@link Gauge} with Fluss.
     *
     * @param name name of the gauge
     * @param gauge gauge to register
     * @param <T> return type of the gauge
     * @return the given gauge
     */
    <T, G extends Gauge<T>> G gauge(String name, G gauge);

    /**
     * Registers a new {@link Histogram} with Fluss.
     *
     * @param name name of the histogram
     * @param histogram histogram to register
     * @param <H> histogram type
     * @return the registered histogram
     */
    <H extends Histogram> H histogram(String name, H histogram);

    /**
     * Registers a new {@link Meter} with Fluss.
     *
     * @param name name of the meter
     * @param meter meter to register
     * @param <M> meter type
     * @return the registered meter
     */
    <M extends Meter> M meter(String name, M meter);

    /**
     * Creates a new MetricGroup and adds it to this groups sub-groups.
     *
     * @param name name of the group
     * @return the created group
     */
    MetricGroup addGroup(String name);

    /**
     * Creates a new key-value MetricGroup pair. The key group is added to this groups sub-groups,
     * while the value group is added to the key group's sub-groups. This method returns the value
     * group.
     *
     * <p>The only difference between calling this method and {@code
     * group.addGroup(key).addGroup(value)} is that {@link #getAllVariables()} of the value group
     * return an additional {@code "<key>"="value"} pair.
     *
     * @param key name of the first group
     * @param value name of the second group
     * @return the second created group
     */
    MetricGroup addGroup(String key, String value);

    // ------------------------------------------------------------------------
    // Scope
    // ------------------------------------------------------------------------

    /**
     * Returns the logical scope for the metric group, for example {@code
     * "tabletserver.table.bucket"}, with the given filter being applied to all scope components and
     * the given delimiter being used to concatenate scope components.
     *
     * @param filter filter to apply to all scope components
     * @param delimiter delimiter to use for concatenating scope components
     * @return logical scope
     */
    String getLogicalScope(CharacterFilter filter, char delimiter);

    /**
     * Gets the scope as an array of the scope components, for example {@code ["host-1", "table",
     * "table1", "bucket1"}.
     */
    String[] getScopeComponents();

    /**
     * Returns a map of all variables and their associated value, for example {@code
     * {"table"="table-1", "bucket_id"="1"}}.
     *
     * @return map of all variables and their associated value
     */
    Map<String, String> getAllVariables();

    /** Closes the metric group and removes all metrics and subgroups. */
    void close();
}
