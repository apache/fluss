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

package com.alibaba.fluss.metrics.groups;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.metrics.Meter;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.SimpleCounter;
import com.alibaba.fluss.metrics.registry.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Abstract {@link MetricGroup} that contains key functionality for adding metrics and groups.
 *
 * <p><b>IMPORTANT IMPLEMENTATION NOTE</b>
 *
 * <p>This class uses locks for adding and removing metrics objects. This is done to prevent
 * resource leaks in the presence of concurrently closing a group and adding metrics and subgroups.
 * Since closing groups recursively closes the subgroups, the lock acquisition order must be
 * strictly from parent group to subgroup. If at any point, a subgroup holds its group lock and
 * calls a parent method that also acquires the lock, it will create a deadlock condition.
 *
 * <p>An AbstractMetricGroup can be {@link #close() closed}. Upon closing, the group de-register all
 * metrics from any metrics reporter and any internal maps. Note that even closed metrics groups
 * return Counters, Gauges, etc to the code, to prevent exceptions in the monitored code. These
 * metrics simply do not get reported any more, when created on a closed group.
 */
@Internal
public abstract class AbstractMetricGroup implements MetricGroup {

    protected static final Logger LOG = LoggerFactory.getLogger(MetricGroup.class);

    /** The parent group containing this group. */
    protected final AbstractMetricGroup parent;

    /** The map containing all variables and their associated values, lazily computed. */
    protected volatile Map<String, String> variables;

    /** The registry that this metrics group belongs to. */
    protected final MetricRegistry registry;

    /** All metrics that are directly contained in this group. */
    private final Map<String, Metric> metrics = new HashMap<>();

    /** All metric subgroups of this group. */
    private final Map<String, AbstractMetricGroup> groups = new HashMap<>();

    /**
     * The metrics scope represented by this group. For example ["host-7", "table-2", "bucket1"].
     */
    private final String[] scopeComponents;

    /**
     * The logical metrics scope represented by this group for each reporter, as a concatenated
     * string, lazily computed. For example: "tabletserver.table.bucket"
     */
    private final String[] logicalScopeStrings;

    /** Flag indicating whether this group has been closed. */
    private volatile boolean closed;

    // ------------------------------------------------------------------------

    public AbstractMetricGroup(
            MetricRegistry registry, String[] scope, AbstractMetricGroup parent) {
        this.registry = checkNotNull(registry);
        this.scopeComponents = checkNotNull(scope);
        this.parent = parent;
        this.logicalScopeStrings = new String[registry.getNumberReporters()];
    }

    @Override
    public Map<String, String> getAllVariables() {
        if (variables == null) {
            Map<String, String> tmpVariables = new HashMap<>();

            putVariables(tmpVariables);

            if (parent != null) { // not true for Coordinator-/TabletServerMetricGroup
                // explicitly call getAllVariables() to prevent cascading caching operations
                // upstream, to prevent caching in groups which are never directly passed to
                // reporters
                tmpVariables.putAll(parent.getAllVariables());
            }
            variables = tmpVariables;
        }
        return variables;
    }

    /**
     * Gets the scope as an array of the scope components, for example {@code ["host-7", "table-1",
     * "bucket-1"]}.
     */
    @Override
    public String[] getScopeComponents() {
        return scopeComponents;
    }

    /**
     * Returns the logical scope of this group, for example {@code "tabletserver.table.bucket"}.
     *
     * @param filter character filter which is applied to the scope components
     * @return logical scope
     */
    @Override
    public String getLogicalScope(CharacterFilter filter, char delimiter) {
        return getLogicalScope(filter, delimiter, -1);
    }

    /**
     * Returns the logical scope of this group, for example {@code "tabletserver.table.bucket"}.
     *
     * @param filter character filter which is applied to the scope components
     * @param delimiter delimiter to use for concatenating scope components
     * @param reporterIndex index of the reporter
     * @return logical scope
     */
    String getLogicalScope(CharacterFilter filter, char delimiter, int reporterIndex) {
        if (logicalScopeStrings.length == 0
                || (reporterIndex < 0 || reporterIndex >= logicalScopeStrings.length)) {
            return createLogicalScope(filter, delimiter);
        } else {
            if (logicalScopeStrings[reporterIndex] == null) {
                logicalScopeStrings[reporterIndex] = createLogicalScope(filter, delimiter);
            }
            return logicalScopeStrings[reporterIndex];
        }
    }

    protected String createLogicalScope(CharacterFilter filter, char delimiter) {
        final String groupName = getGroupName(filter);
        if (parent == null) {
            return groupName;
        }
        if (groupName == null || groupName.isEmpty()) {
            return parent.getLogicalScope(filter, delimiter);
        }
        return parent.getLogicalScope(filter, delimiter) + delimiter + groupName;
    }

    /** Return the parent of the metric group. */
    @Nullable
    public AbstractMetricGroup getParent() {
        return parent;
    }

    /**
     * Enters all variables specific to this {@link AbstractMetricGroup} and their associated values
     * into the map.
     *
     * @param variables map to enter variables and their values into
     */
    protected void putVariables(Map<String, String> variables) {}

    /**
     * Returns the name for this group, meaning what kind of entity it represents, for example
     * "tabletserver".
     *
     * @param filter character filter which is applied to the name
     * @return logical name for this group
     */
    protected abstract String getGroupName(CharacterFilter filter);

    // ------------------------------------------------------------------------
    //  Closing
    // ------------------------------------------------------------------------

    @Override
    public void close() {
        synchronized (this) {
            if (!closed) {
                closed = true;

                // close all subgroups
                for (AbstractMetricGroup group : groups.values()) {
                    group.close();
                }
                groups.clear();

                // un-register all directly contained metrics
                for (Map.Entry<String, Metric> metric : metrics.entrySet()) {
                    registry.unregister(metric.getValue(), metric.getKey(), this);
                }
                metrics.clear();
            }
        }
    }

    public final boolean isClosed() {
        return closed;
    }

    // -----------------------------------------------------------------------------------------------------------------
    //  Metrics
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public Counter counter(String name) {
        return counter(name, new SimpleCounter());
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
        addMetric(name, counter);
        return counter;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
        addMetric(name, gauge);
        return gauge;
    }

    @Override
    public <H extends Histogram> H histogram(String name, H histogram) {
        addMetric(name, histogram);
        return histogram;
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        addMetric(name, meter);
        return meter;
    }

    /**
     * Adds the given metric to the group and registers it at the registry, if the group is not yet
     * closed, and if no metric with the same name has been registered before.
     *
     * @param name the name to register the metric under
     * @param metric the metric to register
     */
    protected void addMetric(String name, Metric metric) {
        if (metric == null) {
            LOG.warn(
                    "Ignoring attempted registration of a metric due to being null for name {}.",
                    name);
            return;
        }
        // add the metric only if the group is still open
        synchronized (this) {
            if (closed) {
                return;
            }

            // immediately put without a 'contains' check to optimize the common case (no
            // collision)
            // collisions are resolved later
            Metric prior = metrics.put(name, metric);

            // check for collisions with other metric names
            if (prior == null) {
                // no other metric with this name yet

                if (groups.containsKey(name)) {
                    // we warn here, rather than failing, because metrics are tools that should
                    // not fail the program when used incorrectly
                    LOG.warn(
                            "Name collision: Adding a metric with the same name as a metric subgroup: '{}'. Metric might not get properly reported. {}",
                            name,
                            Arrays.toString(scopeComponents));
                }

                registry.register(metric, name, this);
            } else {
                // we had a collision. put back the original value
                metrics.put(name, prior);

                // we warn here, rather than failing, because metrics are tools that should not
                // fail the
                // program when used incorrectly
                LOG.warn(
                        "Name collision: Group already contains a Metric with the name '{}'. Metric will not be reported.{}",
                        name,
                        Arrays.toString(scopeComponents));
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Groups
    // ------------------------------------------------------------------------

    @Override
    public MetricGroup addGroup(String name) {
        return addGroup(name, ChildType.GENERIC);
    }

    @Override
    public MetricGroup addGroup(String key, String value) {
        return addGroup(key, ChildType.KEY).addGroup(value, ChildType.VALUE);
    }

    private AbstractMetricGroup addGroup(String name, ChildType childType) {
        synchronized (this) {
            if (!closed) {
                // adding a group with the same name as a metric creates problems in many
                // reporters/dashboards
                // we warn here, rather than failing, because metrics are tools that should not fail
                // the program when used incorrectly
                if (metrics.containsKey(name)) {
                    LOG.warn(
                            "Name collision: Adding a metric subgroup with the same name as an existing metric: '{}'. Metric might not get properly reported. {}",
                            name,
                            Arrays.toString(scopeComponents));
                }

                AbstractMetricGroup newGroup = createChildGroup(name, childType);
                AbstractMetricGroup prior = groups.put(name, newGroup);
                if (prior == null || prior.isClosed()) {
                    // no prior group or closed group with that name
                    return newGroup;
                } else {
                    // had a prior group with that name, add the prior group back
                    groups.put(name, prior);
                    return prior;
                }
            } else {
                // return a non-registered group that is immediately closed already
                GenericMetricGroup closedGroup = new GenericMetricGroup(registry, this, name);
                closedGroup.close();
                return closedGroup;
            }
        }
    }

    protected GenericMetricGroup createChildGroup(String name, ChildType childType) {
        switch (childType) {
            case KEY:
                return new GenericKeyMetricGroup(registry, this, name);
            default:
                return new GenericMetricGroup(registry, this, name);
        }
    }

    @VisibleForTesting
    public Map<String, Metric> getMetrics() {
        return metrics;
    }

    /**
     * Enum for indicating which child group should be created. `KEY` is used to create {@link
     * GenericKeyMetricGroup}. `VALUE` is used to create {@link GenericValueMetricGroup}. `GENERIC`
     * is used to create {@link GenericMetricGroup}.
     */
    protected enum ChildType {
        KEY,
        VALUE,
        GENERIC
    }
}
