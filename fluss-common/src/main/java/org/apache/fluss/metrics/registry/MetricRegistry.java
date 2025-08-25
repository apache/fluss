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

package org.apache.fluss.metrics.registry;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.reporter.MetricReporter;
import org.apache.fluss.metrics.reporter.ReporterSetup;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.utils.AutoCloseableAsync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/** Interface for a metric registry. */
public interface MetricRegistry extends AutoCloseableAsync {

    Logger LOG = LoggerFactory.getLogger(MetricRegistry.class);

    /** Returns the number of registered reporters. */
    int getNumberReporters();

    /**
     * Registers a new {@link Metric} with this registry.
     *
     * @param metric the metric that was added
     * @param metricName the name of the metric
     * @param group the group that contains the metric
     */
    void register(Metric metric, String metricName, AbstractMetricGroup group);

    /**
     * Un-registers the given {@link Metric} with this registry.
     *
     * @param metric the metric that should be removed
     * @param metricName the name of the metric
     * @param group the group that contains the metric
     */
    void unregister(Metric metric, String metricName, AbstractMetricGroup group);

    /**
     * Creates a MetricRegistry from the given configuration. If no reporters are configured, a NOP
     * registry is returned.
     *
     * @param configuration the configuration
     * @param pluginManager the plugin manager to find the reporters, be null if not in a server
     *     environment.
     */
    static MetricRegistry create(
            Configuration configuration, @Nullable PluginManager pluginManager) {
        List<MetricReporter> metricReporters =
                ReporterSetup.fromConfiguration(configuration, pluginManager);
        if (metricReporters.isEmpty()) {
            LOG.info("No metrics reporter configured, no metrics will be exposed/reported.");
            return NOPMetricRegistry.INSTANCE;
        } else {
            return new MetricRegistryImpl(metricReporters);
        }
    }
}
