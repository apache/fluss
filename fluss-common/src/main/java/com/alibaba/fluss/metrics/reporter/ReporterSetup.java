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

package com.alibaba.fluss.metrics.reporter;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.plugin.PluginManager;
import com.alibaba.fluss.shaded.guava32.com.google.common.collect.Iterators;
import com.alibaba.fluss.utils.CollectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Encapsulates everything needed for the instantiation and configuration of a {@link
 * MetricReporter}.
 */
public class ReporterSetup {

    private static final Logger LOG = LoggerFactory.getLogger(ReporterSetup.class);

    public static List<MetricReporter> fromConfiguration(
            final Configuration configuration, @Nullable final PluginManager pluginManager) {
        List<String> reporters = configuration.get(ConfigOptions.METRICS_REPORTERS);
        if (reporters == null || reporters.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> configuredReporters = new HashSet<>(reporters);
        final Map<String, MetricReporterPlugin> discoveredReporterPlugins =
                loadAvailableReporterPlugins(pluginManager);
        return setUpReporters(configuredReporters, discoveredReporterPlugins, configuration);
    }

    private static Map<String, MetricReporterPlugin> loadAvailableReporterPlugins(
            @Nullable PluginManager pluginManager) {
        final Map<String, MetricReporterPlugin> reporterPlugins =
                CollectionUtils.newHashMapWithExpectedSize(2);
        final Iterator<MetricReporterPlugin> pluginIterator = getAllReporterPlugins(pluginManager);
        // do not use streams or for-each loops here because they do not allow catching individual
        // ServiceConfigurationErrors
        // such an error might be caused if the META-INF/services contains an entry to a
        // non-existing plugin class
        while (pluginIterator.hasNext()) {
            try {
                MetricReporterPlugin plugin = pluginIterator.next();
                String reporterName = plugin.identifier();
                MetricReporterPlugin existingPlugin = reporterPlugins.get(reporterName);
                if (existingPlugin == null) {
                    reporterPlugins.put(reporterName, plugin);
                    LOG.debug(
                            "Found {} with name {} at {}.",
                            MetricReporterPlugin.class.getSimpleName(),
                            reporterName,
                            new File(
                                            plugin.getClass()
                                                    .getProtectionDomain()
                                                    .getCodeSource()
                                                    .getLocation()
                                                    .toURI())
                                    .getCanonicalPath());
                } else {
                    LOG.warn(
                            "Multiple implementations of the {} with same reporter name {} were found in 'lib' and/or 'plugins' directories."
                                    + " It is recommended to remove redundant reporter JARs to resolve used versions' ambiguity.",
                            MetricReporter.class.getSimpleName(),
                            reporterName);
                }
            } catch (Exception | ServiceConfigurationError e) {
                LOG.warn("Error while loading {}.", MetricReporterPlugin.class.getSimpleName(), e);
            }
        }
        return Collections.unmodifiableMap(reporterPlugins);
    }

    private static Iterator<MetricReporterPlugin> getAllReporterPlugins(
            @Nullable PluginManager pluginManager) {
        final Iterator<MetricReporterPlugin> pluginIteratorSPI =
                ServiceLoader.load(MetricReporterPlugin.class).iterator();
        final Iterator<MetricReporterPlugin> iteratorPlugins =
                pluginManager != null
                        ? pluginManager.load(MetricReporterPlugin.class)
                        : Collections.emptyIterator();

        return Iterators.concat(iteratorPlugins, pluginIteratorSPI);
    }

    private static List<MetricReporter> setUpReporters(
            Set<String> configuredReporters,
            Map<String, MetricReporterPlugin> discoveredReporterPlugins,
            Configuration reporterConfig) {
        List<MetricReporter> reporters = new ArrayList<>(configuredReporters.size());
        for (String reporterName : configuredReporters) {
            if (!discoveredReporterPlugins.containsKey(reporterName)) {
                LOG.warn(
                        "No reporter plugin can be found for reporter {}. Metrics might not be exposed/reported. Available reporter plugins: {}.",
                        reporterName,
                        discoveredReporterPlugins.keySet());
                continue;
            }
            try {
                MetricReporterPlugin metricReporterPlugin =
                        discoveredReporterPlugins.get(reporterName);
                MetricReporter reporter = metricReporterPlugin.createMetricReporter(reporterConfig);
                Configuration metricConfig = new Configuration(reporterConfig);
                reporter.open(metricConfig);
                reporters.add(reporter);
            } catch (Throwable t) {
                LOG.error(
                        "Could not instantiate {} with name {}. Metrics might not be exposed/reported.",
                        MetricReporter.class.getSimpleName(),
                        reporterName,
                        t);
            }
        }
        return reporters;
    }
}
