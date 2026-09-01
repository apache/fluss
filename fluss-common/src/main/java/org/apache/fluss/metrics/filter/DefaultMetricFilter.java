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

package org.apache.fluss.metrics.filter;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.ConfigurationUtils;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricType;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 * Adapted from Flink 1.20.4 for Fluss package names, metric types and configuration access. */

/** Default metric filter based on reporter-specific include and exclude rules. */
@Internal
public class DefaultMetricFilter implements MetricFilter {

    private static final EnumSet<MetricType> ALL_METRIC_TYPES = EnumSet.allOf(MetricType.class);
    @VisibleForTesting static final String LIST_DELIMITER = ",";

    private final List<FilterSpec> includes;
    private final List<FilterSpec> excludes;

    private DefaultMetricFilter(List<FilterSpec> includes, List<FilterSpec> excludes) {
        this.includes = includes;
        this.excludes = excludes;
    }

    @Override
    public boolean filter(Metric metric, String name, String logicalScope) {
        for (FilterSpec exclude : excludes) {
            if (exclude.namePattern.matcher(name).matches()
                    && exclude.scopePattern.matcher(logicalScope).matches()
                    && exclude.types.contains(metric.getMetricType())) {
                return false;
            }
        }
        for (FilterSpec include : includes) {
            if (include.namePattern.matcher(name).matches()
                    && include.scopePattern.matcher(logicalScope).matches()
                    && include.types.contains(metric.getMetricType())) {
                return true;
            }
        }
        return false;
    }

    /** Creates a filter from the named reporter's Fluss configuration. */
    public static MetricFilter fromConfiguration(Configuration configuration, String reporterName) {
        final String prefix = "metrics.reporter." + reporterName + ".filter.";
        final List<String> includes = readFilters(configuration, prefix + "includes", "*:*:*");
        final List<String> excludes = readFilters(configuration, prefix + "excludes", "");

        final List<FilterSpec> includeFilters =
                includes.stream().map(i -> parse(i)).collect(Collectors.toList());
        final List<FilterSpec> excludeFilters =
                excludes.stream().map(e -> parse(e)).collect(Collectors.toList());

        return new DefaultMetricFilter(includeFilters, excludeFilters);
    }

    private static List<String> readFilters(
            Configuration configuration, String key, String defaultValue) {
        // Fluss list options use commas, which also separate names and types within a rule.
        String value =
                configuration
                        .get(ConfigBuilder.key(key).stringType().defaultValue(defaultValue))
                        .trim();
        if (value.length() >= 2
                && ((value.startsWith("\"") && value.endsWith("\""))
                        || (value.startsWith("'") && value.endsWith("'")))) {
            value = value.substring(1, value.length() - 1).trim();
        }
        return value.isEmpty()
                ? Collections.emptyList()
                : Arrays.stream(value.split(";")).map(String::trim).collect(Collectors.toList());
    }

    private static FilterSpec parse(String filter) {
        final String[] split = filter.split(":");
        final Pattern scope = convertToPattern(split[0]);
        final Pattern name = split.length > 1 ? convertToPattern(split[1]) : convertToPattern("*");
        final EnumSet<MetricType> type =
                split.length > 2 ? parseMetricTypes(split[2]) : ALL_METRIC_TYPES;

        return new FilterSpec(scope, name, type);
    }

    @VisibleForTesting
    static Pattern convertToPattern(String scopeOrNameComponent) {
        final String[] split = scopeOrNameComponent.split(LIST_DELIMITER);

        final String rawPattern =
                Arrays.stream(split)
                        .map(String::trim)
                        .map(s -> s.replaceAll("\\*", ".*"))
                        .collect(Collectors.joining("|", "(", ")"));

        return Pattern.compile(rawPattern);
    }

    @VisibleForTesting
    static EnumSet<MetricType> parseMetricTypes(String typeComponent) {
        final List<String> split =
                Arrays.stream(typeComponent.split(LIST_DELIMITER))
                        .map(String::trim)
                        .collect(Collectors.toList());

        if (split.size() == 1 && split.get(0).equals("*")) {
            return ALL_METRIC_TYPES;
        }

        return EnumSet.copyOf(
                split.stream()
                        .map(s -> ConfigurationUtils.<MetricType>convertValue(s, MetricType.class))
                        .collect(Collectors.toSet()));
    }

    private static class FilterSpec {
        private final Pattern scopePattern;
        private final Pattern namePattern;
        private final EnumSet<MetricType> types;

        private FilterSpec(Pattern scopePattern, Pattern namePattern, EnumSet<MetricType> types) {
            this.scopePattern = scopePattern;
            this.namePattern = namePattern;
            this.types = types;
        }
    }
}
