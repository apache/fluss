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

package org.apache.fluss.metrics.reporter;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.plugin.Plugin;

/**
 * {@link MetricReporter} Plugin.
 *
 * <p>Metric reporters that can be instantiated with a {@link Plugin} automatically qualify for
 * being loaded as a plugin, so long as the reporter jar is self-contained (excluding Fluss
 * dependencies) and contains a {@code
 * META-INF/services/org.apache.fluss.metrics.reporter.MetricReporterFactory} file containing the
 * qualified class name of the plugin.
 *
 * @since 0.2
 */
@PublicEvolving
public interface MetricReporterPlugin extends Plugin {

    /**
     * Creates a new metric reporter.
     *
     * @param configuration fluss configuration
     * @return created metric reporter
     */
    MetricReporter createMetricReporter(final Configuration configuration);

    /**
     * Returns a unique identifier for the metric reporter plugin.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * prometheus}). If multiple factories exist for different versions, a version should be
     * appended using "-" (e.g. {@code jmx-1}).
     */
    String identifier();
}
