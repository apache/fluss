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

package com.alibaba.fluss.metrics.util;

import com.alibaba.fluss.metrics.reporter.MetricReporterPlugin;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/** Test utils for metric reporters. */
public class MetricReporterTestUtils {

    /**
     * Verifies that the given {@link MetricReporterPlugin} class can be loaded by the {@link
     * ServiceLoader}.
     *
     * <p>Essentially, this verifies that the {@code
     * META-INF/services/org.apache.flink.metrics.reporter.MetricReporterFactory} file exists and
     * contains the expected factory class references.
     *
     * @param clazz class to load
     */
    public static void testMetricReporterSetupViaSPI(
            final Class<? extends MetricReporterPlugin> clazz) {
        final Set<Class<? extends MetricReporterPlugin>> loadedFactories =
                StreamSupport.stream(
                                ServiceLoader.load(MetricReporterPlugin.class).spliterator(), false)
                        .map(MetricReporterPlugin::getClass)
                        .collect(Collectors.toSet());
        assertThat(loadedFactories).contains(clazz);
    }
}
