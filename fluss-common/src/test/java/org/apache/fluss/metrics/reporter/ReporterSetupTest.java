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

import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.util.TestReporter;
import org.apache.fluss.testutils.common.ContextClassLoaderExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ReporterSetup}. */
class ReporterSetupTest {

    @RegisterExtension
    static final ContextClassLoaderExtension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            MetricReporterPlugin.class,
                            TestReporter1.class.getName(),
                            TestReporter2.class.getName(),
                            TestReporter11.class.getName(),
                            TestReporter12.class.getName(),
                            TestReporter13.class.getName(),
                            TestReporterPlugin.class.getName(),
                            FailingPlugin.class.getName(),
                            ConfigExposingReporterPlugin.class.getName())
                    .build();

    /** TestReporter1 class only for type differentiation. */
    public static class TestReporter1 extends TestReporter {

        public TestReporter1() {
            super("reporter1");
        }
    }

    /** TestReporter2 class only for type differentiation. */
    public static class TestReporter2 extends TestReporter {
        public TestReporter2() {
            super("reporter2");
        }
    }

    /** Verifies that a reporter can be configured with all it's arguments being forwarded. */
    @Test
    void testReporterArgumentForwarding() {
        final Configuration config = new Configuration();
        config.set(ConfigOptions.METRICS_REPORTERS, Collections.singletonList("reporter1"));

        configureReporter1(config);

        final List<MetricReporter> metricReporters = ReporterSetup.fromConfiguration(config, null);

        assertThat(metricReporters).hasSize(1);

        final MetricReporter metricReporter = metricReporters.get(0);
        assertReporter1Configured(metricReporter);
    }

    /**
     * Verifies that {@link ConfigOptions#METRICS_REPORTERS} is correctly used to filter configured
     * reporters.
     */
    @Test
    void testActivateOneReporterAmongTwoDeclared() {
        final Configuration config = new Configuration();

        configureReporter1(config);
        configureReporter2(config);

        config.set(ConfigOptions.METRICS_REPORTERS, Collections.singletonList("reporter2"));

        final List<MetricReporter> metricReporters = ReporterSetup.fromConfiguration(config, null);

        assertThat(metricReporters).hasSize(1);

        final MetricReporter reporter = metricReporters.get(0);
        assertReporter2Configured(reporter);
    }

    @Test
    void testReporterSetupSupplier() {
        final Configuration config = new Configuration();

        config.set(ConfigOptions.METRICS_REPORTERS, Collections.singletonList("reporter1"));

        final List<MetricReporter> metricReporters = ReporterSetup.fromConfiguration(config, null);

        assertThat(metricReporters).hasSize(1);

        final MetricReporter reporter = metricReporters.get(0);
        assertThat(reporter).isInstanceOf(TestReporter1.class);
    }

    /** Verifies that multiple reporters are instantiated correctly. */
    @Test
    void testMultipleReporterInstantiation() {
        Configuration config = new Configuration();

        config.set(
                ConfigOptions.METRICS_REPORTERS,
                Arrays.asList("reporter11", "reporter12", "reporter13"));

        final List<MetricReporter> metricReporters = ReporterSetup.fromConfiguration(config, null);

        assertThat(metricReporters).hasSize(3);

        assertThat(TestReporter11.wasOpened).isTrue();
        assertThat(TestReporter12.wasOpened).isTrue();
        assertThat(TestReporter13.wasOpened).isTrue();
    }

    /** Verifies that an error thrown by a plugin does not affect the setup of other reporters. */
    @Test
    void testPluginFailureIsolation() {
        final Configuration config = new Configuration();
        config.set(ConfigOptions.METRICS_REPORTERS, Arrays.asList("reporter11", "failingReporter"));

        final List<MetricReporter> metricReporters = ReporterSetup.fromConfiguration(config, null);

        assertThat(metricReporters).hasSize(1);
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestReporter11 extends TestReporter {
        public static boolean wasOpened = false;

        public TestReporter11() {
            super("reporter11");
        }

        @Override
        public void open(Configuration config) {
            wasOpened = true;
        }
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestReporter12 extends TestReporter {
        public static boolean wasOpened = false;

        public TestReporter12() {
            super("reporter12");
        }

        @Override
        public void open(Configuration config) {
            wasOpened = true;
        }
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestReporter13 extends TestReporter {
        public static boolean wasOpened = false;

        public TestReporter13() {
            super("reporter13");
        }

        @Override
        public void open(Configuration config) {
            wasOpened = true;
        }
    }

    private static void configureReporter1(Configuration config) {
        config.setString("arg1", "value1");
        config.setString("arg2", "value2");
    }

    private static void assertReporter1Configured(MetricReporter metricReporter) {
        final TestReporter testReporter = (TestReporter) metricReporter;
        Configuration reporterConfig = testReporter.getConfig();
        assertThat(reporterConfig.get(ConfigBuilder.key("arg1").stringType().noDefaultValue()))
                .isEqualTo("value1");
        assertThat(
                        reporterConfig.getString(
                                ConfigBuilder.key("arg2").stringType().noDefaultValue()))
                .isEqualTo("value2");
    }

    private static void configureReporter2(Configuration config) {
        config.setString("arg1", "value1");
        config.setString("arg3", "value3");
    }

    private static void assertReporter2Configured(MetricReporter metricReporter) {
        final TestReporter testReporter = (TestReporter) metricReporter;
        Configuration reporterConfig = testReporter.getConfig();
        assertThat(reporterConfig.get(ConfigBuilder.key("arg1").stringType().noDefaultValue()))
                .isEqualTo("value1");
        assertThat(
                        reporterConfig.getString(
                                ConfigBuilder.key("arg3").stringType().noDefaultValue()))
                .isEqualTo("value3");
    }

    /** Plugin that exposed the last provided metric config. */
    public static class ConfigExposingReporterPlugin implements MetricReporterPlugin {

        static Configuration lastConfig = null;

        private static final String name = "configExposingReporter";

        @Override
        public MetricReporter createMetricReporter(Configuration configuration) {
            lastConfig = configuration;
            return new TestReporter(name);
        }

        @Override
        public String identifier() {
            return name;
        }
    }

    /** Plugin that returns a static reporter. */
    public static class TestReporterPlugin implements MetricReporterPlugin {

        private static final String name = "testReporter";

        @Override
        public MetricReporter createMetricReporter(Configuration configuration) {
            return new TestReporter(name);
        }

        @Override
        public String identifier() {
            return name;
        }
    }

    /** Plugin that always throws an error. */
    public static class FailingPlugin implements MetricReporterPlugin {

        private static final String name = "failingReporter";

        @Override
        public MetricReporter createMetricReporter(Configuration configuration) {
            throw new RuntimeException();
        }

        @Override
        public String identifier() {
            return name;
        }
    }
}
