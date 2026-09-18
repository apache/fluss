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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.MetricType;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.util.TestMeter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.EnumSet;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 * Adapted from Flink 1.20.4 for Fluss package names, metric types and configuration access. */

/** Tests for {@link DefaultMetricFilter}. */
@Execution(ExecutionMode.CONCURRENT)
class DefaultMetricFilterTest {

    private static final Counter COUNTER = new SimpleCounter();
    private static final Meter METER = new TestMeter();
    private static final Gauge<Integer> GAUGE = () -> 4;

    @Test
    void testConvertToPatternWithoutWildcards() {
        final Pattern pattern = DefaultMetricFilter.convertToPattern("numRecordsIn");
        assertThat(pattern.toString()).isEqualTo("(numRecordsIn)");
        assertThat(pattern.matcher("numRecordsIn").matches()).isTrue();
        assertThat(pattern.matcher("numBytesOut").matches()).isFalse();
    }

    @Test
    void testConvertToPatternSingle() {
        final Pattern pattern = DefaultMetricFilter.convertToPattern("numRecords*");
        assertThat(pattern.toString()).isEqualTo("(numRecords.*)");
        assertThat(pattern.matcher("numRecordsIn").matches()).isTrue();
        assertThat(pattern.matcher("numBytesOut").matches()).isFalse();
    }

    @Test
    void testConvertToPatternMultiple() {
        final Pattern pattern = DefaultMetricFilter.convertToPattern("numRecords*,numBytes*");
        assertThat(pattern.toString()).isEqualTo("(numRecords.*|numBytes.*)");
        assertThat(pattern.matcher("numRecordsIn").matches()).isTrue();
        assertThat(pattern.matcher("numBytesOut").matches()).isTrue();
        assertThat(pattern.matcher("numBytes").matches()).isTrue();
        assertThat(pattern.matcher("hello").matches()).isFalse();
    }

    @Test
    void testConvertToPatternTrimsEntries() {
        final Pattern pattern = DefaultMetricFilter.convertToPattern("numRecordsIn, numBytesIn");

        assertThat(pattern.matcher("numRecordsIn").matches()).isTrue();
        assertThat(pattern.matcher("numBytesIn").matches()).isTrue();
    }

    @Test
    void testRegexMetacharactersAndEscaping() {
        final Pattern regex = DefaultMetricFilter.convertToPattern("bytes(In|Out)[0-9]+");
        assertThat(regex.matcher("bytesIn1").matches()).isTrue();
        assertThat(regex.matcher("bytesOut2").matches()).isTrue();
        assertThat(regex.matcher("bytesIn").matches()).isFalse();

        final Pattern literal = DefaultMetricFilter.convertToPattern("bytes\\.in\\[0\\]\\+");
        assertThat(literal.matcher("bytes.in[0]+").matches()).isTrue();
        assertThat(literal.matcher("bytesXin0").matches()).isFalse();
    }

    @Test
    void testParseMetricTypesSingle() {
        final EnumSet<MetricType> types = DefaultMetricFilter.parseMetricTypes("meter");
        assertThat(types).containsExactly(MetricType.METER);
    }

    @Test
    void testParseMetricTypesMultiple() {
        final EnumSet<MetricType> types = DefaultMetricFilter.parseMetricTypes("meter,counter");
        assertThat(types).containsExactlyInAnyOrder(MetricType.METER, MetricType.COUNTER);
    }

    @Test
    void testParseMetricTypesTrimsEntries() {
        final EnumSet<MetricType> types = DefaultMetricFilter.parseMetricTypes("counter, gauge");

        assertThat(types).containsExactlyInAnyOrder(MetricType.COUNTER, MetricType.GAUGE);
        assertThat(DefaultMetricFilter.parseMetricTypes(" * "))
                .containsExactlyInAnyOrder(MetricType.values());
    }

    @Test
    void testParseMetricTypesCaseIgnored() {
        final EnumSet<MetricType> types = DefaultMetricFilter.parseMetricTypes("meter,CoUnTeR");
        assertThat(types).containsExactlyInAnyOrder(MetricType.METER, MetricType.COUNTER);
    }

    @Test
    void testFromConfigurationIncludeByScope() {
        Configuration configuration = new Configuration();
        configuration.setString(
                "metrics.reporter.test.filter.includes", "include1:*:*;include2.*:*:*");
        configuration.setString("metrics.reporter.test.filter.excludes", "");

        final MetricFilter metricFilter =
                DefaultMetricFilter.fromConfiguration(configuration, "test");

        assertThat(metricFilter.filter(COUNTER, "name", "include1")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "name", "include1.bar")).isFalse();
        assertThat(metricFilter.filter(COUNTER, "name", "include2")).isFalse();
        assertThat(metricFilter.filter(COUNTER, "name", "include2.bar")).isTrue();
    }

    @Test
    void testFromConfigurationIncludeByName() {
        Configuration configuration = new Configuration();
        configuration.setString("metrics.reporter.test.filter.includes", "*:name:*");
        configuration.setString("metrics.reporter.test.filter.excludes", "");

        final MetricFilter metricFilter =
                DefaultMetricFilter.fromConfiguration(configuration, "test");

        assertThat(metricFilter.filter(COUNTER, "name", "bar")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isFalse();
    }

    @Test
    void testFromConfigurationIncludeByType() {
        Configuration configuration = new Configuration();
        configuration.setString("metrics.reporter.test.filter.includes", "*:*:counter");
        configuration.setString("metrics.reporter.test.filter.excludes", "");

        final MetricFilter metricFilter =
                DefaultMetricFilter.fromConfiguration(configuration, "test");

        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isTrue();
        assertThat(metricFilter.filter(METER, "foo", "bar")).isFalse();
    }

    @Test
    void testFromConfigurationExcludeByScope() {
        Configuration configuration = new Configuration();
        configuration.setString("metrics.reporter.test.filter.includes", "*:*:*");
        configuration.setString("metrics.reporter.test.filter.excludes", "include1;include2.*");

        final MetricFilter metricFilter =
                DefaultMetricFilter.fromConfiguration(configuration, "test");

        assertThat(metricFilter.filter(COUNTER, "name", "include1")).isFalse();
        assertThat(metricFilter.filter(COUNTER, "name", "include1.bar")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "name", "include2")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "name", "include2.bar")).isFalse();
    }

    @Test
    void testFromConfigurationExcludeByName() {
        Configuration configuration = new Configuration();
        configuration.setString("metrics.reporter.test.filter.includes", "*:*:*");
        configuration.setString("metrics.reporter.test.filter.excludes", "*:faa*;*:foo");

        final MetricFilter metricFilter =
                DefaultMetricFilter.fromConfiguration(configuration, "test");

        assertThat(metricFilter.filter(COUNTER, "name", "bar")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isFalse();
        assertThat(metricFilter.filter(COUNTER, "foob", "bar")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "faab", "bar")).isFalse();
    }

    @Test
    void testFromConfigurationExcludeByType() {
        Configuration configuration = new Configuration();
        configuration.setString("metrics.reporter.test.filter.includes", "*:*:*");
        configuration.setString("metrics.reporter.test.filter.excludes", "*:*:meter");

        final MetricFilter metricFilter =
                DefaultMetricFilter.fromConfiguration(configuration, "test");

        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isTrue();
        assertThat(metricFilter.filter(METER, "foo", "bar")).isFalse();
    }

    @Test
    void testFromConfigurationIncludeDefault() {
        Configuration configuration = new Configuration();
        configuration.setString("metrics.reporter.test.filter.excludes", "*:*:meter");

        final MetricFilter metricFilter =
                DefaultMetricFilter.fromConfiguration(configuration, "test");

        assertThat(metricFilter.filter(COUNTER, "foo", "hello")).isTrue();
        assertThat(metricFilter.filter(METER, "foo", "hello")).isFalse();
    }

    @Test
    void testFromConfigurationExcludeDefault() {
        Configuration configuration = new Configuration();
        configuration.setString("metrics.reporter.test.filter.includes", "*:*:*");

        final MetricFilter metricFilter =
                DefaultMetricFilter.fromConfiguration(configuration, "test");

        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isTrue();
    }

    @Test
    void testFromConfigurationAllDefault() {
        Configuration configuration = new Configuration();

        final MetricFilter metricFilter =
                DefaultMetricFilter.fromConfiguration(configuration, "test");

        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isTrue();
        assertThat(metricFilter.filter(METER, "foo", "bar")).isTrue();
    }

    @Test
    void testFromConfigurationMultiplePatterns() {
        Configuration configuration = new Configuration();

        configuration.setString("metrics.reporter.test.filter.excludes", "*:*:*");
        configuration.setString(
                "metrics.reporter.test.filter.excludes", "*:foo,bar:meter;*:foo,bar:gauge");

        final MetricFilter metricFilter =
                DefaultMetricFilter.fromConfiguration(configuration, "test");

        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isTrue();
        assertThat(metricFilter.filter(METER, "foo", "bar")).isFalse();
        assertThat(metricFilter.filter(GAUGE, "foo", "bar")).isFalse();
    }
}
