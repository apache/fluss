/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.metrics.influxdb;

import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.util.TestHistogram;
import org.apache.fluss.metrics.util.TestMeter;

import com.influxdb.v3.client.Point;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link InfluxdbPointProducer}. */
class InfluxdbPointProducerTest {

    private final InfluxdbPointProducer pointProducer = InfluxdbPointProducer.getInstance();

    @Test
    void testCreatePointForCounter() {
        Counter counter = new SimpleCounter();
        counter.inc(42L);

        List<Map.Entry<String, String>> tags = new ArrayList<>();
        tags.add(new AbstractMap.SimpleEntry<>("host", "localhost"));
        tags.add(new AbstractMap.SimpleEntry<>("server", "test"));

        Instant time = Instant.now();
        Point point = pointProducer.createPoint(counter, "test_counter", tags, time);

        assertThat(point).isNotNull();
        assertThat(point.getMeasurement()).isEqualTo("test_counter");
        assertThat(point.getField("count")).isEqualTo(42L);
        assertThat(point.getTimestamp())
                .isEqualTo(
                        BigInteger.valueOf(
                                time.getEpochSecond() * 1_000_000_000L + time.getNano()));
    }

    @Test
    void testCreatePointForGauge() {
        List<Map.Entry<String, String>> tags = new ArrayList<>();
        tags.add(new AbstractMap.SimpleEntry<>("host", "localhost"));
        Instant time = Instant.now();

        // Test with Number (Integer)
        Gauge<Integer> intGauge = () -> 123;
        Point intPoint = pointProducer.createPoint(intGauge, "test_gauge_int", tags, time);
        assertThat(intPoint).isNotNull();
        assertThat(intPoint.getMeasurement()).isEqualTo("test_gauge_int");
        assertThat(intPoint.getField("value")).isEqualTo(123);
        assertThat(intPoint.getTimestamp())
                .isEqualTo(
                        BigInteger.valueOf(
                                time.getEpochSecond() * 1_000_000_000L + time.getNano()));

        // Test with Number (Double)
        Gauge<Double> doubleGauge = () -> 123.456;
        Point doublePoint = pointProducer.createPoint(doubleGauge, "test_gauge_double", tags, time);
        assertThat(doublePoint).isNotNull();
        assertThat(doublePoint.getMeasurement()).isEqualTo("test_gauge_double");
        assertThat(doublePoint.getField("value")).isEqualTo(123.456);

        // Test with Boolean
        Gauge<Boolean> boolGauge = () -> true;
        Point boolPoint = pointProducer.createPoint(boolGauge, "test_gauge_boolean", tags, time);
        assertThat(boolPoint).isNotNull();
        assertThat(boolPoint.getMeasurement()).isEqualTo("test_gauge_boolean");
        assertThat(boolPoint.getField("value")).isEqualTo(true);

        // Test with String
        Gauge<String> stringGauge = () -> "test_value";
        Point stringPoint = pointProducer.createPoint(stringGauge, "test_gauge_string", tags, time);
        assertThat(stringPoint).isNotNull();
        assertThat(stringPoint.getMeasurement()).isEqualTo("test_gauge_string");
        assertThat(stringPoint.getField("value")).isEqualTo("test_value");

        // Test with Null
        Gauge<Object> nullGauge = () -> null;
        Point nullPoint = pointProducer.createPoint(nullGauge, "test_gauge_null", tags, time);
        assertThat(nullPoint).isNotNull();
        assertThat(nullPoint.getMeasurement()).isEqualTo("test_gauge_null");
        assertThat(nullPoint.getField("value")).isEqualTo("null");
    }

    @Test
    void testCreatePointForMeter() {
        TestMeter meter = new TestMeter(1000, 5.0);

        List<Map.Entry<String, String>> tags = new ArrayList<>();
        tags.add(new AbstractMap.SimpleEntry<>("host", "localhost"));

        Instant time = Instant.now();
        Point point = pointProducer.createPoint(meter, "test_meter", tags, time);

        assertThat(point).isNotNull();
        assertThat(point.getMeasurement()).isEqualTo("test_meter");
        assertThat(point.getField("rate")).isEqualTo(5.0);
        assertThat(point.getField("count")).isEqualTo(1000L);
        assertThat(point.getTimestamp())
                .isEqualTo(
                        BigInteger.valueOf(
                                time.getEpochSecond() * 1_000_000_000L + time.getNano()));
    }

    @Test
    void testCreatePointForHistogram() {
        TestHistogram histogram = new TestHistogram();
        histogram.setCount(80);
        histogram.setMean(50.5);
        histogram.setStdDev(10.2);
        histogram.setMax(100);
        histogram.setMin(1);

        List<Map.Entry<String, String>> tags = new ArrayList<>();
        tags.add(new AbstractMap.SimpleEntry<>("host", "localhost"));

        Instant time = Instant.now();
        Point point = pointProducer.createPoint(histogram, "test_histogram", tags, time);

        assertThat(point).isNotNull();
        assertThat(point.getMeasurement()).isEqualTo("test_histogram");
        assertThat(point.getField("count")).isEqualTo(80L);
        assertThat(point.getField("mean")).isEqualTo(50.5);
        assertThat(point.getField("stddev")).isEqualTo(10.2);
        assertThat(point.getField("max")).isEqualTo(100L);
        assertThat(point.getField("min")).isEqualTo(1L);
        assertThat(point.getTimestamp())
                .isEqualTo(
                        BigInteger.valueOf(
                                time.getEpochSecond() * 1_000_000_000L + time.getNano()));
    }
}
