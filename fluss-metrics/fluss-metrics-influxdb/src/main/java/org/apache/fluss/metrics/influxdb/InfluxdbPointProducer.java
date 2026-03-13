/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.HistogramStatistics;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.Metric;

import com.influxdb.v3.client.Point;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Producer that creates InfluxDB {@link Point Points} from Fluss {@link Metric Metrics}. */
public class InfluxdbPointProducer {

    private static final InfluxdbPointProducer INSTANCE = new InfluxdbPointProducer();

    public static InfluxdbPointProducer getInstance() {
        return INSTANCE;
    }

    public Point createPoint(
            Metric metric, String metricName, List<Map.Entry<String, String>> tags, Instant time) {
        Point point = Point.measurement(metricName).setTimestamp(time);

        for (Map.Entry<String, String> tag : tags) {
            point.setTag(tag.getKey(), tag.getValue());
        }

        if (metric instanceof Counter) {
            return createPointForCounter((Counter) metric, point);
        }

        if (metric instanceof Gauge) {
            return createPointForGauge((Gauge<?>) metric, point);
        }

        if (metric instanceof Meter) {
            return createPointForMeter((Meter) metric, point);
        }

        if (metric instanceof Histogram) {
            return createPointForHistogram((Histogram) metric, point);
        }

        throw new IllegalArgumentException("Unknown metric type: " + metric.getClass());
    }

    private Point createPointForCounter(Counter counter, Point point) {
        return point.setField("count", counter.getCount());
    }

    private Point createPointForGauge(Gauge<?> gauge, Point point) {
        Object value = gauge.getValue();

        if (value instanceof Number) {
            return point.setField("value", ((Number) value));
        } else if (value instanceof Boolean) {
            return point.setField("value", ((boolean) value));
        } else {
            return point.setField("value", String.valueOf(value));
        }
    }

    private Point createPointForMeter(Meter meter, Point point) {
        return point.setField("rate", meter.getRate()).setField("count", meter.getCount());
    }

    private Point createPointForHistogram(Histogram histogram, Point point) {
        HistogramStatistics stats = histogram.getStatistics();
        return point.setField("count", histogram.getCount())
                .setField("mean", stats.getMean())
                .setField("stddev", stats.getStdDev())
                .setField("min", stats.getMin())
                .setField("max", stats.getMax())
                .setField("p50", stats.getQuantile(0.5))
                .setField("p75", stats.getQuantile(0.75))
                .setField("p95", stats.getQuantile(0.95))
                .setField("p98", stats.getQuantile(0.98))
                .setField("p99", stats.getQuantile(0.99))
                .setField("p999", stats.getQuantile(0.999));
    }
}
