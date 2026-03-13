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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.reporter.ScheduledMetricReporter;
import org.apache.fluss.utils.MapUtils;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.config.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link ScheduledMetricReporter} that exports {@link Metric Metrics} via InfluxDB. */
public class InfluxdbReporter implements ScheduledMetricReporter {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxdbReporter.class);

    private static final char SCOPE_SEPARATOR = '_';
    private static final String SCOPE_PREFIX = "fluss" + SCOPE_SEPARATOR;

    private final Map<Metric, String> metricNames;
    private final Map<Metric, List<Map.Entry<String, String>>> metricTags;
    private final InfluxDBClient client;
    private final Duration pushInterval;
    private final InfluxdbPointProducer pointProducer;

    public InfluxdbReporter(
            String hostUrl, String org, String bucket, String token, Duration pushInterval) {
        ClientConfig clientConfig =
                new ClientConfig.Builder()
                        .host(hostUrl)
                        .token(token.toCharArray())
                        .organization(org)
                        .database(bucket)
                        .build();

        this.client = InfluxDBClient.getInstance(clientConfig);
        this.pushInterval = pushInterval;
        this.metricNames = MapUtils.newConcurrentHashMap();
        this.metricTags = MapUtils.newConcurrentHashMap();
        this.pointProducer = InfluxdbPointProducer.getInstance();

        LOG.info("Started InfluxDB reporter connecting to {}", hostUrl);
    }

    @Override
    public void open(Configuration config) {
        // do nothing
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("Failed to close InfluxDB client", e);
            }
        }
    }

    @Override
    public void report() {
        List<Point> points = new ArrayList<>();
        Instant now = Instant.now();

        for (Map.Entry<Metric, String> entry : metricNames.entrySet()) {
            Metric metric = entry.getKey();
            String metricName = entry.getValue();
            List<Map.Entry<String, String>> tags = metricTags.get(metric);

            try {
                Point point = pointProducer.createPoint(metric, metricName, tags, now);
                points.add(point);
            } catch (Exception e) {
                LOG.warn("Failed to create point for metric {}", metricName, e);
            }
        }

        client.writePoints(points);
    }

    @Override
    public Duration scheduleInterval() {
        return pushInterval;
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        String scopedMetricName = getScopedName(metricName, group);
        List<Map.Entry<String, String>> tags = getTags(group);

        metricNames.put(metric, scopedMetricName);
        metricTags.put(metric, tags);
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        metricNames.remove(metric);
        metricTags.remove(metric);
    }

    private String getScopedName(String metricName, MetricGroup group) {
        return SCOPE_PREFIX
                + group.getLogicalScope(this::filterCharacters, SCOPE_SEPARATOR)
                + SCOPE_SEPARATOR
                + filterCharacters(metricName);
    }

    private List<Map.Entry<String, String>> getTags(MetricGroup group) {
        List<Map.Entry<String, String>> tags = new ArrayList<>();
        for (Map.Entry<String, String> entry : group.getAllVariables().entrySet()) {
            tags.add(
                    new HashMap.SimpleEntry<>(
                            filterCharacters(entry.getKey()), filterCharacters(entry.getValue())));
        }
        return tags;
    }

    private String filterCharacters(String input) {
        return input.replaceAll("[^a-zA-Z0-9_:]", String.valueOf(SCOPE_SEPARATOR));
    }
}
