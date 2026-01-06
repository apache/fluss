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

package org.apache.fluss.metrics.prometheus;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.reporter.ScheduledMetricReporter;

import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;

import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_DELETE_ON_SHUTDOWN;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_FILTER_LABEL_VALUE_CHARACTERS;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_GROUPING_KEY;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_HOST_URL;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_JOB_NAME;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX;

/** {@link ScheduledMetricReporter} that pushes {@link Metric Metrics} to Prometheus PushGateway. */
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter
        implements ScheduledMetricReporter {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusPushGatewayReporter.class);

    private static final Random RANDOM = new Random();

    private PushGateway pushGateway;
    private String jobName;
    private String groupingKey;
    private boolean deleteOnShutdown;
    private boolean filterLabelValueCharacters;

    @Override
    public void open(Configuration config) {
        String hostUrl = config.getString(METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_HOST_URL);
        this.jobName = config.getString(METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_JOB_NAME);
        this.deleteOnShutdown =
                config.getBoolean(METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_DELETE_ON_SHUTDOWN);
        this.filterLabelValueCharacters =
                config.getBoolean(
                        METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_FILTER_LABEL_VALUE_CHARACTERS);

        if (config.getBoolean(METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX)) {
            this.jobName = jobName + "_" + RANDOM.nextLong();
        }

        String configuredGroupingKey =
                config.getString(METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_GROUPING_KEY);
        if (configuredGroupingKey != null) {
            this.groupingKey = configuredGroupingKey;
        }

        this.pushGateway = new PushGateway(hostUrl);
    }

    @Override
    public void close() {
        if (deleteOnShutdown) {
            try {
                pushGateway.delete(jobName, groupingKey);
                LOG.info("Deleted metrics from PushGateway.");
            } catch (IOException e) {
                LOG.warn("Could not delete metrics from PushGateway.", e);
            }
        }
        super.close();
    }

    @Override
    public Duration scheduleInterval() {
        return Duration.ofSeconds(60);
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        notifyOfAddedMetric(metric, metricName, group, filterLabelValueCharacters);
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        notifyOfRemovedMetric(metric, metricName, group, filterLabelValueCharacters);
    }

    @Override
    public void report() {
        try {
            pushGateway.push(registry, jobName, groupingKey);
        } catch (IOException e) {
            LOG.warn("Could not push metrics to PushGateway.", e);
        }
    }
}
