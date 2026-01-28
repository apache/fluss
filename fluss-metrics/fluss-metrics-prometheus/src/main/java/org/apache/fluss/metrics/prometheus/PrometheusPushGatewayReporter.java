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

import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.reporter.ScheduledMetricReporter;

import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.Map;

/** {@link ScheduledMetricReporter} that pushes {@link Metric Metrics} to Prometheus PushGateway. */
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter
        implements ScheduledMetricReporter {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusPushGatewayReporter.class);

    private final PushGateway pushGateway;
    private final String jobName;
    private final Map<String, String> groupingKey;
    private final boolean deleteOnShutdown;
    private final Duration pushInterval;

    public PrometheusPushGatewayReporter(
            URL hostUrl,
            String jobName,
            Map<String, String> groupingKey,
            final boolean deleteOnShutdown,
            Duration pushInterval) {
        this.pushGateway = new PushGateway(hostUrl);
        this.jobName = jobName;
        this.groupingKey = groupingKey;
        this.deleteOnShutdown = deleteOnShutdown;
        this.pushInterval = pushInterval;
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
        return pushInterval;
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
