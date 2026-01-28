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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.reporter.MetricReporter;
import org.apache.fluss.metrics.reporter.MetricReporterPlugin;
import org.apache.fluss.utils.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_DELETE_ON_SHUTDOWN;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_GROUPING_KEY;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_HOST_URL;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_JOB_NAME;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_PUSH_INTERVAL;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX;

/** {@link MetricReporterPlugin} for {@link PrometheusPushGatewayReporter}. */
public class PrometheusPushGatewayReporterPlugin implements MetricReporterPlugin {
    private static final Logger LOG =
            LoggerFactory.getLogger(PrometheusPushGatewayReporterPlugin.class);

    private static final String PLUGIN_NAME = "prometheus-push";

    @Override
    public MetricReporter createMetricReporter(Configuration config) {
        String hostUrl = config.get(METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_HOST_URL);
        String configuredJobName = config.get(METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_JOB_NAME);
        boolean deleteOnShutdown =
                config.get(METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_DELETE_ON_SHUTDOWN);
        boolean randomSuffix =
                config.get(METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX);
        Duration pushInterval = config.get(METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_PUSH_INTERVAL);
        String jobName = configuredJobName;
        if (randomSuffix) {
            jobName = configuredJobName + new Random().nextLong();
        }
        Map<String, String> groupingKey =
                parseGroupingKey(config.get(METRICS_REPORTER_PROMETHEUS_PUSHGATEWAY_GROUPING_KEY));
        LOG.info(
                "Configured PrometheusPushGatewayReporter with {hostUrl:{}, jobName:{}, randomJobNameSuffix:{}, deleteOnShutdown:{}, groupingKey:{}, pushInterval:{}}",
                hostUrl,
                jobName,
                randomSuffix,
                deleteOnShutdown,
                groupingKey,
                pushInterval);
        try {
            return new PrometheusPushGatewayReporter(
                    new URL(hostUrl), jobName, groupingKey, deleteOnShutdown, pushInterval);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String identifier() {
        return PLUGIN_NAME;
    }

    @VisibleForTesting
    static Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
        if (!groupingKeyConfig.isEmpty()) {
            Map<String, String> groupingKey = new HashMap<>();
            String[] kvs = groupingKeyConfig.split(";");
            for (String kv : kvs) {
                int idx = kv.indexOf("=");
                if (idx < 0) {
                    LOG.warn("Invalid prometheusPushGateway groupingKey:{}, will be ignored", kv);
                    continue;
                }

                String labelKey = kv.substring(0, idx);
                String labelValue = kv.substring(idx + 1);
                if (StringUtils.isNullOrWhitespaceOnly(labelKey)
                        || StringUtils.isNullOrWhitespaceOnly(labelValue)) {
                    LOG.warn(
                            "Invalid groupingKey {labelKey:{}, labelValue:{}} must not be empty",
                            labelKey,
                            labelValue);
                    continue;
                }
                groupingKey.put(labelKey, labelValue);
            }

            return groupingKey;
        }

        return Collections.emptyMap();
    }
}
