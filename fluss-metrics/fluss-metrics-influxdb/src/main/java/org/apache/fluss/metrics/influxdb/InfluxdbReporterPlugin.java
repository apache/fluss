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
import org.apache.fluss.metrics.reporter.MetricReporter;
import org.apache.fluss.metrics.reporter.MetricReporterPlugin;
import org.apache.fluss.utils.StringUtils;

import java.time.Duration;

import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_INFLUXDB_BUCKET;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_INFLUXDB_HOST_URL;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_INFLUXDB_ORG;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_INFLUXDB_PUSH_INTERVAL;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_INFLUXDB_TOKEN;

/** {@link MetricReporterPlugin} for {@link InfluxdbReporter}. */
public class InfluxdbReporterPlugin implements MetricReporterPlugin {

    private static final String PLUGIN_NAME = "influxdb";

    @Override
    public MetricReporter createMetricReporter(Configuration configuration) {
        String hostUrl = configuration.getString(METRICS_REPORTER_INFLUXDB_HOST_URL);
        String org = configuration.getString(METRICS_REPORTER_INFLUXDB_ORG);
        String bucket = configuration.getString(METRICS_REPORTER_INFLUXDB_BUCKET);
        String token = configuration.getString(METRICS_REPORTER_INFLUXDB_TOKEN);
        Duration pushInterval = configuration.get(METRICS_REPORTER_INFLUXDB_PUSH_INTERVAL);

        if (StringUtils.isNullOrWhitespaceOnly(hostUrl)) {
            throw new IllegalArgumentException(
                    "InfluxDB host URL must be configured via '"
                            + METRICS_REPORTER_INFLUXDB_HOST_URL.key()
                            + "'");
        }
        if (StringUtils.isNullOrWhitespaceOnly(org)) {
            throw new IllegalArgumentException(
                    "InfluxDB organization must be configured via '"
                            + METRICS_REPORTER_INFLUXDB_ORG.key()
                            + "'");
        }
        if (StringUtils.isNullOrWhitespaceOnly(bucket)) {
            throw new IllegalArgumentException(
                    "InfluxDB bucket must be configured via '"
                            + METRICS_REPORTER_INFLUXDB_BUCKET.key()
                            + "'");
        }
        if (StringUtils.isNullOrWhitespaceOnly(token)) {
            throw new IllegalArgumentException(
                    "InfluxDB token must be configured via '"
                            + METRICS_REPORTER_INFLUXDB_TOKEN.key()
                            + "'");
        }

        return new InfluxdbReporter(hostUrl, org, bucket, token, pushInterval);
    }

    @Override
    public String identifier() {
        return PLUGIN_NAME;
    }
}
