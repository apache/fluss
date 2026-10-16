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

package org.apache.fluss.metrics.datadog;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.Password;
import org.apache.fluss.metrics.reporter.MetricReporter;
import org.apache.fluss.metrics.reporter.MetricReporterPlugin;
import org.apache.fluss.utils.StringUtils;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_API_KEY;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_MAX_METRICS_PER_REQUEST;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_PROXY_HOST;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_PROXY_PORT;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_PUSH_INTERVAL;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_SITE;
import static org.apache.fluss.config.ConfigOptions.METRICS_REPORTER_DATADOG_TAGS;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** {@link MetricReporterPlugin} for {@link DatadogReporter}. */
public class DatadogReporterPlugin implements MetricReporterPlugin {

    private static final String PLUGIN_NAME = "datadog";
    private static final String SERIES_PATH = "/api/v2/series";

    @Override
    public MetricReporter createMetricReporter(Configuration configuration) {
        Password apiKey = configuration.get(METRICS_REPORTER_DATADOG_API_KEY);
        checkArgument(
                apiKey != null && !StringUtils.isNullOrWhitespaceOnly(apiKey.value()),
                "The Datadog API key must be configured via '%s'.",
                METRICS_REPORTER_DATADOG_API_KEY.key());

        String site = configuration.get(METRICS_REPORTER_DATADOG_SITE).trim();
        // The site becomes the host name of the API endpoint, so it may not add a scheme or path.
        checkArgument(
                !site.isEmpty() && site.matches("[A-Za-z0-9.-]+"),
                "Invalid Datadog site '%s' in '%s': expected a host name such as 'datadoghq.com'.",
                site,
                METRICS_REPORTER_DATADOG_SITE.key());

        int maxMetricsPerRequest =
                configuration.get(METRICS_REPORTER_DATADOG_MAX_METRICS_PER_REQUEST);
        checkArgument(
                maxMetricsPerRequest > 0,
                "'%s' must be positive, got %s.",
                METRICS_REPORTER_DATADOG_MAX_METRICS_PER_REQUEST.key(),
                maxMetricsPerRequest);

        Duration pushInterval = configuration.get(METRICS_REPORTER_DATADOG_PUSH_INTERVAL);
        List<String> tags = configuration.get(METRICS_REPORTER_DATADOG_TAGS);

        String proxyHost = configuration.get(METRICS_REPORTER_DATADOG_PROXY_HOST);
        Proxy proxy =
                StringUtils.isNullOrWhitespaceOnly(proxyHost)
                        ? null
                        : new Proxy(
                                Proxy.Type.HTTP,
                                InetSocketAddress.createUnresolved(
                                        proxyHost.trim(),
                                        configuration.get(METRICS_REPORTER_DATADOG_PROXY_PORT)));

        URL seriesUrl;
        try {
            seriesUrl = new URL("https://api." + site + SERIES_PATH);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid Datadog site '" + site + "'.", e);
        }

        return new DatadogReporter(
                new DatadogHttpClient(seriesUrl, apiKey, proxy, pushInterval.getSeconds()),
                pushInterval,
                maxMetricsPerRequest,
                tags == null ? Collections.emptyList() : tags);
    }

    @Override
    public String identifier() {
        return PLUGIN_NAME;
    }
}
