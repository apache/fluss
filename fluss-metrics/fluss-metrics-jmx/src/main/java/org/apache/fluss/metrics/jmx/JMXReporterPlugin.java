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

package org.apache.fluss.metrics.jmx;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.reporter.MetricReporterPlugin;

/** {@link MetricReporterPlugin} for {@link JMXReporter}. */
public class JMXReporterPlugin implements MetricReporterPlugin {

    private static final String name = "jmx";

    @Override
    public JMXReporter createMetricReporter(Configuration configuration) {
        String portsConfig = configuration.getString(ConfigOptions.METRICS_REPORTER_JMX_HOST);
        return new JMXReporter(portsConfig);
    }

    @Override
    public String identifier() {
        return name;
    }
}
