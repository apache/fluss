/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.registry.MetricRegistry;

/**
 * Metrics for the overall user level in server with {@link TabletServerMetricGroup} as parent
 * group.
 */
public class UserMetricGroup extends AbstractUserMetricGroup {

    public UserMetricGroup(
            MetricRegistry registry,
            String principalName,
            long inactiveMetricExpirationTimeMs,
            TabletServerMetricGroup tabletServerMetricGroup) {
        super(registry, principalName, inactiveMetricExpirationTimeMs, tabletServerMetricGroup);

        meter(MetricNames.BYTES_IN_RATE, new MeterView(bytesIn));
        meter(MetricNames.BYTES_OUT_RATE, new MeterView(bytesOut));
    }
}
