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

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.registry.MetricRegistry;

import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Metrics for the users per table in server with {@link TabletServerMetricGroup} as parent group.
 */
public class UserPerTableMetricGroup extends AbstractUserMetricGroup {

    private final TablePath tablePath;

    public UserPerTableMetricGroup(
            MetricRegistry registry,
            String principalName,
            TablePath tablePath,
            long inactiveMetricExpirationTimeMs,
            TabletServerMetricGroup tabletServerMetricGroup) {
        super(registry, principalName, inactiveMetricExpirationTimeMs, tabletServerMetricGroup);
        this.tablePath = checkNotNull(tablePath);

        // only track counters for per-table user metrics for billing purposes,
        // the corresponding rates are tracked at the overall user level
        counter(MetricNames.BYTES_IN, bytesIn);
        counter(MetricNames.BYTES_OUT, bytesOut);
    }

    @Override
    protected void putVariables(Map<String, String> variables) {
        super.putVariables(variables);
        variables.put("database", tablePath.getDatabaseName());
        variables.put("table", tablePath.getTableName());
    }
}
