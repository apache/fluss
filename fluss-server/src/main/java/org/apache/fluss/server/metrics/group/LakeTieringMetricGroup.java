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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.fluss.metrics.utils.MetricGroupUtils.makeScope;

/** Metrics for lake tiering. */
public class LakeTieringMetricGroup extends AbstractMetricGroup {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTieringMetricGroup.class);

    public LakeTieringMetricGroup(MetricRegistry registry, CoordinatorMetricGroup parent) {
        super(registry, makeScope(parent), parent);
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "lakeTiering";
    }
}
