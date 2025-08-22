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

package com.alibaba.fluss.server.metrics.group;

import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.DescriptiveStatisticsHistogram;
import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.ThreadSafeSimpleCounter;
import com.alibaba.fluss.metrics.groups.AbstractMetricGroup;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.server.coordinator.event.CoordinatorEvent;

import java.util.Map;

import static com.alibaba.fluss.metrics.utils.MetricGroupUtils.makeScope;

/**
 * Metric group for coordinator event types. This group adds an additional dimension "event_type" to
 * the metrics and manages event-specific metrics like queue size and processing time.
 */
public class CoordinatorEventMetricGroup extends AbstractMetricGroup {

    private final Class<? extends CoordinatorEvent> eventClass;
    private final Histogram eventProcessingTime;
    private final Counter queuedEventCount;

    public CoordinatorEventMetricGroup(
            MetricRegistry registry,
            Class<? extends CoordinatorEvent> eventClass,
            CoordinatorMetricGroup parent) {
        super(registry, makeScope(parent, eventClass.getSimpleName()), parent);
        this.eventClass = eventClass;

        this.eventProcessingTime =
                histogram(
                        MetricNames.EVENT_PROCESSING_TIME_MS,
                        new DescriptiveStatisticsHistogram(100));
        this.queuedEventCount =
                counter(MetricNames.EVENT_QUEUE_SIZE, new ThreadSafeSimpleCounter());
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "event";
    }

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put("event_type", eventClass.getSimpleName());
    }

    /**
     * Returns the histogram for event processing time.
     *
     * @return the event processing time histogram
     */
    public Histogram eventProcessingTime() {
        return eventProcessingTime;
    }

    /**
     * Returns the counter for event count.
     *
     * @return the event count counter
     */
    public Counter queuedEventCount() {
        return queuedEventCount;
    }
}
