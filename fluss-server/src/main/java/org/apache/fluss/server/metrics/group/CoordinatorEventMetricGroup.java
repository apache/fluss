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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;

import java.util.Map;

import static org.apache.fluss.metrics.utils.MetricGroupUtils.makeScope;

/**
 * Metric group for coordinator event types. This group adds an additional dimension "event_type" to
 * the metrics and manages event-specific metrics.
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
