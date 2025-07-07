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

package org.apache.fluss.metrics.registry;

import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;

import java.util.concurrent.CompletableFuture;

/** A metric registry do nothing. */
public class NOPMetricRegistry implements MetricRegistry {

    public static final MetricRegistry INSTANCE = new NOPMetricRegistry();

    public NOPMetricRegistry() {}

    @Override
    public int getNumberReporters() {
        return 0;
    }

    @Override
    public void register(Metric metric, String metricName, AbstractMetricGroup group) {
        // do nothing
    }

    @Override
    public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
        // do nothing
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    }
}
