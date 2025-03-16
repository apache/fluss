/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.metrics.groups;

import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.metrics.Meter;

import java.util.Map;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Metric group which forwards all registration calls to its parent metric group.
 *
 * @param <P> Type of the parent metric group
 */
public class ProxyMetricGroup<P extends MetricGroup> implements MetricGroup {

    protected final P parentMetricGroup;

    public ProxyMetricGroup(P parentMetricGroup) {
        this.parentMetricGroup = checkNotNull(parentMetricGroup);
    }

    @Override
    public final Counter counter(String name) {
        return parentMetricGroup.counter(name);
    }

    @Override
    public final <C extends Counter> C counter(String name, C counter) {
        return parentMetricGroup.counter(name, counter);
    }

    @Override
    public final <T, G extends Gauge<T>> G gauge(String name, G gauge) {
        return parentMetricGroup.gauge(name, gauge);
    }

    @Override
    public final <H extends Histogram> H histogram(String name, H histogram) {
        return parentMetricGroup.histogram(name, histogram);
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        return parentMetricGroup.meter(name, meter);
    }

    @Override
    public final MetricGroup addGroup(String name) {
        return parentMetricGroup.addGroup(name);
    }

    @Override
    public final MetricGroup addGroup(String key, String value) {
        return parentMetricGroup.addGroup(key, value);
    }

    @Override
    public String getLogicalScope(CharacterFilter filter, char delimiter) {
        return parentMetricGroup.getLogicalScope(filter, delimiter);
    }

    @Override
    public String[] getScopeComponents() {
        return parentMetricGroup.getScopeComponents();
    }

    @Override
    public Map<String, String> getAllVariables() {
        return parentMetricGroup.getAllVariables();
    }

    @Override
    public void close() {
        parentMetricGroup.close();
    }
}
