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

/**
 * Metric group which forwards all registration calls to a variable parent metric group that injects
 * a variable reporter index into calls to {@link MetricGroup#getLogicalScope(CharacterFilter,
 * char)}. This allows us to use reporter-specific delimiters, without requiring any action by the
 * reporter.
 *
 * @param <P> parentMetricGroup to {@link AbstractMetricGroup AbstractMetricGroup}
 */
public class FrontMetricGroup<P extends AbstractMetricGroup> extends ProxyMetricGroup<P> {

    private final ReporterScopedSettings settings;

    public FrontMetricGroup(ReporterScopedSettings settings, P reference) {
        super(reference);
        this.settings = settings;
    }

    @Override
    public String getLogicalScope(CharacterFilter filter, char delimiter) {
        return parentMetricGroup.getLogicalScope(
                filter, delimiter, this.settings.getReporterIndex());
    }
}
