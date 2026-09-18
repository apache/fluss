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

package org.apache.fluss.metrics.groups;

import org.apache.fluss.metrics.filter.MetricFilter;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Encapsulates all settings that are defined per reporter. */
public class ReporterScopedSettings {

    private final int reporterIndex;
    private final MetricFilter filter;

    /** Creates settings that allow all metrics for the given reporter index. */
    public ReporterScopedSettings(int reporterIndex) {
        this(reporterIndex, MetricFilter.NO_OP_FILTER);
    }

    /** Creates settings with the given reporter index and metric filter. */
    public ReporterScopedSettings(int reporterIndex, MetricFilter filter) {
        checkArgument(reporterIndex >= 0);
        this.reporterIndex = reporterIndex;
        this.filter = checkNotNull(filter);
    }

    /** Returns the reporter's index in the registry. */
    public int getReporterIndex() {
        return reporterIndex;
    }

    /** Returns the filter applied before notifying this reporter of metric changes. */
    public MetricFilter getFilter() {
        return filter;
    }
}
