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

package org.apache.fluss.metrics.filter;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metrics.Metric;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 * Adapted from Flink 1.20.4 for Fluss package names, metric types and configuration access. */

/** A filter for metrics. */
@Internal
public interface MetricFilter {

    /** Filter that accepts every metric. */
    MetricFilter NO_OP_FILTER = (metric, name, scope) -> true;

    /**
     * Filters a given metric.
     *
     * @param metric the metric to filter
     * @param name the name of the metric
     * @param logicalScope the logical scope of the metric
     * @return true, if the metric matches, false otherwise
     */
    boolean filter(Metric metric, String name, String logicalScope);
}
