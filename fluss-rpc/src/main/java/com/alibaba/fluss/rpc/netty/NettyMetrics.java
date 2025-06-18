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

package com.alibaba.fluss.rpc.netty;

import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.MeterView;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.PoolArenaMetric;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocatorMetric;

/** A netty metrics class to register metrics from netty. */
public class NettyMetrics {

    public static final String NETTY_METRIC_GROUP = "netty";

    public static void registerNettyMetrics(
            MetricGroup metricGroup, PooledByteBufAllocator pooledAllocator) {
        MetricGroup nettyMetricGroup = metricGroup.addGroup(NETTY_METRIC_GROUP);
        PooledByteBufAllocatorMetric pooledAllocatorMetric = pooledAllocator.metric();
        nettyMetricGroup.<Long, Gauge<Long>>gauge(
                MetricNames.NETTY_USED_DIRECT_MEMORY, pooledAllocatorMetric::usedDirectMemory);
        nettyMetricGroup.<Integer, Gauge<Integer>>gauge(
                MetricNames.NETTY_NUM_DIRECT_ARENAS, pooledAllocatorMetric::numDirectArenas);
        nettyMetricGroup.meter(
                MetricNames.NETTY_NUM_ALLOCATIONS_PER_SECONDS,
                new MeterView(
                        () ->
                                pooledAllocatorMetric.directArenas().stream()
                                        .mapToLong(PoolArenaMetric::numAllocations)
                                        .sum()));
        nettyMetricGroup.meter(
                MetricNames.NETTY_NUM_HUGE_ALLOCATIONS_PER_SECONDS,
                new MeterView(
                        () ->
                                pooledAllocatorMetric.directArenas().stream()
                                        .mapToLong(PoolArenaMetric::numHugeAllocations)
                                        .sum()));
    }
}
