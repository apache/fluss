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

package org.apache.fluss.server.coordinator.channel;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.metrics.groups.MetricGroup;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.BlockingQueue;

/**
 * Per-tablet-server channel state held by {@link
 * org.apache.fluss.server.coordinator.CoordinatorChannelManager}: the queue, the sender thread, and
 * the metric handles needed to deregister the per-TS metrics on remove.
 */
@ThreadSafe
public final class TabletServerChannelState {

    private final int tabletServerId;
    private final ServerNode serverNode;
    private final BlockingQueue<QueueItem> queue;
    private final ControlRequestSendThread sendThread;

    /**
     * The per-TS child {@link MetricGroup}. Held so {@code removeTabletServer} can {@code close()}
     * it and deregister all per-TS metrics in one shot.
     */
    private final MetricGroup metricGroup;

    public TabletServerChannelState(
            int tabletServerId,
            ServerNode serverNode,
            BlockingQueue<QueueItem> queue,
            ControlRequestSendThread sendThread,
            MetricGroup metricGroup) {
        this.tabletServerId = tabletServerId;
        this.serverNode = serverNode;
        this.queue = queue;
        this.sendThread = sendThread;
        this.metricGroup = metricGroup;
    }

    public BlockingQueue<QueueItem> getQueue() {
        return queue;
    }

    public ControlRequestSendThread getSendThread() {
        return sendThread;
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }
}
