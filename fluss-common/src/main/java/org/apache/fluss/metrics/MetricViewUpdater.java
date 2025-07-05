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

package org.apache.fluss.metrics;

import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.metrics.MetricView.UPDATE_INTERVAL_SECONDS;

/**
 * The MeterViewUpdater is responsible for updating all metrics that implement the {@link
 * MetricView} interface.
 */
public class MetricViewUpdater {

    private final Set<MetricView> toAdd = new HashSet<>();
    private final Set<MetricView> toRemove = new HashSet<>();

    private final Object lock = new Object();

    public MetricViewUpdater(ScheduledExecutorService executor) {
        executor.scheduleWithFixedDelay(
                new MeterViewUpdaterTask(lock, toAdd, toRemove),
                5,
                UPDATE_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
    }

    /**
     * Notifies this ViewUpdater of a new metric that should be regularly updated.
     *
     * @param metricView meter that should be regularly updated
     */
    public void notifyOfAddedView(MetricView metricView) {
        synchronized (lock) {
            toAdd.add(metricView);
        }
    }

    /**
     * Notifies this ViewUpdater of a metric that should no longer be regularly updated.
     *
     * @param metricView meter that should no longer be regularly updated
     */
    public void notifyOfRemovedView(MetricView metricView) {
        synchronized (lock) {
            toRemove.add(metricView);
        }
    }

    /** The TimerTask doing the actual updating. */
    private static class MeterViewUpdaterTask extends TimerTask {
        private final Object lock;
        private final Set<MetricView> metricViews;
        private final Set<MetricView> toAdd;
        private final Set<MetricView> toRemove;

        private MeterViewUpdaterTask(Object lock, Set<MetricView> toAdd, Set<MetricView> toRemove) {
            this.lock = lock;
            this.metricViews = new HashSet<>();
            this.toAdd = toAdd;
            this.toRemove = toRemove;
        }

        @Override
        public void run() {
            for (MetricView toUpdate : this.metricViews) {
                toUpdate.update();
            }

            synchronized (lock) {
                metricViews.addAll(toAdd);
                toAdd.clear();
                metricViews.removeAll(toRemove);
                toRemove.clear();
            }
        }
    }
}
