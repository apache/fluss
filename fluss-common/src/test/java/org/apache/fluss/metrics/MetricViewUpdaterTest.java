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

import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.testutils.common.ScheduledTask;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MetricViewUpdater}. */
class MetricViewUpdaterTest {

    @Test
    void testViewUpdate() {
        ManuallyTriggeredScheduledExecutorService scheduledExecutorService =
                new ManuallyTriggeredScheduledExecutorService();
        MetricViewUpdater metricViewUpdater = new MetricViewUpdater(scheduledExecutorService);

        MeterView m = new MeterView(1);
        m.markEvent();

        metricViewUpdater.notifyOfAddedView(m);

        // get all scheduled tasks
        List<ScheduledFuture<?>> scheduledFutures = scheduledExecutorService.getAllScheduledTasks();
        // trigger all
        scheduledExecutorService.triggerScheduledTasks();

        // schedule the tasks again
        scheduleTasks(scheduledExecutorService, scheduledFutures);
        // trigger all
        scheduledExecutorService.triggerScheduledTasks();

        assertThat(m.getRate()).isEqualTo(0.2); // 1 / 5 = 0.2

        metricViewUpdater.notifyOfRemovedView(m);
        m.markEvent();
        // schedule the tasks again
        scheduleTasks(scheduledExecutorService, scheduledFutures);
        // trigger all
        scheduledExecutorService.triggerScheduledTasks();
        // shouldn't change since the view is removed
        assertThat(m.getRate()).isEqualTo(0.2);
    }

    private void scheduleTasks(
            ScheduledExecutorService executorService, List<ScheduledFuture<?>> scheduledFutures) {
        for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
            ScheduledTask<?> scheduledTask = (ScheduledTask<?>) scheduledFuture;
            executorService.scheduleWithFixedDelay(
                    () -> {
                        try {
                            scheduledTask.getCallable().call();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    0,
                    0,
                    TimeUnit.MILLISECONDS);
        }
    }
}
