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

package com.alibaba.fluss.utils.concurrent;

import com.alibaba.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;

import javax.annotation.Nonnull;

import java.util.concurrent.Executor;

/** Simple {@link Executor} implementation for testing purposes. */
public class ManuallyTriggeredScheduledExecutor implements Executor {

    /**
     * The service that we redirect to. We wrap this rather than extending it to limit the surfaced
     * interface.
     */
    ManuallyTriggeredScheduledExecutorService execService =
            new ManuallyTriggeredScheduledExecutorService();

    @Override
    public void execute(@Nonnull Runnable command) {
        execService.execute(command);
    }

    /** Triggers all {@code queuedRunnables}. */
    public void triggerAll() {
        execService.triggerAll();
    }
}
