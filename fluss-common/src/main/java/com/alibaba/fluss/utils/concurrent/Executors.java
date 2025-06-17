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

package com.alibaba.fluss.utils.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/** Collection of {@link Executor} and {@link ExecutorService} implementations. */
public class Executors {

    private Executors() {}

    /**
     * Return a direct executor. The direct executor directly executes the runnable in the calling
     * thread.
     *
     * @return Direct executor
     */
    public static Executor directExecutor() {
        return DirectExecutor.INSTANCE;
    }

    private enum DirectExecutor implements Executor {
        INSTANCE;

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }
}
