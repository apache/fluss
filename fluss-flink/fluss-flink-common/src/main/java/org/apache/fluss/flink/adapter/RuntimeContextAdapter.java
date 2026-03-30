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

package org.apache.fluss.flink.adapter;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

/**
 * An adapter for Flink {@link RuntimeContext} class. The {@link RuntimeContext} class added the
 * `getJobInfo` and `getTaskInfo` methods in version 1.19 and deprecated many methods, such as
 * `getAttemptNumber`.
 *
 * <p>TODO: remove this class when no longer support flink 1.18.
 */
public class RuntimeContextAdapter {

    public static int getAttemptNumber(RuntimeContext runtimeContext) {
        return runtimeContext.getTaskInfo().getAttemptNumber();
    }

    public static int getIndexOfThisSubtask(StreamingRuntimeContext runtimeContext) {
        return runtimeContext.getTaskInfo().getIndexOfThisSubtask();
    }

    public static int getNumberOfParallelSubtasks(StreamingRuntimeContext runtimeContext) {
        return runtimeContext.getTaskInfo().getNumberOfParallelSubtasks();
    }

    /**
     * Gets the JobID from the RuntimeContext.
     *
     * <p>In Flink 1.19+, use getJobInfo().getJobId(). In Flink 1.18, the shim overrides this method
     * to use the direct getJobId() method.
     *
     * @param runtimeContext the runtime context
     * @return the JobID
     */
    public static JobID getJobId(RuntimeContext runtimeContext) {
        return runtimeContext.getJobInfo().getJobId();
    }
}
