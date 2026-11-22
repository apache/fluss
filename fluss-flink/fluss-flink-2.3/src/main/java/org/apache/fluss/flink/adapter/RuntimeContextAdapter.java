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
import org.apache.flink.table.functions.FunctionContext;

/**
 * An adapter for Flink {@link RuntimeContext} class for Flink 2.x. It shadows the version-neutral
 * class in fluss-flink-common.
 *
 * <p>In Flink 2.x, the job and task information is only available through getJobInfo() and
 * getTaskInfo(), and {@link FunctionContext} exposes the task info directly, so no reflection is
 * needed to read the subtask information.
 *
 * <p>TODO: remove this class when no longer supporting Flink 1.x.
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
     * @param runtimeContext the runtime context
     * @return the JobID
     */
    public static JobID getJobId(RuntimeContext runtimeContext) {
        return runtimeContext.getJobInfo().getJobId();
    }

    /**
     * Gets the index of this subtask from the given function context.
     *
     * @param functionContext the function context to read the subtask index from
     * @return the index of this subtask
     */
    public static int getIndexOfThisSubtask(FunctionContext functionContext) {
        return functionContext.getTaskInfo().getIndexOfThisSubtask();
    }

    /**
     * Gets the number of parallel subtasks from the given function context.
     *
     * @param functionContext the function context to read the parallelism from
     * @return the number of parallel subtasks
     */
    public static int getNumberOfParallelSubtasks(FunctionContext functionContext) {
        return functionContext.getTaskInfo().getNumberOfParallelSubtasks();
    }
}
