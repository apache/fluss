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

import java.lang.reflect.Field;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * An adapter for Flink {@link RuntimeContext} class. The {@link RuntimeContext} class added the
 * `getJobInfo` and `getTaskInfo` methods in version 1.19 and deprecated many methods, such as
 * `getAttemptNumber`.
 *
 * <p>Flink 1.x does not expose the runtime information on {@link FunctionContext}, so this
 * version-neutral implementation reads the wrapped runtime context from the function context. The
 * Flink 2.x modules shadow this class to ask the function context for the task info directly.
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

    /**
     * Gets the index of this subtask from the given function context.
     *
     * @param functionContext the function context to read the subtask index from
     * @return the index of this subtask
     */
    public static int getIndexOfThisSubtask(FunctionContext functionContext) {
        return getRuntimeContext(functionContext).getTaskInfo().getIndexOfThisSubtask();
    }

    /**
     * Gets the number of parallel subtasks from the given function context.
     *
     * @param functionContext the function context to read the parallelism from
     * @return the number of parallel subtasks
     */
    public static int getNumberOfParallelSubtasks(FunctionContext functionContext) {
        return getRuntimeContext(functionContext).getTaskInfo().getNumberOfParallelSubtasks();
    }

    /**
     * Extracts the runtime context wrapped by the given function context.
     *
     * <p>{@link FunctionContext} only exposes the runtime information from Flink 2.x on, so the
     * private runtime context field is read reflectively.
     */
    private static RuntimeContext getRuntimeContext(FunctionContext functionContext) {
        RuntimeContext runtimeContext;
        try {
            Field field = FunctionContext.class.getDeclaredField("context");
            field.setAccessible(true);
            runtimeContext = (RuntimeContext) field.get(functionContext);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(
                    "Unable to extract the runtime context from the Flink function context.", e);
        }
        checkState(
                runtimeContext != null,
                "The Flink function context does not carry a runtime context, so the subtask "
                        + "information is unavailable.");
        return runtimeContext;
    }
}
