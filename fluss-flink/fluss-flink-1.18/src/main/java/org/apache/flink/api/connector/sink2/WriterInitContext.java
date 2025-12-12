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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.api.common.TaskInfo;

/**
 * Compatibility adapter for WriterInitContext in Flink 1.18.
 *
 * <p>In Flink 1.18, the InitContext interface doesn't have getTaskInfo() and getJobInfo() methods,
 * This interface extends InitContext and provides default implementations of these methods using
 * the existing API.
 */
public interface WriterInitContext extends Sink.InitContext {

    /**
     * Gets the TaskInfo for the parallel subtask. This method provides task-related metadata such
     * as subtask index and parallelism.
     *
     * <p>In Flink 1.18, we create a TaskInfo instance based on the available context methods.
     *
     * @return TaskInfo containing metadata about this subtask.
     */
    default TaskInfo getTaskInfo() {
        int subtaskIndex = getSubtaskId();
        int parallelism = getNumberOfParallelSubtasks();
        return new TaskInfo("FlinkSinkTask", parallelism, subtaskIndex, parallelism, 0, "UNKNOWN");
    }

    /**
     * Gets the JobInfo for the current job. This method provides job-related metadata such as job
     * ID.
     *
     * <p>In Flink 1.18, there is no JobInfo interface, so we create a simple adapter.
     *
     * @return JobInfo containing metadata about the current job.
     */
    default org.apache.flink.api.common.JobInfo getJobInfo() {
        return new SimpleJobInfo(getJobId());
    }

    /** Simple implementation of JobInfo for Flink 1.18 compatibility. */
    class SimpleJobInfo implements org.apache.flink.api.common.JobInfo {
        private final org.apache.flink.api.common.JobID jobId;

        SimpleJobInfo(org.apache.flink.api.common.JobID jobId) {
            this.jobId = jobId;
        }

        @Override
        public org.apache.flink.api.common.JobID getJobId() {
            return jobId;
        }
    }
}
