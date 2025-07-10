/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.connector.sink2;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import java.io.IOException;
import java.io.Serializable;

/**
 * Placeholder class to resolve compatibility issues. This placeholder class can be removed once we
 * drop the support of Flink v1.18 which requires the legacy InitContext interface.
 */
public interface Sink<InputT> extends Serializable {

    /**
     * Creates a {@link SinkWriter}.
     *
     * @param context the runtime context.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     */
    SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException;

    /** The interface exposes some runtime info for creating a {@link SinkWriter}. */
    interface InitContext {

        /**
         * Returns the mailbox executor that allows to execute {@link Runnable}s inside the task
         * thread in between record processing.
         *
         * <p>Note that this method should not be used per-record for performance reasons in the
         * same way as records should not be sent to the external system individually. Rather,
         * implementers are expected to batch records and only enqueue a single {@link Runnable} per
         * batch to handle the result.
         */
        MailboxExecutor getMailboxExecutor();

        /** @return The metric group this writer belongs to. */
        SinkWriterMetricGroup metricGroup();
    }
}
