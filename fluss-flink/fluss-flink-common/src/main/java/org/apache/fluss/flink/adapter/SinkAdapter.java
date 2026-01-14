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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import java.io.IOException;

/**
 * Flink sink adapter which hide the different version of createWriter method.
 *
 * <p>TODO: remove this class when no longer support all the Flink 1.x series.
 */
public abstract class SinkAdapter<InputT> implements Sink<InputT> {

    @Override
    public SinkWriter<InputT> createWriter(InitContext initContext) throws IOException {
        return createWriter(initContext.getMailboxExecutor(), initContext.metricGroup());
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext writerInitContext) throws IOException {
        return createWriter(
                writerInitContext.getMailboxExecutor(), writerInitContext.metricGroup());
    }

    protected abstract SinkWriter<InputT> createWriter(
            MailboxExecutor mailboxExecutor, SinkWriterMetricGroup metricGroup);
}
