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

package org.apache.fluss.client.metrics;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.fluss.metrics.utils.MetricGroupUtils.makeScope;

/**
 * Metrics for {@link WriterClient}.
 *
 * <p>The connection-level shared writer client uses the {@code client.writer} scope without any
 * extra variables. Writer clients that may have multiple instances under the same {@link
 * ClientMetricGroup} should use a subclass with a dedicated scope and a disambiguating {@code
 * writer_instance_id} variable, see {@link MultiTableWriterMetricGroup}.
 */
@Internal
public class WriterMetricGroup extends AbstractMetricGroup {
    private static final String name = "writer";
    private static final int WINDOW_SIZE = 1024;

    private final String groupName;

    /** Only set for subclasses whose instances may coexist under the same parent. */
    @Nullable private final String writerInstanceId;

    private final Counter recordsRetryTotal;
    private final Counter recordsSendTotal;
    private final Counter bytesSendTotal;
    private final Histogram bytesPerBatch;
    private final Histogram recordPerBatch;

    private volatile long sendLatencyInMs = -1;
    private volatile long batchQueueTimeMs = -1;

    public WriterMetricGroup(ClientMetricGroup parent) {
        this(parent, name, null);
    }

    protected WriterMetricGroup(
            ClientMetricGroup parent, String groupName, @Nullable String writerInstanceId) {
        super(parent.getMetricRegistry(), makeScope(parent, groupName), parent);
        this.groupName = groupName;
        // must be assigned before the first metric registration below, as reporters resolve
        // getAllVariables() (which reads writerInstanceId) when a metric is added
        this.writerInstanceId = writerInstanceId;

        gauge(MetricNames.WRITER_BATCH_QUEUE_TIME_MS, () -> batchQueueTimeMs);

        recordsRetryTotal = new ThreadSafeSimpleCounter();
        meter(MetricNames.WRITER_RECORDS_RETRY_RATE, new MeterView(recordsRetryTotal));
        recordsSendTotal = new ThreadSafeSimpleCounter();
        meter(MetricNames.WRITER_RECORDS_SEND_RATE, new MeterView(recordsSendTotal));
        bytesSendTotal = new ThreadSafeSimpleCounter();
        meter(MetricNames.WRITER_BYTES_SEND_RATE, new MeterView(bytesSendTotal));
        gauge(MetricNames.WRITER_SEND_LATENCY_MS, () -> sendLatencyInMs);

        bytesPerBatch =
                histogram(
                        MetricNames.WRITER_BYTES_PER_BATCH,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        recordPerBatch =
                histogram(
                        MetricNames.WRITER_RECORDS_PER_BATCH,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
    }

    public void setBatchQueueTimeMs(long batchQueueTimeMs) {
        this.batchQueueTimeMs = batchQueueTimeMs;
    }

    public void setSendLatencyInMs(long sendLatencyInMs) {
        this.sendLatencyInMs = sendLatencyInMs;
    }

    public Counter recordsRetryTotal() {
        return recordsRetryTotal;
    }

    public Counter recordsSendTotal() {
        return recordsSendTotal;
    }

    public Histogram bytesPerBatch() {
        return bytesPerBatch;
    }

    public Counter bytesSendTotal() {
        return bytesSendTotal;
    }

    public Histogram recordPerBatch() {
        return recordPerBatch;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return groupName;
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        if (writerInstanceId != null) {
            variables.put("writer_instance_id", writerInstanceId);
        }
    }
}
