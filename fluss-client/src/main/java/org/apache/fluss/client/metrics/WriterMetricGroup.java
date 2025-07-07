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

import static org.apache.fluss.metrics.utils.MetricGroupUtils.makeScope;

/** Metrics for {@link WriterClient}. */
@Internal
public class WriterMetricGroup extends AbstractMetricGroup {
    private static final String name = "writer";
    private static final int WINDOW_SIZE = 1024;

    private final Counter recordsRetryTotal;
    private final Counter recordsSendTotal;
    private final Counter bytesSendTotal;
    private final Histogram bytesPerBatch;
    private final Histogram recordPerBatch;

    private volatile long sendLatencyInMs = -1;
    private volatile long batchQueueTimeMs = -1;

    public WriterMetricGroup(ClientMetricGroup parent) {
        super(parent.getMetricRegistry(), makeScope(parent, name), parent);

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
        return name;
    }
}
