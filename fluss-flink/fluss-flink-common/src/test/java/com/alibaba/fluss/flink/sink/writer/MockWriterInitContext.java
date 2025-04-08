/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.sink.writer;

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;

/** A mock implementation of {@link WriterInitContext} for testing purposes. */
public class MockWriterInitContext extends TestSinkInitContext {
    private OperatorMetricGroup operatorMetricGroup;

    public MockWriterInitContext(OperatorMetricGroup operatorMetricGroup) {
        this.operatorMetricGroup = operatorMetricGroup;
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return InternalSinkWriterMetricGroup.wrap(operatorMetricGroup);
    }
}
