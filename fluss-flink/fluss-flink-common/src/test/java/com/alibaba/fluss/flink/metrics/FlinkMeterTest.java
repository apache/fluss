/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.metrics;

import com.alibaba.fluss.metrics.Meter;
import com.alibaba.fluss.metrics.util.TestMeter;

import org.apache.flink.metrics.MetricType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FlinkMeterTest {

    private static final double DELTA = 0.0001;

    @Test
    void testWrapper() {
        Meter meter = new TestMeter();

        FlinkMeter wrapper = new FlinkMeter(meter);
        assertThat(wrapper.getCount()).isEqualTo(100);
        assertThat(wrapper.getRate()).isEqualTo(5);
        assertThat(wrapper.getMetricType()).isEqualTo(MetricType.METER);
        assertThat(wrapper.getCount()).isEqualTo(100L);
    }

    @Test
    void testMarkEvent() {
        Meter meter = mock(Meter.class);

        FlinkMeter wrapper = new FlinkMeter(meter);
        wrapper.markEvent();

        verify(meter).markEvent();
    }

    @Test
    void testMarkEventN() {
        Meter meter = mock(Meter.class);

        FlinkMeter wrapper = new FlinkMeter(meter);
        wrapper.markEvent(10L);

        verify(meter).markEvent(10L);
    }
}
