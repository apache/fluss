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

import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.metrics.util.TestHistogram;

import org.apache.flink.metrics.HistogramStatistics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FlinkHistogramTest {

    @Test
    void testHistogramWrapper() {
        int size = 3;
        TestHistogram testHistogram = new TestHistogram();
        FlinkHistogram histogram = new FlinkHistogram(testHistogram);
        HistogramStatistics statistics = histogram.getStatistics();
        assertThat(histogram.getCount()).isEqualTo(1);
        assertThat(statistics.size()).isEqualTo(statistics.size());
        assertThat(statistics.getMax()).isEqualTo(6);
        assertThat(statistics.getMin()).isEqualTo(7);
        assertThat(statistics.getMean()).isEqualTo(4);
        assertThat(statistics.getStdDev()).isEqualTo(5);
        assertThat(statistics.getValues()).isEqualTo(new long[0]);

        statistics = histogram.getStatistics();
        assertThat(statistics.size()).isEqualTo(size);

        assertThat(statistics.getQuantile(0.5d)).isEqualTo(0.5d);
    }

    @Test
    void testUpdate() {
        Histogram histogram = mock(Histogram.class);

        FlinkHistogram wrapper = new FlinkHistogram(histogram);
        wrapper.update(10L);

        verify(histogram).update(10L);
    }
}
