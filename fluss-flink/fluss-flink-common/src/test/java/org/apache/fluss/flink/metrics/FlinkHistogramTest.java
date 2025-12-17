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

package org.apache.fluss.flink.metrics;

import org.apache.fluss.metrics.Histogram;

import org.apache.flink.metrics.HistogramStatistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkHistogram}. */
class FlinkHistogramTest {

    private FlinkHistogram flinkHistogram;
    private TestFlussHistogram testFlussHistogram;

    @BeforeEach
    void setUp() {
        testFlussHistogram = new TestFlussHistogram();
        flinkHistogram = new FlinkHistogram(testFlussHistogram);
    }

    @Test
    void testUpdate() {
        flinkHistogram.update(100L);
        assertThat(testFlussHistogram.getUpdateCount()).isEqualTo(1);
        assertThat(testFlussHistogram.getLastUpdateValue()).isEqualTo(100L);
    }

    @Test
    void testGetCount() {
        testFlussHistogram.setCount(5L);
        assertThat(flinkHistogram.getCount()).isEqualTo(5L);
    }

    @Test
    void testGetStatisticsReturnsNonNull() {
        HistogramStatistics statistics = flinkHistogram.getStatistics();
        assertThat(statistics).isNotNull();
    }

    @Test
    void testGetStatisticsGetMin() {
        testFlussHistogram.setMin(10L);
        HistogramStatistics statistics = flinkHistogram.getStatistics();
        assertThat(statistics.getMin()).isEqualTo(10L);
    }

    @Test
    void testGetStatisticsGetMax() {
        testFlussHistogram.setMax(100L);
        HistogramStatistics statistics = flinkHistogram.getStatistics();
        assertThat(statistics.getMax()).isEqualTo(100L);
    }

    @Test
    void testGetStatisticsGetMean() {
        testFlussHistogram.setMean(50.5);
        HistogramStatistics statistics = flinkHistogram.getStatistics();
        assertThat(statistics.getMean()).isEqualTo(50.5);
    }

    @Test
    void testGetStatisticsGetStdDev() {
        testFlussHistogram.setStdDev(15.2);
        HistogramStatistics statistics = flinkHistogram.getStatistics();
        assertThat(statistics.getStdDev()).isEqualTo(15.2);
    }

    @Test
    void testGetStatisticsGetQuantile() {
        testFlussHistogram.setQuantile(0.5, 25.0);
        HistogramStatistics statistics = flinkHistogram.getStatistics();
        assertThat(statistics.getQuantile(0.5)).isEqualTo(25.0);
    }

    @Test
    void testGetStatisticsGetValues() {
        long[] expectedValues = {1L, 2L, 3L};
        testFlussHistogram.setValues(expectedValues);
        HistogramStatistics statistics = flinkHistogram.getStatistics();
        assertThat(statistics.getValues()).isEqualTo(expectedValues);
    }

    @Test
    void testGetStatisticsSize() {
        testFlussHistogram.setSize(42);
        HistogramStatistics statistics = flinkHistogram.getStatistics();
        assertThat(statistics.size()).isEqualTo(42);
    }

    /** Test implementation of Fluss Histogram for unit testing. */
    private static class TestFlussHistogram implements Histogram {
        private long count = 0;
        private long lastUpdateValue = 0;
        private int updateCount = 0;
        private long min = 0;
        private long max = 0;
        private double mean = 0.0;
        private double stdDev = 0.0;
        private long[] values = new long[0];
        private int size = 0;
        private final Map<Double, Double> quantiles = new HashMap<>();

        @Override
        public void update(long value) {
            lastUpdateValue = value;
            updateCount++;
        }

        @Override
        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public org.apache.fluss.metrics.HistogramStatistics getStatistics() {
            return new org.apache.fluss.metrics.HistogramStatistics() {
                @Override
                public double getQuantile(double quantile) {
                    return quantiles.getOrDefault(quantile, quantile);
                }

                @Override
                public long[] getValues() {
                    return values;
                }

                @Override
                public int size() {
                    return size;
                }

                @Override
                public double getMean() {
                    return mean;
                }

                @Override
                public double getStdDev() {
                    return stdDev;
                }

                @Override
                public long getMax() {
                    return max;
                }

                @Override
                public long getMin() {
                    return min;
                }
            };
        }

        public long getLastUpdateValue() {
            return lastUpdateValue;
        }

        public int getUpdateCount() {
            return updateCount;
        }

        public void setMin(long min) {
            this.min = min;
        }

        public void setMax(long max) {
            this.max = max;
        }

        public void setMean(double mean) {
            this.mean = mean;
        }

        public void setStdDev(double stdDev) {
            this.stdDev = stdDev;
        }

        public void setValues(long[] values) {
            this.values = values;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public void setQuantile(double quantile, double value) {
            this.quantiles.put(quantile, value);
        }
    }
}
