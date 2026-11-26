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

import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricView;
import org.apache.fluss.metrics.SimpleCounter;

import org.apache.flink.metrics.View;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkMeter}. */
class FlinkMeterTest {

    @Test
    void testFlinkViewUpdateComputesTheRateOfAFlussMeterView() {
        int timeSpanInSeconds = MetricView.UPDATE_INTERVAL_SECONDS;
        FlinkMeter flinkMeter =
                new FlinkMeter(new MeterView(new SimpleCounter(), timeSpanInSeconds));

        flinkMeter.markEvent(10);
        assertThat(flinkMeter.getCount()).isEqualTo(10);
        assertThat(flinkMeter.getRate()).isZero();

        // Flink's registry calls update() on every View it registers.
        ((View) flinkMeter).update();

        assertThat(flinkMeter.getRate()).isEqualTo(10.0 / timeSpanInSeconds);
    }

    @Test
    void testFlinkAndFlussUpdateOnTheSameInterval() {
        assertThat(View.UPDATE_INTERVAL_SECONDS).isEqualTo(MetricView.UPDATE_INTERVAL_SECONDS);
    }
}
