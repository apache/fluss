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

import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.util.TestCounter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class FlinkCounterTest {

    @Test
    void testWrapperIncDec() {
        Counter counter = new TestCounter();
        counter.inc();

        FlinkCounter wrapper = new FlinkCounter(counter);
        assertThat(wrapper.getCount()).isEqualTo(1L);
        wrapper.dec();
        assertThat(wrapper.getCount()).isEqualTo(0L);
        wrapper.inc();
        wrapper.inc(1);
        assertThat(wrapper.getCount()).isEqualTo(2L);
        wrapper.dec(2);
        assertThat(wrapper.getCount()).isEqualTo(0L);
    }
}
