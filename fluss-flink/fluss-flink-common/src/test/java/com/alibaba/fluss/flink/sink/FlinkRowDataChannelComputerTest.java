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

package com.alibaba.fluss.flink.sink;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkRowDataChannelComputer}. */
class FlinkRowDataChannelComputerTest {

    @Test
    void testSelectChanel() {
        FlinkRowDataChannelComputer channelComputer =
                new FlinkRowDataChannelComputer(
                        DATA1_ROW_TYPE,
                        Collections.singletonList("a"),
                        Collections.emptyList(),
                        null,
                        10);

        for (int numChannel = 1; numChannel <= 10; numChannel++) {
            channelComputer.setup(numChannel);
            assertThat(channelComputer.isCombineShuffleWithPartitionName()).isFalse();
            for (int i = 0; i < 100; i++) {
                int expectedChannel = -1;
                for (int retry = 0; retry < 5; retry++) {
                    GenericRowData row = GenericRowData.of(i, StringData.fromString("a1"));
                    int channel = channelComputer.channel(row);
                    if (expectedChannel < 0) {
                        expectedChannel = channel;
                    } else {
                        assertThat(channel).isEqualTo(expectedChannel);
                        assertThat(channel).isLessThan(numChannel);
                    }
                }
            }
        }
    }

    @Test
    void testSelectChanelForPartitionedTable() {
        FlinkRowDataChannelComputer channelComputer =
                new FlinkRowDataChannelComputer(
                        DATA1_ROW_TYPE,
                        Collections.singletonList("a"),
                        Collections.singletonList("b"),
                        null,
                        10);

        for (int numChannel = 1; numChannel <= 10; numChannel++) {
            channelComputer.setup(numChannel);
            if (10 % numChannel != 0) {
                assertThat(channelComputer.isCombineShuffleWithPartitionName()).isTrue();
            } else {
                assertThat(channelComputer.isCombineShuffleWithPartitionName()).isFalse();
            }
            for (int i = 0; i < 100; i++) {
                int expectedChannel = -1;
                for (int retry = 0; retry < 5; retry++) {
                    GenericRowData row = GenericRowData.of(i, StringData.fromString("a1"));
                    int channel = channelComputer.channel(row);
                    if (expectedChannel < 0) {
                        expectedChannel = channel;
                    } else {
                        assertThat(channel).isEqualTo(expectedChannel);
                        assertThat(channel).isLessThan(numChannel);
                    }
                }
            }
        }

        // numChannels is divisible by 10
        channelComputer.setup(5);
        GenericRowData row1 = GenericRowData.of(0, StringData.fromString("hello"));
        GenericRowData row2 = GenericRowData.of(0, StringData.fromString("no"));
        assertThat(channelComputer.channel(row1)).isEqualTo(channelComputer.channel(row2));

        // numChannels is not divisible by 10
        channelComputer.setup(3);
        row1 = GenericRowData.of(0, StringData.fromString("hello"));
        row2 = GenericRowData.of(0, StringData.fromString("no"));
        assertThat(channelComputer.channel(row1)).isNotEqualTo(channelComputer.channel(row2));
    }
}
