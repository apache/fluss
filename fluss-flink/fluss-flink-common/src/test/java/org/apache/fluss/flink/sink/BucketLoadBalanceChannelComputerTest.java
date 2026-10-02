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

package org.apache.fluss.flink.sink;

import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BucketLoadBalanceChannelComputer}. */
class BucketLoadBalanceChannelComputerTest {

    private static final FlussSerializationSchema<RowData> serializationSchema =
            new RowDataSerializationSchema(false, false);

    @BeforeAll
    static void init() throws Exception {
        serializationSchema.open(new SerializerInitContextImpl(DATA1_ROW_TYPE, false));
    }

    @Test
    void testDeterministicRouting() {
        int numBucket = 10;
        BucketLoadBalanceChannelComputer<RowData> channelComputer =
                new BucketLoadBalanceChannelComputer<>(
                        DATA1_ROW_TYPE,
                        Collections.singletonList("a"),
                        null,
                        numBucket,
                        serializationSchema);

        // Same bucket key always routes to the same channel
        for (int numChannel = 1; numChannel <= 10; numChannel++) {
            channelComputer.setup(numChannel);
            for (int i = 0; i < 100; i++) {
                int expectedChannel = -1;
                for (int retry = 0; retry < 5; retry++) {
                    GenericRowData row = GenericRowData.of(i, StringData.fromString("a1"));
                    int channel = channelComputer.channel(row);
                    if (expectedChannel < 0) {
                        expectedChannel = channel;
                    } else {
                        assertThat(channel)
                                .as(
                                        "Same key should route to same channel for numChannels=%d, key=%d",
                                        numChannel, i)
                                .isEqualTo(expectedChannel);
                    }
                    assertThat(channel).isLessThan(numChannel);
                }
            }
        }
    }

    @Test
    void testAllChannelsReceiveData() {
        int numBucket = 3;
        int numChannels = 7;
        BucketLoadBalanceChannelComputer<RowData> channelComputer =
                new BucketLoadBalanceChannelComputer<>(
                        DATA1_ROW_TYPE,
                        Collections.singletonList("a"),
                        null,
                        numBucket,
                        serializationSchema);
        channelComputer.setup(numChannels);

        Set<Integer> usedChannels = new HashSet<>();
        // Generate many different bucket key values to cover all channels
        for (int i = 0; i < 500; i++) {
            GenericRowData row = GenericRowData.of(i, StringData.fromString("val" + i));
            int channel = channelComputer.channel(row);
            usedChannels.add(channel);
        }

        // Unlike BUCKET mode where some channels may be idle when numBucket < numChannels,
        // BUCKET_LOAD_BALANCE should use all channels
        assertThat(usedChannels).hasSize(numChannels);
    }

    @Test
    void testEvenDistribution() {
        int numBucket = 5;
        int numChannels = 8;
        BucketLoadBalanceChannelComputer<RowData> channelComputer =
                new BucketLoadBalanceChannelComputer<>(
                        DATA1_ROW_TYPE,
                        Collections.singletonList("a"),
                        null,
                        numBucket,
                        serializationSchema);
        channelComputer.setup(numChannels);

        Map<Integer, Integer> channelCounts = new HashMap<>();
        int totalRecords = 10000;
        for (int i = 0; i < totalRecords; i++) {
            GenericRowData row = GenericRowData.of(i, StringData.fromString("key" + i));
            int channel = channelComputer.channel(row);
            channelCounts.merge(channel, 1, Integer::sum);
        }

        // Each channel should have roughly totalRecords / numChannels records
        double expectedPerChannel = (double) totalRecords / numChannels;
        for (int c = 0; c < numChannels; c++) {
            int count = channelCounts.getOrDefault(c, 0);
            // Allow 20% deviation from expected
            assertThat((double) count)
                    .as("Channel %d has %d records, expected ~%.0f", c, count, expectedPerChannel)
                    .isBetween(expectedPerChannel * 0.8, expectedPerChannel * 1.2);
        }
    }

    @Test
    void testExactDivisibleCase() {
        int numBucket = 6;
        int numChannels = 3;
        BucketLoadBalanceChannelComputer<RowData> channelComputer =
                new BucketLoadBalanceChannelComputer<>(
                        DATA1_ROW_TYPE,
                        Collections.singletonList("a"),
                        null,
                        numBucket,
                        serializationSchema);
        channelComputer.setup(numChannels);

        // When numBucket % numChannels == 0, should behave like BUCKET mode
        // (each channel gets numBucket / numChannels buckets)
        for (int i = 0; i < 100; i++) {
            GenericRowData row = GenericRowData.of(i, StringData.fromString("v" + i));
            int channel = channelComputer.channel(row);
            assertThat(channel).isBetween(0, numChannels - 1);
        }
    }

    @Test
    void testMoreChannelsThanBuckets() {
        int numBucket = 3;
        int numChannels = 6;
        BucketLoadBalanceChannelComputer<RowData> channelComputer =
                new BucketLoadBalanceChannelComputer<>(
                        DATA1_ROW_TYPE,
                        Collections.singletonList("a"),
                        null,
                        numBucket,
                        serializationSchema);
        channelComputer.setup(numChannels);

        Set<Integer> usedChannels = new HashSet<>();
        for (int i = 0; i < 500; i++) {
            GenericRowData row = GenericRowData.of(i, StringData.fromString("k" + i));
            int channel = channelComputer.channel(row);
            usedChannels.add(channel);
        }

        assertThat(usedChannels).hasSize(numChannels);
    }
}
