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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkRowDataChannelComputer}. */
class FlinkRowDataChannelComputerTest {

    private static final FlussSerializationSchema<RowData> serializationSchema =
            new RowDataSerializationSchema(false, false);

    @BeforeAll
    static void init() throws Exception {
        serializationSchema.open(new SerializerInitContextImpl(DATA1_ROW_TYPE, false));
    }

    @Test
    void testSelectChanel() {

        FlinkRowDataChannelComputer<RowData> channelComputer =
                new FlinkRowDataChannelComputer<>(
                        DATA1_ROW_TYPE,
                        Collections.singletonList("a"),
                        Collections.emptyList(),
                        null,
                        10,
                        serializationSchema);

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
        FlinkRowDataChannelComputer<RowData> channelComputer =
                new FlinkRowDataChannelComputer<>(
                        DATA1_ROW_TYPE,
                        Collections.singletonList("a"),
                        Collections.singletonList("b"),
                        null,
                        10,
                        serializationSchema);

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

    @Test
    void testSelectChannelForImplicitPartitionedTable() throws Exception {
        RowType rowType = implicitPartitionedRowType();
        FlinkRowDataChannelComputer<RowData> channelComputer =
                new FlinkRowDataChannelComputer<>(
                        rowType,
                        Collections.singletonList("id"),
                        Collections.singletonList("event_month"),
                        implicitPartitionExpressions(),
                        null,
                        10,
                        new RowDataSerializationSchema(false, false));
        channelComputer.setup(3);
        assertThat(channelComputer.isCombineShuffleWithPartitionName()).isTrue();

        GenericRowData marchRow = rowData(7, LocalDateTime.of(2024, 3, 15, 10, 30), "march");
        int bucket = bucket(rowType, marchRow);
        assertThat(channelComputer.channel(marchRow))
                .isEqualTo(ChannelComputer.select("202403", bucket, 3));

        GenericRowData samePartitionRow =
                rowData(7, LocalDateTime.of(2024, 3, 31, 23, 59), "other");
        assertThat(channelComputer.channel(samePartitionRow))
                .isEqualTo(ChannelComputer.select("202403", bucket, 3));

        GenericRowData aprilRow = rowData(7, LocalDateTime.of(2024, 4, 1, 0, 0), "april");
        assertThat(channelComputer.channel(aprilRow))
                .isEqualTo(ChannelComputer.select("202404", bucket, 3));
    }

    private static RowType implicitPartitionedRowType() {
        return RowType.of(
                new DataType[] {DataTypes.INT(), DataTypes.TIMESTAMP(), DataTypes.STRING()},
                new String[] {"id", "event_time", "payload"});
    }

    private static List<PartitionExpression> implicitPartitionExpressions() {
        return Collections.singletonList(
                PartitionExpression.of(
                        "event_month",
                        DateTruncPartitionTransform.of("event_time", AutoPartitionTimeUnit.MONTH)));
    }

    private static GenericRowData rowData(int id, LocalDateTime eventTime, String payload) {
        return GenericRowData.of(
                id, TimestampData.fromLocalDateTime(eventTime), StringData.fromString(payload));
    }

    private static int bucket(RowType rowType, RowData rowData) {
        KeyEncoder keyEncoder =
                KeyEncoder.ofBucketKeyEncoder(rowType, Collections.singletonList("id"), null);
        return BucketingFunction.of(null)
                .bucketing(keyEncoder.encodeKey(new FlinkAsFlussRow(rowData)), 10);
    }
}
