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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TimestampType;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests timestamp provenance and ownership of protocol-independent Produce commands. */
class KafkaProduceCommandTest {

    @ParameterizedTest
    @ValueSource(longs = {-1L, 0L, 123L})
    void testLegacyConstructorDoesNotInferBatchType(long timestamp) {
        Record record = new Record(timestamp, null, null, Collections.emptyList());

        assertThat(record.timestamp()).isEqualTo(timestamp);
        assertThat(record.hasTimestamp()).isEqualTo(timestamp != Record.NO_TIMESTAMP);
        assertThat(record.timestampType()).isEqualTo(TimestampType.UNKNOWN);
        assertThat(record.key()).isNull();
        assertThat(record.value()).isNull();
    }

    @ParameterizedTest
    @EnumSource(TimestampType.class)
    void testTimestampPresenceDoesNotChangeExplicitBatchType(TimestampType timestampType) {
        for (long timestamp : new long[] {Record.NO_TIMESTAMP, 0L, 123L}) {
            Record record =
                    new Record(timestamp, timestampType, null, null, Collections.emptyList());

            assertThat(record.timestampType()).isEqualTo(timestampType);
            assertThat(record.timestamp()).isEqualTo(timestamp);
            assertThat(record.hasTimestamp()).isEqualTo(timestamp != Record.NO_TIMESTAMP);
        }
    }

    @Test
    void testExplicitBatchTypeMustNotBeNull() {
        assertThatThrownBy(() -> new Record(123L, null, null, null, Collections.emptyList()))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testBothConstructorsOwnTheirRecordData(boolean explicitTimestampType) {
        byte[] key = {1};
        byte[] value = {2};
        byte[] headerValue = {3};
        List<RecordHeader> headers =
                new ArrayList<>(
                        Arrays.asList(
                                new RecordHeader("header", headerValue),
                                new RecordHeader("header", null)));
        Record record =
                explicitTimestampType
                        ? new Record(123L, TimestampType.LOG_APPEND_TIME, key, value, headers)
                        : new Record(123L, key, value, headers);
        key[0] = 4;
        value[0] = 5;
        headerValue[0] = 6;
        headers.clear();
        record.key()[0] = 7;
        record.value()[0] = 8;
        record.headers().get(0).value()[0] = 9;

        assertThat(record.key()).containsExactly((byte) 1);
        assertThat(record.value()).containsExactly((byte) 2);
        assertThat(record.headers())
                .extracting(RecordHeader::name)
                .containsExactly("header", "header");
        assertThat(record.headers().get(0).value()).containsExactly((byte) 3);
        assertThat(record.headers().get(1).value()).isNull();
        assertThat(record.timestamp()).isEqualTo(123L);
        assertThat(record.timestampType())
                .isEqualTo(
                        explicitTimestampType
                                ? TimestampType.LOG_APPEND_TIME
                                : TimestampType.UNKNOWN);
        assertThatThrownBy(() -> record.headers().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testCommandPreservesMixedTimestampTypesAndOwnsItsLists() {
        List<Record> records =
                new ArrayList<>(
                        Arrays.asList(
                                new Record(
                                        123L,
                                        TimestampType.CREATE_TIME,
                                        null,
                                        null,
                                        Collections.emptyList()),
                                new Record(
                                        123L,
                                        TimestampType.LOG_APPEND_TIME,
                                        null,
                                        null,
                                        Collections.emptyList()),
                                new Record(
                                        Record.NO_TIMESTAMP, null, null, Collections.emptyList())));
        List<PartitionWrite> partitions =
                new ArrayList<>(Collections.singletonList(new PartitionWrite(2, records)));
        List<TopicWrite> topics =
                new ArrayList<>(Collections.singletonList(new TopicWrite("db.topic", partitions)));
        KafkaProduceCommand command =
                new KafkaProduceCommand((short) -1, 1000, topics, "KAFKA", null);
        records.clear();
        partitions.clear();
        topics.clear();

        assertThat(command.acks()).isEqualTo((short) -1);
        assertThat(command.timeoutMs()).isEqualTo(1000);
        assertThat(command.topics()).extracting(TopicWrite::topicName).containsExactly("db.topic");
        assertThat(command.topics().get(0).partitions())
                .extracting(PartitionWrite::partitionId)
                .containsExactly(2);
        List<Record> copied = command.topics().get(0).partitions().get(0).records();
        assertThat(copied)
                .extracting(Record::timestampType)
                .containsExactly(
                        TimestampType.CREATE_TIME,
                        TimestampType.LOG_APPEND_TIME,
                        TimestampType.UNKNOWN);
        assertThat(copied)
                .extracting(Record::timestamp)
                .containsExactly(123L, 123L, Record.NO_TIMESTAMP);
        assertThatThrownBy(() -> command.topics().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> command.topics().get(0).partitions().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(copied::clear).isInstanceOf(UnsupportedOperationException.class);
    }
}
