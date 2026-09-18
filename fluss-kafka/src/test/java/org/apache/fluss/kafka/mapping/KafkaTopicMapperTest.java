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

package org.apache.fluss.kafka.mapping;

import org.apache.fluss.metadata.TablePath;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaTopicMapper}. */
public class KafkaTopicMapperTest {

    private final KafkaTopicMapper mapper = new KafkaTopicMapper();

    @Test
    public void testQualifiedNamesAcrossDatabases() {
        for (String database : Arrays.asList("sales", "archive", "sales_1-2026")) {
            TablePath tablePath = TablePath.of(database, "orders_1-2026");
            String topicName = database + ".orders_1-2026";
            assertThat(mapper.toTablePath(topicName)).isEqualTo(tablePath);
            assertThat(mapper.toTopicName(tablePath)).isEqualTo(topicName);
            assertThat(mapper.isMappedTable(tablePath)).isTrue();
            assertThat(KafkaTopicMapper.isValidTopic(topicName)).isTrue();
        }
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                "orders",
                ".orders",
                "sales.",
                "sales.orders.extra",
                "sales..orders",
                "sales.bad name",
                " sales.orders",
                "sales.orders ",
                "sales/orders",
                "__system.orders",
                "sales.__internal",
                "数据库.orders",
                "sales.订单"
            })
    public void testRejectsInvalidTopicNames(String topicName) {
        assertThat(KafkaTopicMapper.isValidTopic(topicName)).isFalse();
        assertThatThrownBy(() -> mapper.toTablePath(topicName))
                .isInstanceOf(InvalidTopicException.class);
    }

    @Test
    public void testNameLengthLimits() {
        for (TablePath valid :
                Arrays.asList(
                        TablePath.of(repeat('a', 200), "orders"),
                        TablePath.of("sales", repeat('a', 200)),
                        TablePath.of(repeat('a', 124), repeat('b', 124)))) {
            assertThat(mapper.toTablePath(mapper.toTopicName(valid))).isEqualTo(valid);
        }
        for (TablePath invalid :
                Arrays.asList(
                        TablePath.of(repeat('a', 201), "orders"),
                        TablePath.of("sales", repeat('a', 201)),
                        TablePath.of(repeat('a', 124), repeat('b', 125)))) {
            assertThat(mapper.isMappedTable(invalid)).isFalse();
            assertThatThrownBy(() -> mapper.toTablePath(invalid.toString()))
                    .isInstanceOf(InvalidTopicException.class);
            assertThatThrownBy(() -> mapper.toTopicName(invalid))
                    .isInstanceOf(InvalidTopicException.class);
        }
    }

    @Test
    public void testRejectsUnrepresentableTablePaths() {
        for (TablePath invalid :
                Arrays.asList(
                        null,
                        TablePath.of(null, "orders"),
                        TablePath.of("sales", null),
                        TablePath.of("", "orders"),
                        TablePath.of("sales", ""),
                        TablePath.of("sales.region", "orders"),
                        TablePath.of("sales", "orders.v1"),
                        TablePath.of("__system", "orders"),
                        TablePath.of("sales", "__internal"),
                        TablePath.of("sales", "bad name"))) {
            assertThat(mapper.isMappedTable(invalid)).isFalse();
            assertThatThrownBy(() -> mapper.toTopicName(invalid))
                    .isInstanceOf(InvalidTopicException.class);
        }
    }

    @Test
    public void testTopicIdMapping() {
        Uuid topicId = mapper.toTopicId(123L);
        assertThat(topicId).isNotIn(Uuid.ZERO_UUID, Uuid.ONE_UUID, Uuid.METADATA_TOPIC_ID);
        assertThat(mapper.isMappedTopicId(topicId)).isTrue();
        assertThat(mapper.toTableId(topicId)).isEqualTo(123L);

        Uuid firstTableTopicId = mapper.toTopicId(0L);
        assertThat(firstTableTopicId).isNotEqualTo(Uuid.ZERO_UUID);
        assertThat(mapper.isMappedTopicId(firstTableTopicId)).isTrue();
        assertThat(mapper.toTableId(firstTableTopicId)).isZero();
    }

    @Test
    public void testRejectsInvalidTopicIds() {
        assertThatThrownBy(() -> mapper.toTopicId(-1L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(mapper.isMappedTopicId(Uuid.ZERO_UUID)).isFalse();
        assertThatThrownBy(() -> mapper.toTableId(Uuid.ZERO_UUID))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static String repeat(char character, int count) {
        return String.join("", Collections.nCopies(count, String.valueOf(character)));
    }
}
