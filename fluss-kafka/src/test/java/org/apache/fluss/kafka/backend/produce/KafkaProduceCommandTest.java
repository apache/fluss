/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies borrowed payloads preserve command ownership and defensive copy accessors. */
class KafkaProduceCommandTest {

    @Test
    void testBorrowedPayloadsReuseOwnedArraysWhileCopyAccessorsRemainDefensive() {
        byte[] key = {1};
        byte[] value = {2};
        byte[] headerValue = {3};
        RecordHeader header = new RecordHeader("header", headerValue);
        List<RecordHeader> headers = new ArrayList<>();
        headers.add(header);
        Record record = new Record(1L, key, value, headers);

        assertThat(record.borrowedKey()).isNotSameAs(key);
        assertThat(record.borrowedValue()).isNotSameAs(value);
        assertThat(header.borrowedValue()).isNotSameAs(headerValue);
        key[0] = 10;
        value[0] = 20;
        headerValue[0] = 30;
        headers.clear();

        assertThat(record.borrowedKey()).containsExactly((byte) 1);
        assertThat(record.borrowedValue()).containsExactly((byte) 2);
        assertThat(header.borrowedValue()).containsExactly((byte) 3);
        assertThat(record.headers()).containsExactly(header);
        assertThat(record.borrowedKey()).isSameAs(record.borrowedKey());
        assertThat(record.borrowedValue()).isSameAs(record.borrowedValue());
        assertThat(header.borrowedValue()).isSameAs(header.borrowedValue());

        record.key()[0] = 40;
        record.value()[0] = 50;
        header.value()[0] = 60;
        assertThat(record.key()).containsExactly((byte) 1).isNotSameAs(record.borrowedKey());
        assertThat(record.value()).containsExactly((byte) 2).isNotSameAs(record.borrowedValue());
        assertThat(header.value()).containsExactly((byte) 3).isNotSameAs(header.borrowedValue());
    }

    @Test
    void testBorrowedPayloadsPreserveNullAndEmptyBytes() {
        Record nullRecord = new Record(1L, null, null, Collections.emptyList());
        assertThat(nullRecord.borrowedKey()).isNull();
        assertThat(nullRecord.borrowedValue()).isNull();
        assertThat(new RecordHeader("header", null).borrowedValue()).isNull();

        Record emptyRecord = new Record(1L, new byte[0], new byte[0], Collections.emptyList());
        assertThat(emptyRecord.borrowedKey()).isEmpty();
        assertThat(emptyRecord.borrowedValue()).isEmpty();
        assertThat(new RecordHeader("header", new byte[0]).borrowedValue()).isEmpty();
    }
}
