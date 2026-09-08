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

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaTopicMapper}. */
public class KafkaTopicMapperTest {

    @Test
    public void testTopicNameAndIdMapping() {
        KafkaTopicMapper mapper = new KafkaTopicMapper("kafka");

        assertThat(mapper.toTablePath("topic").toString()).isEqualTo("kafka.topic");
        Uuid topicId = mapper.toTopicId(123L);
        assertThat(topicId).isNotIn(Uuid.ZERO_UUID, Uuid.ONE_UUID, Uuid.METADATA_TOPIC_ID);
        assertThat(mapper.isMappedTopicId(topicId)).isTrue();
        assertThat(mapper.toTableId(topicId)).isEqualTo(123L);

        Uuid firstTableTopicId = mapper.toTopicId(0L);
        assertThat(firstTableTopicId).isNotEqualTo(Uuid.ZERO_UUID);
        assertThat(mapper.isMappedTopicId(firstTableTopicId)).isTrue();
        assertThat(mapper.toTableId(firstTableTopicId)).isZero();
    }
}
