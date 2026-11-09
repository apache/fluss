/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.exception.ConsumerGroupIdNotFoundException;
import org.apache.fluss.exception.ConsumerGroupOwnerLoadingException;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.exception.ConsumerOffsetMetadataTooLargeException;
import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.exception.NotConsumerGroupOwnerException;
import org.apache.fluss.exception.RetriableException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for consumer-group error mappings. */
class ConsumerGroupErrorsTest {

    @Test
    void testErrorCodes() {
        assertThat(Errors.INVALID_CONSUMER_GROUP_ID.code()).isEqualTo(75);
        assertThat(Errors.CONSUMER_GROUP_OWNER_LOADING.code()).isEqualTo(76);
        assertThat(Errors.CONSUMER_GROUP_OWNER_NOT_AVAILABLE.code()).isEqualTo(77);
        assertThat(Errors.NOT_CONSUMER_GROUP_OWNER.code()).isEqualTo(78);
        assertThat(Errors.CONSUMER_GROUP_ID_NOT_FOUND.code()).isEqualTo(79);
        assertThat(Errors.CONSUMER_OFFSET_METADATA_TOO_LARGE.code()).isEqualTo(80);
    }

    @Test
    void testExceptionMappings() {
        assertThat(Errors.forException(new InvalidConsumerGroupIdException("invalid")))
                .isEqualTo(Errors.INVALID_CONSUMER_GROUP_ID);
        assertThat(Errors.forException(new ConsumerGroupOwnerLoadingException("loading")))
                .isEqualTo(Errors.CONSUMER_GROUP_OWNER_LOADING);
        assertThat(Errors.forException(new ConsumerGroupOwnerNotAvailableException("unavailable")))
                .isEqualTo(Errors.CONSUMER_GROUP_OWNER_NOT_AVAILABLE);
        assertThat(Errors.forException(new NotConsumerGroupOwnerException("not owner")))
                .isEqualTo(Errors.NOT_CONSUMER_GROUP_OWNER);
        assertThat(Errors.forException(new ConsumerGroupIdNotFoundException("missing")))
                .isEqualTo(Errors.CONSUMER_GROUP_ID_NOT_FOUND);
        assertThat(Errors.forException(new ConsumerOffsetMetadataTooLargeException("large")))
                .isEqualTo(Errors.CONSUMER_OFFSET_METADATA_TOO_LARGE);
    }

    @Test
    void testOwnerTransitionErrorsAreRetriable() {
        assertThat(Errors.CONSUMER_GROUP_OWNER_LOADING.exception())
                .isInstanceOf(RetriableException.class);
        assertThat(Errors.CONSUMER_GROUP_OWNER_NOT_AVAILABLE.exception())
                .isInstanceOf(RetriableException.class);
        assertThat(Errors.NOT_CONSUMER_GROUP_OWNER.exception())
                .isInstanceOf(RetriableException.class);
    }
}
