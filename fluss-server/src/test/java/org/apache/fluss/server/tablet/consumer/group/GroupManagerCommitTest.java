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

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the consumer-offset commit path. */
class GroupManagerCommitTest {

    @Test
    void testMetadataSizeIsMeasuredInUtf8Bytes() {
        assertThat(GroupManager.isOffsetMetadataSizeValid(null, 0)).isTrue();
        assertThat(GroupManager.isOffsetMetadataSizeValid("abcd", 4)).isTrue();
        assertThat(GroupManager.isOffsetMetadataSizeValid("abcd", 3)).isFalse();

        // A CJK character occupies three bytes in UTF-8 despite having a Java length of one.
        assertThat(GroupManager.isOffsetMetadataSizeValid("中", 3)).isTrue();
        assertThat(GroupManager.isOffsetMetadataSizeValid("中", 2)).isFalse();

        // An emoji occupies four bytes in UTF-8 and two Java UTF-16 code units.
        assertThat(GroupManager.isOffsetMetadataSizeValid("😀", 4)).isTrue();
        assertThat(GroupManager.isOffsetMetadataSizeValid("😀", 3)).isFalse();
    }

    @Test
    void testValidationErrorsArePreservedPerBucket() {
        TableBucket validBucket = new TableBucket(1L, 0);
        TableBucket invalidBucket = new TableBucket(1L, 1);
        Map<TableBucket, ConsumerOffsetAndMetadata> offsets = new LinkedHashMap<>();
        offsets.put(validBucket, new ConsumerOffsetAndMetadata(10L, "valid"));
        offsets.put(invalidBucket, new ConsumerOffsetAndMetadata(20L, "too-large"));

        Map<TableBucket, Errors> validationErrors = new LinkedHashMap<>();
        validationErrors.put(invalidBucket, Errors.CONSUMER_OFFSET_METADATA_TOO_LARGE);

        Map<TableBucket, Errors> result =
                GroupManager.buildCommitResult(offsets, validationErrors, Errors.NONE);

        assertThat(result.keySet()).containsExactly(validBucket, invalidBucket);
        assertThat(result)
                .containsEntry(validBucket, Errors.NONE)
                .containsEntry(invalidBucket, Errors.CONSUMER_OFFSET_METADATA_TOO_LARGE);
    }

    @Test
    void testWriteErrorOnlyAppliesToValidBuckets() {
        TableBucket validBucket = new TableBucket(1L, 0);
        TableBucket invalidBucket = new TableBucket(1L, 1);
        Map<TableBucket, ConsumerOffsetAndMetadata> offsets = new LinkedHashMap<>();
        offsets.put(validBucket, new ConsumerOffsetAndMetadata(10L, "valid"));
        offsets.put(invalidBucket, new ConsumerOffsetAndMetadata(20L, "too-large"));

        Map<TableBucket, Errors> validationErrors = new LinkedHashMap<>();
        validationErrors.put(invalidBucket, Errors.CONSUMER_OFFSET_METADATA_TOO_LARGE);

        Map<TableBucket, Errors> result =
                GroupManager.buildCommitResult(
                        offsets, validationErrors, Errors.NOT_CONSUMER_GROUP_OWNER);

        assertThat(result)
                .containsEntry(validBucket, Errors.NOT_CONSUMER_GROUP_OWNER)
                .containsEntry(invalidBucket, Errors.CONSUMER_OFFSET_METADATA_TOO_LARGE);
    }
}
