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

package com.alibaba.fluss.server.testutils;

import com.alibaba.fluss.server.metadata.BucketMetadata;
import com.alibaba.fluss.server.metadata.PartitionMetadata;

import org.assertj.core.api.AbstractAssert;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Extend assertj assertions to easily assert {@link PartitionMetadata}. */
public class PartitionMetadataAssert
        extends AbstractAssert<PartitionMetadataAssert, PartitionMetadata> {

    /** Creates assertions for {@link PartitionMetadata}. */
    public static PartitionMetadataAssert assertPartitionMetadata(PartitionMetadata actual) {
        return new PartitionMetadataAssert(actual);
    }

    private PartitionMetadataAssert(PartitionMetadata actual) {
        super(actual, PartitionMetadataAssert.class);
    }

    public PartitionMetadataAssert isEqualTo(PartitionMetadata expected) {
        assertThat(expected.getPartitionName()).isEqualTo(actual.getPartitionName());
        List<BucketMetadata> bucketMetadataList = expected.getBucketMetadataList();
        List<BucketMetadata> actualBucketMetadataList = actual.getBucketMetadataList();
        assertThat(bucketMetadataList)
                .hasSameSizeAs(actualBucketMetadataList)
                .hasSameElementsAs(actualBucketMetadataList);
        return this;
    }
}
