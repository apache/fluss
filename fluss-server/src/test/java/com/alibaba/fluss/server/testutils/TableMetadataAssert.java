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

package com.alibaba.fluss.server.testutils;

import com.alibaba.fluss.server.metadata.BucketMetadata;
import com.alibaba.fluss.server.metadata.TableMetadata;

import org.assertj.core.api.AbstractAssert;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Extend assertj assertions to easily assert {@link TableMetadata}. */
public class TableMetadataAssert extends AbstractAssert<TableMetadataAssert, TableMetadata> {
    /** Creates assertions for {@link TableMetadata}. */
    public static TableMetadataAssert assertTableMetadata(TableMetadata actual) {
        return new TableMetadataAssert(actual);
    }

    private TableMetadataAssert(TableMetadata actual) {
        super(actual, TableMetadataAssert.class);
    }

    public TableMetadataAssert isEqualTo(TableMetadata expected) {
        assertThat(expected.getTableInfo()).isEqualTo(actual.getTableInfo());
        List<BucketMetadata> bucketMetadataList = expected.getBucketMetadataList();
        List<BucketMetadata> actualBucketMetadataList = actual.getBucketMetadataList();
        assertThat(bucketMetadataList)
                .hasSameSizeAs(actualBucketMetadataList)
                .hasSameElementsAs(actualBucketMetadataList);
        return this;
    }
}
