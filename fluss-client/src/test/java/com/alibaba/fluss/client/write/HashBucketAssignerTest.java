/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.bucketing.BucketingFunction;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.CompactedKeyEncoder;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HashBucketAssigner}. */
class HashBucketAssignerTest {

    @Test
    void testBucketAssign() {
        final RowType rowType =
                DataTypes.ROW(
                        new DataField("a", DataTypes.INT()),
                        new DataField("b", DataTypes.INT()),
                        new DataField("c", DataTypes.STRING()),
                        new DataField("d", DataTypes.BIGINT()));

        // Suppose a, b are primary keys.
        int[] pkIndices = {0, 1};
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(rowType, pkIndices);
        InternalRow row1 = row(1, 1, "2", 3L);
        InternalRow row2 = row(1, 1, "3", 4L);
        InternalRow row3 = row(1, 2, "4", 5L);
        InternalRow row4 = row(1, 1, "4", 5L);

        HashBucketAssigner hashBucketAssigner = new HashBucketAssigner(3);

        int bucket1 = hashBucketAssigner.assignBucket(keyEncoder.encodeKey(row1));
        int bucket2 = hashBucketAssigner.assignBucket(keyEncoder.encodeKey(row2));
        int bucket3 = hashBucketAssigner.assignBucket(keyEncoder.encodeKey(row3));
        int bucket4 = hashBucketAssigner.assignBucket(keyEncoder.encodeKey(row4));

        assertThat(bucket1).isEqualTo(bucket2);
        assertThat(bucket1).isNotEqualTo(bucket3);
        assertThat(bucket3).isNotEqualTo(bucket4);
        assertThat(bucket1 < 3).isTrue();
        assertThat(bucket2 < 3).isTrue();
        assertThat(bucket3 < 3).isTrue();
        assertThat(bucket4 < 3).isTrue();
    }

    @Test
    void testBucketForRowKey() {
        final RowType rowType =
                DataTypes.ROW(
                        new DataField("a", DataTypes.INT()),
                        new DataField("b", DataTypes.INT()),
                        new DataField("c", DataTypes.STRING()),
                        new DataField("d", DataTypes.BIGINT()));

        List<byte[]> keyList = new ArrayList<>();
        int rowCount = 3000;
        int[] pkIndices = {0, 1, 2};
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(rowType, pkIndices);
        for (int i = 0; i < rowCount; i++) {
            InternalRow row = row(i, rowCount - i, String.valueOf(rowCount - i), (long) i);
            keyList.add(keyEncoder.encodeKey(row));
        }

        for (int bucketNumber = 3; bucketNumber < 10; bucketNumber++) {
            HashBucketAssigner hashBucketAssigner = new HashBucketAssigner(bucketNumber);
            for (byte[] key : keyList) {
                int bucket = hashBucketAssigner.assignBucket(key);
                assertThat(bucket >= 0).isTrue();
                assertThat(bucket < bucketNumber).isTrue();
            }
        }
    }

    static Stream<Arguments> lakeParameters() {
        return Stream.of(
                Arguments.of(true, true),
                Arguments.of(true, false),
                Arguments.of(false, true),
                Arguments.of(false, false));
    }

    @ParameterizedTest
    @MethodSource("lakeParameters")
    void testLakeBucketAssign(boolean isPartitioned, boolean isLogTable) {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING());
        Schema schema =
                isLogTable ? schemaBuilder.build() : schemaBuilder.primaryKey("a", "c").build();

        // bucket key
        List<String> bucketKey =
                isPartitioned ? Collections.singletonList("a") : Arrays.asList("a", "c");

        InternalRow row1 = row(1, "2", "a");
        InternalRow row2 = row(1, "3", "b");
        InternalRow row3 = row(2, "4", "a");
        InternalRow row4 = row(2, "4", "b");
        KeyEncoder keyEncoder =
                KeyEncoder.of(schema.getRowType(), bucketKey, DataLakeFormat.PAIMON);
        HashBucketAssigner bucketAssigner =
                new HashBucketAssigner(3, BucketingFunction.of(DataLakeFormat.PAIMON));

        int row1Bucket = bucketAssigner.assignBucket(keyEncoder.encodeKey(row1));
        int row2Bucket = bucketAssigner.assignBucket(keyEncoder.encodeKey(row2));
        int row3Bucket = bucketAssigner.assignBucket(keyEncoder.encodeKey(row3));
        int row4Bucket = bucketAssigner.assignBucket(keyEncoder.encodeKey(row4));

        if (isPartitioned) {
            // bucket key is the column 'a'
            assertThat(row1Bucket).isEqualTo(row2Bucket);
            assertThat(row3Bucket).isEqualTo(row4Bucket);
            assertThat(row1Bucket).isNotEqualTo(row3Bucket);
        } else {
            // bucket key is the column 'a', 'c'
            assertThat(row1Bucket).isNotEqualTo(row2Bucket);
            assertThat(row3Bucket).isNotEqualTo(row4Bucket);
        }
    }
}
