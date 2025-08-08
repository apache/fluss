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

package com.alibaba.fluss.flink.tiering.source;

import com.alibaba.fluss.flink.tiering.TestingWriteResult;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableBucketWriteResultSerializer}. */
class TableBucketWriteResultSerializerTest {

    private static final TableBucketWriteResultSerializer<TestingWriteResult>
            tableBucketWriteResultSerializer =
                    new TableBucketWriteResultSerializer<>(new TestingWriteResultSerializer());

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSerializeAndDeserialize(boolean isPartitioned) throws Exception {
        // verify when writeResult is not null
        TestingWriteResult testingWriteResult = new TestingWriteResult(2);
        TablePath tablePath = TablePath.of("db1", "tb1");
        TableBucket tableBucket =
                isPartitioned ? new TableBucket(1, 1000L, 2) : new TableBucket(1, 2);
        String partitionName = isPartitioned ? "partition1" : null;
        TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, partitionName, testingWriteResult, 10, 20);

        // test serialize and deserialize
        byte[] serialized = tableBucketWriteResultSerializer.serialize(tableBucketWriteResult);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);

        assertThat(deserialized.tablePath()).isEqualTo(tablePath);
        assertThat(deserialized.tableBucket()).isEqualTo(tableBucket);
        assertThat(deserialized.partitionName()).isEqualTo(partitionName);
        TestingWriteResult deserializedWriteResult = deserialized.writeResult();
        assertThat(deserializedWriteResult).isNotNull();
        assertThat(deserializedWriteResult.getWriteResult())
                .isEqualTo(testingWriteResult.getWriteResult());
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(20);

        // verify when writeResult is null
        tableBucketWriteResult =
                new TableBucketWriteResult<>(tablePath, tableBucket, partitionName, null, 20, 30);
        serialized = tableBucketWriteResultSerializer.serialize(tableBucketWriteResult);
        deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);
        assertThat(deserialized.tablePath()).isEqualTo(tablePath);
        assertThat(deserialized.tableBucket()).isEqualTo(tableBucket);
        assertThat(deserialized.partitionName()).isEqualTo(partitionName);
        assertThat(deserialized.writeResult()).isNull();
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(30);
    }
}
