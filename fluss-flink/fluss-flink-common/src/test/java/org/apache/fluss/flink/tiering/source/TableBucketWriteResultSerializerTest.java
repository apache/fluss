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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;
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
                        tablePath, tableBucket, partitionName, testingWriteResult, 10, 30L, 20);

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
        assertThat(deserialized.isFailedMarker()).isFalse();
        assertThat(deserialized.failReason()).isNull();

        // verify when writeResult is null
        tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, partitionName, null, 20, 30L, 30);
        serialized = tableBucketWriteResultSerializer.serialize(tableBucketWriteResult);
        deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);
        assertThat(deserialized.tablePath()).isEqualTo(tablePath);
        assertThat(deserialized.tableBucket()).isEqualTo(tableBucket);
        assertThat(deserialized.partitionName()).isEqualTo(partitionName);
        assertThat(deserialized.writeResult()).isNull();
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(30);
        assertThat(deserialized.isFailedMarker()).isFalse();
    }

    @Test
    void testSerializeAndDeserializeFailedMarker() throws Exception {
        long tableId = 12345L;
        String failReason = "Test failure reason";

        TableBucketWriteResult<TestingWriteResult> failedMarker =
                TableBucketWriteResult.failedMarker(tableId, failReason);

        // test serialize and deserialize
        byte[] serialized = tableBucketWriteResultSerializer.serialize(failedMarker);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);

        assertThat(deserialized.isFailedMarker()).isTrue();
        assertThat(deserialized.tableBucket().getTableId()).isEqualTo(tableId);
        assertThat(deserialized.failReason()).isEqualTo(failReason);

        // verify other fields are set to expected values for failed markers
        assertThat(deserialized.tablePath()).isNull();
        assertThat(deserialized.writeResult()).isNull();
        assertThat(deserialized.tableBucket().getBucket()).isEqualTo(-1);
    }

    @Test
    void testSerializeAndDeserializeFailedMarkerWithNullReason() throws Exception {
        long tableId = 67890L;

        TableBucketWriteResult<TestingWriteResult> failedMarker =
                TableBucketWriteResult.failedMarker(tableId, null);

        // test serialize and deserialize
        byte[] serialized = tableBucketWriteResultSerializer.serialize(failedMarker);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);

        assertThat(deserialized.isFailedMarker()).isTrue();
        assertThat(deserialized.tableBucket().getTableId()).isEqualTo(tableId);
        assertThat(deserialized.failReason()).isNull();
    }
}
