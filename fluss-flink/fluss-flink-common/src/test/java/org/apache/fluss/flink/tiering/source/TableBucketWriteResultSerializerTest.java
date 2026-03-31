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

import org.apache.flink.core.memory.DataOutputSerializer;
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

        TableBucketWriteResult<TestingWriteResult> result =
                serializeAndDeserialize(
                        new TableBucketWriteResult<>(
                                tablePath,
                                tableBucket,
                                partitionName,
                                testingWriteResult,
                                10,
                                30L,
                                20,
                                false));
        assertThat(result.tablePath()).isEqualTo(tablePath);
        assertThat(result.tableBucket()).isEqualTo(tableBucket);
        assertThat(result.partitionName()).isEqualTo(partitionName);
        assertThat(result.writeResult().getWriteResult())
                .isEqualTo(testingWriteResult.getWriteResult());
        assertThat(result.isCancelled()).isFalse();

        // verify when writeResult is null, cancelled=true
        result =
                serializeAndDeserialize(
                        new TableBucketWriteResult<>(
                                tablePath, tableBucket, partitionName, null, 20, 30L, 30, true));
        assertThat(result.writeResult()).isNull();
        assertThat(result.isCancelled()).isTrue();
    }

    private TableBucketWriteResult<TestingWriteResult> serializeAndDeserialize(
            TableBucketWriteResult<TestingWriteResult> input) throws Exception {
        byte[] serialized = tableBucketWriteResultSerializer.serialize(input);
        return tableBucketWriteResultSerializer.deserialize(
                tableBucketWriteResultSerializer.getVersion(), serialized);
    }

    @Test
    void testDeserializeVersion1IsBackwardCompatible() throws Exception {
        // Manually construct a version-1 payload (no cancelled flag) and verify
        // that it deserializes correctly with cancelled defaulting to false.
        TablePath tablePath = TablePath.of("db1", "tb1");
        TableBucket tableBucket = new TableBucket(1, 2);
        TestingWriteResult testingWriteResult = new TestingWriteResult(42);
        TestingWriteResultSerializer writeResultSerializer = new TestingWriteResultSerializer();

        DataOutputSerializer out = new DataOutputSerializer(64);
        // table path
        out.writeUTF(tablePath.getDatabaseName());
        out.writeUTF(tablePath.getTableName());
        // bucket (no partition)
        out.writeLong(tableBucket.getTableId());
        out.writeBoolean(false);
        out.writeInt(tableBucket.getBucket());
        // write result
        byte[] writeResultBytes = writeResultSerializer.serialize(testingWriteResult);
        out.writeInt(writeResultBytes.length);
        out.write(writeResultBytes);
        // log end offset
        out.writeLong(100L);
        // max timestamp
        out.writeLong(200L);
        // number of write results
        out.writeInt(3);
        // NOTE: no cancelled flag — this is a version-1 payload
        byte[] v1Bytes = out.getCopyOfBuffer();

        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(1, v1Bytes);

        assertThat(deserialized.tablePath()).isEqualTo(tablePath);
        assertThat(deserialized.tableBucket()).isEqualTo(tableBucket);
        assertThat(deserialized.writeResult()).isNotNull();
        assertThat(deserialized.writeResult().getWriteResult()).isEqualTo(42);
        assertThat(deserialized.logEndOffset()).isEqualTo(100L);
        assertThat(deserialized.maxTimestamp()).isEqualTo(200L);
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(3);
        // cancelled must default to false when deserializing a version-1 payload
        assertThat(deserialized.isCancelled()).isFalse();
    }
}
