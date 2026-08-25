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
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableBucketWriteResultSerializer}. */
class TableBucketWriteResultSerializerTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSerializeAndDeserialize(boolean isPartitioned) throws Exception {
        RecordingWriteResultSerializer writeResultSerializer = new RecordingWriteResultSerializer();
        TableBucketWriteResultSerializer<TestingWriteResult> serializer =
                new TableBucketWriteResultSerializer<>(writeResultSerializer);

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
        byte[] serialized = serializer.serialize(tableBucketWriteResult);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserialized.tablePath()).isEqualTo(tablePath);
        assertThat(deserialized.tableBucket()).isEqualTo(tableBucket);
        assertThat(deserialized.partitionName()).isEqualTo(partitionName);
        TestingWriteResult deserializedWriteResult = deserialized.writeResult();
        assertThat(deserializedWriteResult).isNotNull();
        assertThat(deserializedWriteResult.getWriteResult())
                .isEqualTo(testingWriteResult.getWriteResult());
        assertThat(writeResultSerializer.deserializedVersion).isEqualTo(7);
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(20);

        // verify when writeResult is null
        tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, partitionName, null, 20, 30L, 30);
        serialized = serializer.serialize(tableBucketWriteResult);
        deserialized = serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserialized.tablePath()).isEqualTo(tablePath);
        assertThat(deserialized.tableBucket()).isEqualTo(tableBucket);
        assertThat(deserialized.partitionName()).isEqualTo(partitionName);
        assertThat(deserialized.writeResult()).isNull();
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(30);

        // VERSION_1 did not store the nested serializer version and passed 1 to it.
        tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, partitionName, testingWriteResult, 10, 30L, 20);
        byte[] version2Bytes = serializer.serialize(tableBucketWriteResult);
        byte[] version1Bytes = removeNestedSerializerVersion(version2Bytes);
        deserialized = serializer.deserialize(1, version1Bytes);
        assertThat(deserialized.writeResult().getWriteResult())
                .isEqualTo(testingWriteResult.getWriteResult());
        assertThat(writeResultSerializer.deserializedVersion).isOne();
    }

    private byte[] removeNestedSerializerVersion(byte[] version2Bytes) throws IOException {
        int versionOffset;
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(version2Bytes))) {
            in.readUTF();
            in.readUTF();
            in.readLong();
            if (in.readBoolean()) {
                in.readLong();
                in.readUTF();
            }
            in.readInt();
            versionOffset = version2Bytes.length - in.available();
        }

        byte[] version1Bytes = new byte[version2Bytes.length - Integer.BYTES];
        System.arraycopy(version2Bytes, 0, version1Bytes, 0, versionOffset);
        System.arraycopy(
                version2Bytes,
                versionOffset + Integer.BYTES,
                version1Bytes,
                versionOffset,
                version1Bytes.length - versionOffset);
        return version1Bytes;
    }

    private static class RecordingWriteResultSerializer
            implements SimpleVersionedSerializer<TestingWriteResult> {

        private int deserializedVersion;

        @Override
        public int getVersion() {
            return 7;
        }

        @Override
        public byte[] serialize(TestingWriteResult obj) throws IOException {
            return new TestingWriteResultSerializer().serialize(obj);
        }

        @Override
        public TestingWriteResult deserialize(int version, byte[] serialized) throws IOException {
            deserializedVersion = version;
            return new TestingWriteResultSerializer().deserialize(version, serialized);
        }
    }
}
