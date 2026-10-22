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

import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    }

    @Test
    void testLakeWriteResultSerializerVersionIsPersisted() throws Exception {
        VersionRecordingWriteResultSerializer lakeSerializer =
                new VersionRecordingWriteResultSerializer(7);
        TableBucketWriteResultSerializer<TestingWriteResult> serializer =
                new TableBucketWriteResultSerializer<>(lakeSerializer);

        TableBucketWriteResult<TestingWriteResult> deserialized =
                serializer.deserialize(
                        serializer.getVersion(),
                        serializer.serialize(
                                new TableBucketWriteResult<>(
                                        TablePath.of("db1", "tb1"),
                                        new TableBucket(1, 2),
                                        null,
                                        new TestingWriteResult(5),
                                        10,
                                        30L,
                                        1)));

        assertThat(lakeSerializer.deserializedVersions).containsExactly(7);
        assertThat(deserialized.writeResult().getWriteResult()).isEqualTo(5);
        assertThat(deserialized.logEndOffset()).isEqualTo(10);
        assertThat(deserialized.maxTimestamp()).isEqualTo(30L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeserializeVersionOne(boolean withWriteResult) throws Exception {
        VersionRecordingWriteResultSerializer lakeSerializer =
                new VersionRecordingWriteResultSerializer(7);
        TableBucketWriteResultSerializer<TestingWriteResult> serializer =
                new TableBucketWriteResultSerializer<>(lakeSerializer);

        // the layout of VERSION_1, which doesn't persist the lake write result serializer version
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("db1");
        out.writeUTF("tb1");
        out.writeLong(1L);
        out.writeBoolean(true);
        out.writeLong(1000L);
        out.writeUTF("partition1");
        out.writeInt(2);
        if (withWriteResult) {
            byte[] writeResultBytes = lakeSerializer.serialize(new TestingWriteResult(5));
            out.writeInt(writeResultBytes.length);
            out.write(writeResultBytes);
        } else {
            out.writeInt(-1);
        }
        out.writeLong(10L);
        out.writeLong(30L);
        out.writeInt(3);

        TableBucketWriteResult<TestingWriteResult> deserialized =
                serializer.deserialize(1, out.getCopyOfBuffer());

        assertThat(deserialized.tablePath()).isEqualTo(TablePath.of("db1", "tb1"));
        assertThat(deserialized.tableBucket()).isEqualTo(new TableBucket(1L, 1000L, 2));
        assertThat(deserialized.partitionName()).isEqualTo("partition1");
        assertThat(deserialized.logEndOffset()).isEqualTo(10L);
        assertThat(deserialized.maxTimestamp()).isEqualTo(30L);
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(3);
        if (withWriteResult) {
            // VERSION_1 passed its own version to the lake serializer
            assertThat(lakeSerializer.deserializedVersions).containsExactly(1);
            assertThat(deserialized.writeResult().getWriteResult()).isEqualTo(5);
        } else {
            assertThat(lakeSerializer.deserializedVersions).isEmpty();
            assertThat(deserialized.writeResult()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 3})
    void testDeserializeUnknownVersion(int version) {
        assertThatThrownBy(() -> tableBucketWriteResultSerializer.deserialize(version, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessage("Unknown version " + version);
    }

    /** A lake write result serializer which records the versions passed for deserialization. */
    private static class VersionRecordingWriteResultSerializer
            implements SimpleVersionedSerializer<TestingWriteResult> {

        private final int version;
        private final List<Integer> deserializedVersions = new ArrayList<>();

        private VersionRecordingWriteResultSerializer(int version) {
            this.version = version;
        }

        @Override
        public int getVersion() {
            return version;
        }

        @Override
        public byte[] serialize(TestingWriteResult writeResult) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(4);
            out.writeInt(writeResult.getWriteResult());
            return out.getCopyOfBuffer();
        }

        @Override
        public TestingWriteResult deserialize(int version, byte[] serialized) throws IOException {
            deserializedVersions.add(version);
            return new TestingWriteResult(new DataInputDeserializer(serialized).readInt());
        }
    }
}
