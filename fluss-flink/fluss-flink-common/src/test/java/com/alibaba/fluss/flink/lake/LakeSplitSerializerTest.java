/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.lake;

import com.alibaba.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import com.alibaba.fluss.flink.lake.split.LakeSnapshotSplit;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lake.source.LakeSplit;
import com.alibaba.fluss.metadata.TableBucket;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;

import static com.alibaba.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static com.alibaba.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit.LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test case for {@link LakeSplitSerializer}. */
public class LakeSplitSerializerTest {
    private static final byte LAKE_SNAPSHOT_SPLIT_KIND = -1;

    private final SimpleVersionedSerializer<LakeSplit> mockSourceSerializer =
            Mockito.mock(SimpleVersionedSerializer.class);

    @Mock private LakeSplit mockLakeSplit = Mockito.mock(LakeSplit.class);

    @Mock private TableBucket mockTableBucket = Mockito.mock(TableBucket.class);

    private final LakeSplitSerializer serializer = new LakeSplitSerializer(mockSourceSerializer);

    @Test
    void testSerializeAndDeserializeLakeSnapshotSplit() throws IOException {
        // 准备测试数据
        byte[] testData = "test-lake-split".getBytes();
        int serializerVersion = 3;

        when(mockSourceSerializer.serialize(mockLakeSplit)).thenReturn(testData);
        when(mockSourceSerializer.getVersion()).thenReturn(serializerVersion);
        when(mockSourceSerializer.deserialize(serializerVersion, testData))
                .thenReturn(mockLakeSplit);

        LakeSnapshotSplit originalSplit =
                new LakeSnapshotSplit(mockTableBucket, "2025-08-15", mockLakeSplit);

        DataOutputSerializer output = new DataOutputSerializer(1024);
        serializer.serialize(output, originalSplit);

        SourceSplitBase deserializedSplit =
                serializer.deserialize(
                        LAKE_SNAPSHOT_SPLIT_KIND,
                        mockTableBucket,
                        "2025-08-15",
                        new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(deserializedSplit instanceof LakeSnapshotSplit).isTrue();
        LakeSnapshotSplit result = (LakeSnapshotSplit) deserializedSplit;

        assertThat(mockTableBucket).isEqualTo(result.getTableBucket());
        assertThat("2025-08-15").isEqualTo(result.getPartitionName());
        assertThat(mockLakeSplit).isEqualTo(result.getLakeSplit());

        verify(mockSourceSerializer).serialize(mockLakeSplit);
        verify(mockSourceSerializer).getVersion();
        verify(mockSourceSerializer).deserialize(serializerVersion, testData);
    }

    @Test
    void testSerializeAndDeserializeLakeSnapshotAndFlussLogSplit() throws IOException {
        byte[] testData = "test-lake-split".getBytes();
        int serializerVersion = 3;
        long stoppingOffset = 1024;

        when(mockSourceSerializer.serialize(mockLakeSplit)).thenReturn(testData);
        when(mockSourceSerializer.getVersion()).thenReturn(serializerVersion);
        when(mockSourceSerializer.deserialize(serializerVersion, testData))
                .thenReturn(mockLakeSplit);

        LakeSnapshotAndFlussLogSplit originalSplit =
                new LakeSnapshotAndFlussLogSplit(
                        mockTableBucket,
                        "2025-08-15",
                        Collections.singletonList(mockLakeSplit),
                        EARLIEST_OFFSET,
                        stoppingOffset);

        DataOutputSerializer output = new DataOutputSerializer(1024);
        serializer.serialize(output, originalSplit);

        SourceSplitBase deserializedSplit =
                serializer.deserialize(
                        LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND,
                        mockTableBucket,
                        "2025-08-15",
                        new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(deserializedSplit instanceof LakeSnapshotAndFlussLogSplit).isTrue();
        LakeSnapshotAndFlussLogSplit result = (LakeSnapshotAndFlussLogSplit) deserializedSplit;

        assertThat(mockTableBucket).isEqualTo(result.getTableBucket());
        assertThat("2025-08-15").isEqualTo(result.getPartitionName());
        assertThat(Collections.singletonList(mockLakeSplit)).isEqualTo(result.getLakeSplits());
        assertThat(EARLIEST_OFFSET).isEqualTo(result.getStartingOffset());
        assertThat(stoppingOffset).isEqualTo(result.getStoppingOffset().get());

        verify(mockSourceSerializer).serialize(mockLakeSplit);
        verify(mockSourceSerializer).getVersion();
        verify(mockSourceSerializer).deserialize(serializerVersion, testData);
    }

    @Test
    void testDeserializeWithWrongSplitKind() throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(1024);
        output.writeInt(0);

        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        (byte) 99,
                                        mockTableBucket,
                                        "2023-10-01",
                                        new DataInputDeserializer(output.getCopyOfBuffer())))
                .withFailMessage(() -> "Unsupported split kind: ")
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
