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
 *
 *
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test case for {@link PaimonSplitSerializer}. */
class PaimonSplitSerializerTest extends PaimonSourceTestBase {
    private final PaimonSplitSerializer serializer = new PaimonSplitSerializer();

    @Test
    void testSerializeAndDeserialize() throws Exception {
        // partitioned pk-table split and bucket-unaware append-table split with empty partition
        assertV3RoundTrip(createStringPartitionSplit());

        PaimonSplit bucketUnAwareSplit = createBucketUnAwareSplit();
        assertThat(bucketUnAwareSplit.isBucketUnAware()).isTrue();
        assertThat(bucketUnAwareSplit.partition()).isEmpty();
        assertV3RoundTrip(bucketUnAwareSplit);
    }

    @Test
    void testJavaSerializationRoundTrip() throws Exception {
        PaimonSplit original = createStringPartitionSplit();

        // Serialize via Java serialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }
        byte[] bytes = baos.toByteArray();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            PaimonSplit deserialized = (PaimonSplit) ois.readObject();

            assertThat(deserialized.bucket()).isEqualTo(original.bucket());
            assertThat(deserialized.partition()).isEqualTo(original.partition());
            assertThat(deserialized.dataSplit()).isEqualTo(original.dataSplit());
        }
    }

    @Test
    void testDeserializeLegacyBytes() throws Exception {
        // VERSION_1/VERSION_2 bytes written by older versions must remain restorable after the
        // VERSION_3 upgrade
        PaimonSplit original = createStringPartitionSplit();

        // VERSION_1 did not store partition values, they were derived from DataSplit.partition()
        PaimonSplit fromV1 = serializer.deserialize(1, serializeVersion1(original));
        assertThat(fromV1.dataSplit()).isEqualTo(original.dataSplit());
        assertThat(fromV1.isBucketUnAware()).isEqualTo(original.isBucketUnAware());
        assertThat(fromV1.partition()).isEqualTo(Collections.singletonList("A"));

        PaimonSplit fromV2 = serializer.deserialize(2, serializeVersion2(original));
        assertThat(fromV2.dataSplit()).isEqualTo(original.dataSplit());
        assertThat(fromV2.isBucketUnAware()).isEqualTo(original.isBucketUnAware());
        assertThat(fromV2.partition()).isEqualTo(original.partition());
    }

    @Test
    void testDataSplitSubclassDispatch() throws Exception {
        PaimonSplit original = createStringPartitionSplit();

        // FallbackDataSplit appends the isFallback flag after the base payload; the subtype and
        // its extra state must survive the round trip. Its constructors are private, so build one
        // via its public deserialize API.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        original.dataSplit().serialize(view);
        view.writeBoolean(true);
        DataSplit fallbackDataSplit =
                FallbackReadFileStoreTable.FallbackDataSplit.deserialize(
                        new DataInputViewStreamWrapper(
                                new ByteArrayInputStream(out.toByteArray())));

        PaimonSplit fallbackSplit = new PaimonSplit(fallbackDataSplit, false, original.partition());
        PaimonSplit deserialized = assertV3RoundTrip(fallbackSplit);
        assertThat(deserialized.dataSplit())
                .isInstanceOf(FallbackReadFileStoreTable.FallbackDataSplit.class);

        // unknown subclasses may append extra fields as well and must be rejected at write time
        // instead of silently corrupting the stream on restore
        PaimonSplit unknown = new PaimonSplit(new DataSplit() {}, false, Collections.emptyList());
        assertThatThrownBy(() -> serializer.serialize(unknown))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported DataSplit class");
    }

    @Test
    void testDeserializeInvalidInput() {
        byte[] invalidData = "invalid".getBytes();
        assertThatThrownBy(() -> serializer.deserialize(1, invalidData))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> serializer.deserialize(3, invalidData))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> serializer.deserialize(99, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported PaimonSplit serialization version");
    }

    private PaimonSplit assertV3RoundTrip(PaimonSplit original) throws IOException {
        byte[] serialized = serializer.serialize(original);
        // VERSION_3 bytes must not embed the DataSplit class name, otherwise relocation (shading)
        // in downstream distributions would break state restore again
        assertThat(new String(serialized, StandardCharsets.ISO_8859_1))
                .doesNotContain(DataSplit.class.getName());

        PaimonSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserialized.dataSplit()).isEqualTo(original.dataSplit());
        assertThat(deserialized.isBucketUnAware()).isEqualTo(original.isBucketUnAware());
        assertThat(deserialized.partition()).isEqualTo(original.partition());
        return deserialized;
    }

    private PaimonSplit createStringPartitionSplit() throws Exception {
        // prepare paimon table
        int bucketNum = 1;
        TablePath tablePath = TablePath.of(DEFAULT_DB, DEFAULT_TABLE);
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("c3", DataTypes.STRING());
        builder.partitionKeys("c3");
        builder.primaryKey("c1", "c3");
        builder.option(CoreOptions.BUCKET.key(), String.valueOf(bucketNum));
        createTable(tablePath, builder.build());
        Table table = getTable(tablePath);

        GenericRow record1 =
                GenericRow.of(12, BinaryString.fromString("a"), BinaryString.fromString("A"));
        writeRecord(tablePath, Collections.singletonList(record1));
        Snapshot snapshot = table.latestSnapshot().get();

        LakeSource<PaimonSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<PaimonSplit> plan = lakeSource.createPlanner(snapshot::id).plan();

        return plan.get(0);
    }

    private PaimonSplit createBucketUnAwareSplit() throws Exception {
        // an append table without primary key and bucket key is bucket-unaware
        TablePath tablePath = TablePath.of(DEFAULT_DB, "bucket_unaware_table");
        Schema.Builder builder =
                Schema.newBuilder().column("c1", DataTypes.INT()).column("c2", DataTypes.STRING());
        createTable(tablePath, builder.build());
        Table table = getTable(tablePath);

        GenericRow record1 = GenericRow.of(12, BinaryString.fromString("a"));
        writeRecord(tablePath, Collections.singletonList(record1));
        Snapshot snapshot = table.latestSnapshot().get();

        LakeSource<PaimonSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<PaimonSplit> plan = lakeSource.createPlanner(snapshot::id).plan();

        return plan.get(0);
    }

    private byte[] serializeVersion1(PaimonSplit paimonSplit) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        InstantiationUtil.serializeObject(view, paimonSplit.dataSplit());
        view.writeBoolean(paimonSplit.isBucketUnAware());
        return out.toByteArray();
    }

    private byte[] serializeVersion2(PaimonSplit paimonSplit) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        InstantiationUtil.serializeObject(view, paimonSplit.dataSplit());
        view.writeBoolean(paimonSplit.isBucketUnAware());
        List<String> partition = paimonSplit.partition();
        view.writeInt(partition.size());
        for (String value : partition) {
            view.writeUTF(value);
        }
        return out.toByteArray();
    }
}
