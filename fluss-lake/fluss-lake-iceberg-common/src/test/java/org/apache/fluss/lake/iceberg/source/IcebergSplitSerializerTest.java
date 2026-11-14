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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.SPLIT_DIR;
import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.assertSplitEquals;
import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.goldenSplit;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Test case for {@link IcebergSplitSerializer}. */
class IcebergSplitSerializerTest extends IcebergSourceTestBase {

    private final IcebergSplitSerializer serializer = new IcebergSplitSerializer();

    @Test
    void testSerializeAndDeserialize() throws Exception {
        // prepare iceberg table
        TablePath tablePath = TablePath.of(DEFAULT_DB, DEFAULT_TABLE);
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()),
                        optional(3, "c3", Types.StringType.get()));
        PartitionSpec partitionSpec =
                PartitionSpec.builderFor(schema).bucket("c1", DEFAULT_BUCKET_NUM).build();
        createTable(tablePath, schema, partitionSpec);

        // write data
        Table table = getTable(tablePath);

        GenericRecord record1 = createIcebergRecord(schema, 12, "a", "A");
        GenericRecord record2 = createIcebergRecord(schema, 13, "b", "B");

        writeRecord(table, Arrays.asList(record1, record2), null, 0);
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();

        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<IcebergSplit> plan = lakeSource.createPlanner(snapshot::snapshotId).plan();

        IcebergSplit originalIcebergSplit = plan.get(0);
        byte[] serialized = serializer.serialize(originalIcebergSplit);
        IcebergSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

        assertSplitEquals(deserialized, originalIcebergSplit);
    }

    @Test
    void testSerializeNullPartitionValues() throws Exception {
        IcebergSplit goldenSplit = goldenSplit();
        IcebergSplit nullPartition = new IcebergSplit(goldenSplit.fileScanTask(), 1, null);
        IcebergSplit nullPartitionValue =
                new IcebergSplit(goldenSplit.fileScanTask(), 1, Arrays.asList("a", null));

        for (IcebergSplit split : Arrays.asList(nullPartition, nullPartitionValue)) {
            IcebergSplit deserialized =
                    serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
            assertSplitEquals(deserialized, split);
        }
    }

    @Test
    void testDeserializeGoldenFiles() throws Exception {
        IcebergSplit expected = goldenSplit();
        for (int version = TestingGoldenFiles.firstReadableVersion();
                version <= serializer.getVersion();
                version++) {
            IcebergSplit deserialized =
                    serializer.deserialize(version, TestingGoldenFiles.read(SPLIT_DIR, version));
            assertSplitEquals(deserialized, expected);
        }
    }

    @Test
    void testCurrentVersionMatchesGoldenFile() throws Exception {
        // other Iceberg versions write the Iceberg objects in the JSON differently
        assumeThat(TestingGoldenFiles.isGoldenIcebergVersion())
                .as("the golden files were written with another Iceberg version")
                .isTrue();
        // the current format must not change without bumping the version
        assertThat(serializer.serialize(goldenSplit()))
                .isEqualTo(TestingGoldenFiles.read(SPLIT_DIR, serializer.getVersion()));
    }

    @Test
    void testRegenerateGoldenFiles() throws Exception {
        assumeThat(TestingGoldenFiles.shouldRegenerate())
                .as("golden files are only regenerated on request")
                .isTrue();

        IcebergSplit goldenSplit = goldenSplit();
        TestingGoldenFiles.write(SPLIT_DIR, 1, InstantiationUtils.serializeObject(goldenSplit));
        TestingGoldenFiles.write(
                SPLIT_DIR, serializer.getVersion(), serializer.serialize(goldenSplit));
    }

    @Test
    void testJavaSerializationRoundTrip() throws Exception {
        // prepare iceberg table
        TablePath tablePath = TablePath.of(DEFAULT_DB, DEFAULT_TABLE);
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()),
                        optional(3, "c3", Types.StringType.get()));
        PartitionSpec partitionSpec =
                PartitionSpec.builderFor(schema).bucket("c1", DEFAULT_BUCKET_NUM).build();
        createTable(tablePath, schema, partitionSpec);

        // write data
        Table table = getTable(tablePath);

        GenericRecord record1 = createIcebergRecord(schema, 12, "a", "A");
        GenericRecord record2 = createIcebergRecord(schema, 13, "b", "B");

        writeRecord(table, Arrays.asList(record1, record2), null, 0);
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();

        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<IcebergSplit> plan = lakeSource.createPlanner(snapshot::snapshotId).plan();

        IcebergSplit original = plan.get(0);

        // Serialize via Java serialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }
        byte[] bytes = baos.toByteArray();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            IcebergSplit deserialized = (IcebergSplit) ois.readObject();

            assertThat(deserialized.bucket()).isEqualTo(original.bucket());
            assertThat(deserialized.partition()).isEqualTo(original.partition());
            assertThat(deserialized.fileScanTask().file().location())
                    .isEqualTo(original.fileScanTask().file().location());
        }
    }

    @Test
    void testDeserializeWithInvalidData() throws Exception {
        byte[] invalidData = "invalid".getBytes();
        assertThatThrownBy(() -> serializer.deserialize(1, invalidData))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> serializer.deserialize(2, invalidData))
                .isInstanceOf(IOException.class);

        assertThatThrownBy(() -> serializer.deserialize(2, "{}".getBytes()))
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining("Missing field");
    }

    @Test
    void testDeserializeUnknownVersion() {
        assertThatThrownBy(() -> serializer.deserialize(3, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported IcebergSplit serializer version 3");
    }
}
