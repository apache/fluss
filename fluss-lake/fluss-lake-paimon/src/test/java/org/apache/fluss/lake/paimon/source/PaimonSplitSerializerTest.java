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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test case for {@link PaimonSplitSerializer}. */
class PaimonSplitSerializerTest extends PaimonSourceTestBase {
    private final PaimonSplitSerializer serializer = new PaimonSplitSerializer();

    @Test
    void testSerializeAndDeserialize() throws Exception {
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

        PaimonSplit originalPaimonSplit = plan.get(0);
        byte[] serialized = serializer.serialize(originalPaimonSplit);
        PaimonSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserialized.dataSplit()).isEqualTo(originalPaimonSplit.dataSplit());
        assertThat(deserialized.isBucketUnAware()).isEqualTo(originalPaimonSplit.isBucketUnAware());
    }

    @Test
    void testJavaSerializationRoundTrip() throws Exception {
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

        PaimonSplit original = plan.get(0);

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
    void testDeserializeWithInvalidData() {
        byte[] invalidData = "invalid".getBytes();
        assertThatThrownBy(() -> serializer.deserialize(1, invalidData))
                .isInstanceOf(IOException.class);
    }
}
