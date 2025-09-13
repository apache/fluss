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

import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        assertThat(deserialized.fileScanTask().file().location())
                .isEqualTo(originalIcebergSplit.fileScanTask().file().location());
        assertThat(deserialized.partition()).isEqualTo(originalIcebergSplit.partition());
        assertThat(deserialized.bucket()).isEqualTo(originalIcebergSplit.bucket());
    }

    @Test
    void testDeserializeWithInvalidData() {
        byte[] invalidData = "invalid".getBytes();
        assertThatThrownBy(() -> serializer.deserialize(1, invalidData))
                .isInstanceOf(IOException.class);
    }
}
