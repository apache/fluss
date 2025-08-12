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

package com.alibaba.fluss.lake.paimon.source;

import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
    }

    @Test
    void testDeserializeWithInvalidData() {
        byte[] invalidData = "invalid".getBytes();
        assertThatThrownBy(() -> serializer.deserialize(1, invalidData))
                .isInstanceOf(IOException.class);
    }
}
