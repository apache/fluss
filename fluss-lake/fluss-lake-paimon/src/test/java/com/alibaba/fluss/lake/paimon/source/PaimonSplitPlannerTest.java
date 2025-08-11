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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test case for {@link PaimonSplitPlanner}. */
class PaimonSplitPlannerTest extends PaimonSourceTestBase {
    @Test
    void testPlan() throws Exception {
        // prepare paimon table
        int bucketNum = 2;
        TablePath tablePath = TablePath.of(DEFAULT_DB, DEFAULT_TABLE);
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("c3", DataTypes.STRING());
        builder.partitionKeys("c3");
        builder.primaryKey("c1", "c3");
        builder.option(CoreOptions.BUCKET.key(), String.valueOf(bucketNum));
        builder.option(CoreOptions.BUCKET_KEY.key(), "c1");
        createTable(tablePath, builder.build());
        Table table =
                paimonCatalog.getTable(
                        Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName()));

        GenericRow record1 =
                GenericRow.of(12, BinaryString.fromString("a"), BinaryString.fromString("A"));
        GenericRow record2 =
                GenericRow.of(13, BinaryString.fromString("a"), BinaryString.fromString("A"));
        writeRecord(tablePath, Arrays.asList(record1, record2));
        Snapshot snapshot = table.latestSnapshot().get();

        LakeSource<PaimonSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<PaimonSplit> paimonSplits = lakeSource.createPlanner(snapshot::id).plan();

        List<Split> actualSplits = ((FileStoreTable) table).newScan().plan().splits();

        assertThat(actualSplits).hasSize(paimonSplits.size());
        assertThat(actualSplits)
                .isEqualTo(
                        paimonSplits.stream()
                                .map(PaimonSplit::dataSplit)
                                .collect(Collectors.toList()));
    }
}
