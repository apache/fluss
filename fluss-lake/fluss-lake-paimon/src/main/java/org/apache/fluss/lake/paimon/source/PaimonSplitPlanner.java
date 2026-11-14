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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.paimon.source.PaimonTableLoader.CatalogTableLoader;
import org.apache.fluss.lake.paimon.source.PaimonTableLoader.PaimonTableHandle;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toFlussPartitionValues;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toFlussRowType;

/** Split panner for paimon table. */
public class PaimonSplitPlanner implements Planner<PaimonSplit> {

    private final PaimonTableLoader tableLoader;
    private final @Nullable Predicate predicate;
    private final long snapshotId;

    public PaimonSplitPlanner(
            Configuration paimonConfig,
            TablePath tablePath,
            @Nullable Predicate predicate,
            long snapshotId) {
        this(new CatalogTableLoader(paimonConfig, tablePath), predicate, snapshotId);
    }

    PaimonSplitPlanner(
            PaimonTableLoader tableLoader, @Nullable Predicate predicate, long snapshotId) {
        this.tableLoader = tableLoader;
        this.predicate = predicate;
        this.snapshotId = snapshotId;
    }

    @Override
    public List<PaimonSplit> plan() {
        try {
            List<PaimonSplit> splits = new ArrayList<>();
            try (PaimonTableHandle handle = tableLoader.load()) {
                FileStoreTable fileStoreTable = getTable(handle, snapshotId);
                InnerTableScan tableScan = fileStoreTable.newScan();
                boolean isBucketUnAware = fileStoreTable.bucketMode() == BucketMode.BUCKET_UNAWARE;
                RowType flussPartitionType =
                        toFlussRowType(fileStoreTable.schema().logicalPartitionType());

                if (predicate != null) {
                    tableScan = tableScan.withFilter(predicate);
                }
                for (Split split : tableScan.plan().splits()) {
                    DataSplit dataSplit = (DataSplit) split;
                    BinaryRow partitionRow = dataSplit.partition();
                    List<String> partition =
                            partitionRow.getFieldCount() == 0
                                    ? Collections.emptyList()
                                    : toFlussPartitionValues(partitionRow, flussPartitionType);
                    splits.add(new PaimonSplit(dataSplit, isBucketUnAware, partition));
                }
            }
            return splits;
        } catch (Exception e) {
            throw new RuntimeException("Failed to plan splits for paimon.", e);
        }
    }

    private FileStoreTable getTable(PaimonTableHandle handle, long snapshotId) {
        // Filters are built with the current schema. Keep it when selecting an older snapshot so
        // Paimon can evolve the file statistics to match the predicates.
        return handle.table()
                .copyWithoutTimeTravel(
                        Collections.singletonMap(
                                CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshotId)));
    }
}
