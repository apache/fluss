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

package com.alibaba.fluss.lake.paimon.source;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.source.Planner;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimon;

/** Split panner for paimon table. */
public class PaimonSplitPlanner implements Planner<PaimonSplit> {

    private final Configuration paimonConfig;
    private final TablePath tablePath;
    private final @Nullable Predicate predicate;
    private final long snapshotId;

    public PaimonSplitPlanner(
            Configuration paimonConfig,
            TablePath tablePath,
            @Nullable Predicate predicate,
            long snapshotId) {
        this.paimonConfig = paimonConfig;
        this.tablePath = tablePath;
        this.predicate = predicate;
        this.snapshotId = snapshotId;
    }

    @Override
    public List<PaimonSplit> plan() {
        try {
            List<PaimonSplit> splits = new ArrayList<>();
            try (Catalog catalog = getCatalog()) {
                FileStoreTable fileStoreTable = getTable(catalog, tablePath, snapshotId);
                // TODO: support filter .withFilter(predicate)
                InnerTableScan tableScan = fileStoreTable.newScan();
                for (Split split : tableScan.plan().splits()) {
                    DataSplit dataSplit = (DataSplit) split;
                    splits.add(new PaimonSplit(dataSplit));
                }
            }
            return splits;
        } catch (Exception e) {
            throw new RuntimeException("Failed to plan splits for paimon.", e);
        }
    }

    private Catalog getCatalog() {
        return CatalogFactory.createCatalog(
                CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
    }

    private FileStoreTable getTable(Catalog catalog, TablePath tablePath, long snapshotId)
            throws Exception {
        return (FileStoreTable)
                catalog.getTable(toPaimon(tablePath))
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                        String.valueOf(snapshotId)));
    }
}
