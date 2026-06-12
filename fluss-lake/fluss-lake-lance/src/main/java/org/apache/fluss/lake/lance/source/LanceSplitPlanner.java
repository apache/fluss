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

package org.apache.fluss.lake.lance.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lance.LanceConfig;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.metadata.TablePath;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.ReadOptions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Split planner for Lance table. */
public class LanceSplitPlanner implements Planner<LanceSplit> {
    private final LanceConfig lanceConfig;
    private final long snapshotId;
    private final @Nullable Integer limit;

    public LanceSplitPlanner(
            Configuration configuration,
            TablePath tablePath,
            long snapshotId,
            @Nullable Integer limit) {
        this.lanceConfig =
                LanceConfig.from(
                        configuration.toMap(),
                        Collections.emptyMap(),
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());
        this.snapshotId = snapshotId;
        this.limit = limit;
    }

    @Override
    public List<LanceSplit> plan() {
        ReadOptions readOptions = LanceConfig.genReadOptionFromConfig(lanceConfig);

        List<LanceSplit> splits = new ArrayList<LanceSplit>();
        try (Dataset dataset = Dataset.open(lanceConfig.getDatasetUri(), readOptions)) {
            if (snapshotId > 0) {
                dataset.checkoutVersion(snapshotId);
            }

            long remaining = limit == null ? -1L : Math.max(0L, limit.longValue());
            for (Fragment fragment : dataset.getFragments()) {
                long fragmentRows = fragment.countRows();
                long scanLimit = -1L;
                if (limit != null) {
                    scanLimit = Math.max(0L, Math.min(fragmentRows, remaining));
                    remaining -= scanLimit;
                }

                splits.add(
                        new LanceSplit(
                                fragment.getId(),
                                snapshotId,
                                fragmentRows,
                                scanLimit,
                                -1,
                                Collections.<String>emptyList()));
            }
        }
        return splits;
    }
}
