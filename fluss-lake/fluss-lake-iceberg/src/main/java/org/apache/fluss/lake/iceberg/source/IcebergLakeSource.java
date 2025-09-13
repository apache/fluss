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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.utils.IcebergCatalogUtils;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;

/** Iceberg lake source. */
public class IcebergLakeSource implements LakeSource<IcebergSplit> {
    private static final long serialVersionUID = 1L;
    private final Configuration icebergConfig;
    private final TablePath tablePath;
    private @Nullable int[][] project;

    public IcebergLakeSource(Configuration icebergConfig, TablePath tablePath) {
        this.icebergConfig = icebergConfig;
        this.tablePath = tablePath;
    }

    @Override
    public void withProject(int[][] project) {
        this.project = project;
    }

    @Override
    public void withLimit(int limit) {
        throw new UnsupportedOperationException("Not impl.");
    }

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        // TODO: Support filter push down. #1676
        return FilterPushDownResult.of(Collections.emptyList(), predicates);
    }

    @Override
    public Planner<IcebergSplit> createPlanner(PlannerContext context) throws IOException {
        return new IcebergSplitPlanner(icebergConfig, tablePath, context.snapshotId());
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<IcebergSplit> context) throws IOException {
        Catalog catalog = IcebergCatalogUtils.createIcebergCatalog(icebergConfig);
        Table table = catalog.loadTable(toIceberg(tablePath));
        return new IcebergRecordReader(context.lakeSplit().fileScanTask(), table, project);
    }

    @Override
    public SimpleVersionedSerializer<IcebergSplit> getSplitSerializer() {
        return new IcebergSplitSerializer();
    }
}
