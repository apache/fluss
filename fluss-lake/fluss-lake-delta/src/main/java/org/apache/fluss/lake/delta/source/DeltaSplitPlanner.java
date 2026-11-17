/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.delta.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.delta.DeltaConfig;
import org.apache.fluss.lake.delta.utils.DeltaPartitionLiterals;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.metadata.TablePath;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.util.PartitionUtils;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Plans one {@link DeltaSplit} per data file of the Delta table version that Fluss recorded as its
 * lake snapshot.
 */
public class DeltaSplitPlanner implements Planner<DeltaSplit> {

    private final Configuration options;
    private final TablePath tablePath;
    private final long snapshotId;
    private final @Nullable int[][] project;

    public DeltaSplitPlanner(
            Configuration options,
            TablePath tablePath,
            long snapshotId,
            @Nullable int[][] project) {
        this.options = options;
        this.tablePath = tablePath;
        this.snapshotId = snapshotId;
        this.project = project;
    }

    @Override
    public List<DeltaSplit> plan() throws IOException {
        Engine engine = DeltaConfig.createEngine(options);
        Snapshot snapshot =
                TableManager.loadSnapshot(DeltaConfig.tablePath(options, tablePath))
                        .atVersion(snapshotId)
                        .build(engine);
        StructType schema = snapshot.getSchema();
        Scan scan = snapshot.getScanBuilder().withReadSchema(readSchema(schema)).build();
        Row scanState = scan.getScanState(engine);
        String scanStateSchema = scanState.getSchema().toJson();
        String scanStateJson = KernelRowJson.toJson(scanState);
        List<String> partitionColumns = snapshot.getPartitionColumnNames();

        List<DeltaSplit> splits = new ArrayList<>();
        try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine)) {
            while (scanFiles.hasNext()) {
                try (CloseableIterator<Row> rows = scanFiles.next().getRows()) {
                    while (rows.hasNext()) {
                        Row scanFile = rows.next();
                        splits.add(
                                new DeltaSplit(
                                        scanStateSchema,
                                        scanStateJson,
                                        scanFile.getSchema().toJson(),
                                        KernelRowJson.toJson(scanFile),
                                        partition(scanFile, partitionColumns, schema)));
                    }
                }
            }
        }
        return splits;
    }

    /** The columns to read, in the order of the projection. */
    private StructType readSchema(StructType schema) {
        if (project == null) {
            return schema;
        }
        List<StructField> fields = new ArrayList<>(project.length);
        for (int[] path : project) {
            // Fluss only pushes down top-level projections for lake sources
            fields.add(schema.at(path[0]));
        }
        return new StructType(fields);
    }

    /** The partition values of the data file, as they appear in the Fluss partition name. */
    private static List<String> partition(
            Row scanFile, List<String> partitionColumns, StructType schema) {
        Map<String, String> values = InternalScanFileUtils.getPartitionValues(scanFile);
        List<String> partition = new ArrayList<>(partitionColumns.size());
        for (String column : partitionColumns) {
            partition.add(
                    DeltaPartitionLiterals.toFlussPartitionValue(
                            PartitionUtils.literalForPartitionValue(
                                    schema.get(column).getDataType(), values.get(column))));
        }
        return partition;
    }
}
