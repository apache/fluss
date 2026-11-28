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

package org.apache.fluss.flink.action.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.action.Action;
import org.apache.fluss.flink.utils.DataLakeUtils;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;

import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Loads a Paimon snapshot into a newly created Fluss primary-key table. The caller creates the
 * Fluss metadata before submission and prevents ordinary Fluss writes until initialization
 * completes, including across resumptions. The job commits the load, registers that same source
 * snapshot and enables lake access before completing.
 */
@Internal
public class LoadLakeDataToFlussAction implements Action {

    private final Configuration flussConfig;
    private final Configuration paimonConfig;
    private final TablePath tablePath;
    @Nullable private final Long sourceSnapshotId;
    @Nullable private final String bulkLoadId;
    @Nullable private final Long expectedTableId;

    /**
     * Creates a load action without creating or altering the target table. A null snapshot ID
     * selects the latest Paimon snapshot once at submission. Paimon options override catalog
     * properties stored in the Fluss table; Fluss options include writable remote-filesystem
     * credentials. The job selects partitions using the table's load policy and uses Flink's
     * configured parallelism for the bucket-building operator. If the source has no snapshot, the
     * job only enables lake access.
     *
     * <p>The snapshot ID and Bulk Load ID must either both be null or both be provided. When
     * omitted, the action selects the latest snapshot and generates an ID once before Begin.
     * Existing transactions reuse their frozen target partitions. An enabled table needs no further
     * initialization.
     *
     * <p>When reusing a Bulk Load ID, the caller must persist and reuse the original source
     * snapshot. The SDK does not store the Paimon snapshot associated with that ID; argument
     * validation here only checks that the parameters are supplied together. The caller must stop
     * all old writers before resubmitting.
     */
    public LoadLakeDataToFlussAction(
            Configuration flussConfig,
            Configuration paimonConfig,
            TablePath tablePath,
            @Nullable Long sourceSnapshotId,
            @Nullable String bulkLoadId,
            @Nullable Long expectedTableId) {
        this.flussConfig = new Configuration(checkNotNull(flussConfig));
        this.paimonConfig = new Configuration(checkNotNull(paimonConfig));
        this.tablePath = checkNotNull(tablePath);
        tablePath.validate();
        checkArgument(
                sourceSnapshotId == null || sourceSnapshotId > 0,
                "The source snapshot ID must be positive.");
        this.sourceSnapshotId = sourceSnapshotId;
        checkArgument(
                bulkLoadId == null || !bulkLoadId.trim().isEmpty(),
                "The Bulk Load ID must not be empty.");
        checkArgument(
                expectedTableId == null || expectedTableId >= 0,
                "The expected table ID must not be negative.");
        checkArgument(
                (sourceSnapshotId == null) == (bulkLoadId == null),
                "The source snapshot ID and Bulk Load ID must be supplied together.");
        this.bulkLoadId = bulkLoadId;
        this.expectedTableId = expectedTableId;
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        JobClient job = submit(environment);
        if (environment.getConfiguration().get(DeploymentOptions.ATTACHED)) {
            job.getJobExecutionResult().get();
        }
    }

    /** Submits the bounded load job; callers may wait for or independently track its JobID. */
    public JobClient submit(StreamExecutionEnvironment environment) throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Admin admin = connection.getAdmin();
                Table target = connection.getTable(tablePath)) {
            TableInfo tableInfo = target.getTableInfo();
            checkState(
                    expectedTableId == null || expectedTableId == tableInfo.getTableId(),
                    "The original Fluss table was replaced.");
            checkArgument(tableInfo.hasPrimaryKey(), "Loading requires a primary-key Fluss table.");
            checkArgument(
                    tableInfo.getTableConfig().getDataLakeFormat().orElse(null)
                            == DataLakeFormat.PAIMON,
                    "Loading currently supports only Paimon lake tables.");
            String fixedBulkLoadId = bulkLoadId == null ? UUID.randomUUID().toString() : bulkLoadId;

            Configuration lakeConfig =
                    Configuration.fromMap(
                            DataLakeUtils.extractLakeCatalogProperties(tableInfo.getProperties()));
            lakeConfig.addAll(paimonConfig);
            LakeStorage lakeStorage =
                    LakeStoragePluginSetUp.fromDataLakeFormat(
                                    DataLakeFormat.PAIMON.toString(), null)
                            .createLakeStorage(lakeConfig);
            Long snapshotId;
            if (sourceSnapshotId != null) {
                snapshotId = sourceSnapshotId;
            } else {
                try (LakeCatalog catalog = lakeStorage.createLakeCatalog()) {
                    snapshotId =
                            catalog.getLatestSnapshotId(tableInfo.getLakeTablePath()).orElse(null);
                }
            }
            return LoadLakeToFluss.submit(
                    environment,
                    flussConfig,
                    admin,
                    target.newBulkLoad(),
                    tableInfo,
                    lakeStorage.createLakeSource(tableInfo.getLakeTablePath()),
                    snapshotId,
                    fixedBulkLoadId);
        }
    }
}
