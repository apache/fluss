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

package org.apache.fluss.server.coordinator.system;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SystemTableConstants;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Provider for the {@code sys.table_buckets} system view.
 *
 * <p>This view exposes per-bucket metadata for all tables in the cluster, including replica
 * assignments, leader info, and ISR distribution. Data is fetched from ZooKeeper.
 *
 * <p>Supports filter pushdown on {@code database_name} and {@code table_name} to narrow the scope
 * of ZK queries.
 *
 * <p>Schema:
 *
 * <ul>
 *   <li>{@code database_name} (STRING) — database name
 *   <li>{@code table_name} (STRING) — table name
 *   <li>{@code partition_name} (STRING, nullable) — partition name, NULL for non-partitioned tables
 *   <li>{@code bucket_id} (INT) — bucket ID
 *   <li>{@code leader_id} (INT, nullable) — leader tablet server ID
 *   <li>{@code replicas} (STRING) — comma-separated replica server IDs
 *   <li>{@code isr} (STRING, nullable) — comma-separated ISR server IDs
 *   <li>{@code replica_count} (INT) — total number of replicas
 *   <li>{@code isr_count} (INT, nullable) — number of in-sync replicas
 * </ul>
 */
@Internal
public class TableBucketsViewProvider implements SystemViewProvider {

    static final int MAX_ROWS_LIMIT = 10_000;

    private static final int NUM_COLUMNS = 9;

    private static final TablePath VIEW_PATH =
            TablePath.of(
                    SystemTableConstants.SYSTEM_DATABASE, SystemTableConstants.TABLE_BUCKETS_VIEW);

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("database_name", DataTypes.STRING())
                    .column("table_name", DataTypes.STRING())
                    .column("partition_name", DataTypes.STRING())
                    .column("bucket_id", DataTypes.INT())
                    .column("leader_id", DataTypes.INT())
                    .column("replicas", DataTypes.STRING())
                    .column("isr", DataTypes.STRING())
                    .column("replica_count", DataTypes.INT())
                    .column("isr_count", DataTypes.INT())
                    .build();

    private final ZooKeeperClient zkClient;

    public TableBucketsViewProvider(ZooKeeperClient zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public TablePath viewPath() {
        return VIEW_PATH;
    }

    @Override
    public Schema schema() {
        return SCHEMA;
    }

    @Override
    public int schemaId() {
        return 2;
    }

    @Override
    public byte[] scanRows(@Nullable int[] projectedFields, @Nullable Predicate filterPredicate)
            throws Exception {
        // Step 0: Extract filter pushdown conditions.
        String filterDbName = null;
        String filterTableName = null;

        List<Predicate> conjuncts = PredicateBuilder.splitAnd(filterPredicate);
        for (Predicate p : conjuncts) {
            if (p instanceof LeafPredicate) {
                LeafPredicate leaf = (LeafPredicate) p;
                if (leaf.function() instanceof Equal) {
                    switch (leaf.fieldName()) {
                        case "database_name":
                            filterDbName = ((BinaryString) leaf.literals().get(0)).toString();
                            break;
                        case "table_name":
                            filterTableName = ((BinaryString) leaf.literals().get(0)).toString();
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        boolean enforceRowLimit = (filterTableName == null);

        // Step 1: List databases and tables (narrowed by filter).
        // Collect table metadata: tablePath -> TableRegistration
        Map<TablePath, TableRegistration> matchedTables = new HashMap<>();
        List<String> databases;
        if (filterDbName != null) {
            databases = Collections.singletonList(filterDbName);
        } else {
            databases = zkClient.listDatabases();
        }

        for (String database : databases) {
            if (SystemTableConstants.isSystemDatabase(database)) {
                continue;
            }
            List<String> tableNames;
            if (filterTableName != null) {
                tableNames = Collections.singletonList(filterTableName);
            } else {
                tableNames = zkClient.listTables(database);
            }

            for (String tableName : tableNames) {
                TablePath tablePath = TablePath.of(database, tableName);
                Optional<TableRegistration> optReg = zkClient.getTable(tablePath);
                if (!optReg.isPresent()) {
                    continue;
                }
                TableRegistration reg = optReg.get();
                matchedTables.put(tablePath, reg);
            }
        }

        if (matchedTables.isEmpty()) {
            return SystemViewProvider.serializeRows(
                    new ArrayList<>(), projectSchema(projectedFields), schemaId());
        }

        // Step 2: Classify tables into non-partitioned and partitioned.
        List<TablePath> nonPartitionedPaths = new ArrayList<>();
        List<Long> nonPartitionedIds = new ArrayList<>();
        List<TablePath> partitionedPaths = new ArrayList<>();

        for (Map.Entry<TablePath, TableRegistration> entry : matchedTables.entrySet()) {
            if (entry.getValue().isPartitioned()) {
                partitionedPaths.add(entry.getKey());
            } else {
                nonPartitionedPaths.add(entry.getKey());
                nonPartitionedIds.add(entry.getValue().tableId);
            }
        }

        List<InternalRow> rows = new ArrayList<>();

        // Step 3: Fetch bucket metadata for non-partitioned tables.
        if (!nonPartitionedIds.isEmpty()) {
            Map<Long, TableAssignment> tableAssignments =
                    zkClient.getTablesAssignments(nonPartitionedIds);

            List<TableBucket> tableBuckets = new ArrayList<>();
            tableAssignments.forEach(
                    (tableId, assignment) -> {
                        for (Integer bucketId : assignment.getBuckets()) {
                            tableBuckets.add(new TableBucket(tableId, bucketId));
                        }
                    });

            Map<TableBucket, LeaderAndIsr> leaderAndIsrs = zkClient.getLeaderAndIsrs(tableBuckets);

            for (TablePath tablePath : nonPartitionedPaths) {
                TableRegistration reg = matchedTables.get(tablePath);
                TableAssignment assignment = tableAssignments.get(reg.tableId);
                if (assignment == null) {
                    continue;
                }
                for (Map.Entry<Integer, BucketAssignment> bucketEntry :
                        assignment.getBucketAssignments().entrySet()) {
                    int bucketId = bucketEntry.getKey();
                    List<Integer> replicas = bucketEntry.getValue().getReplicas();
                    TableBucket tb = new TableBucket(reg.tableId, bucketId);
                    LeaderAndIsr leaderAndIsr = leaderAndIsrs.get(tb);

                    GenericRow row =
                            buildRow(
                                    tablePath.getDatabaseName(),
                                    tablePath.getTableName(),
                                    null,
                                    bucketId,
                                    replicas,
                                    leaderAndIsr);

                    if (filterPredicate == null || filterPredicate.test(row)) {
                        checkRowLimit(enforceRowLimit, rows.size());
                        rows.add(projectRow(row, projectedFields));
                    }
                }
            }
        }

        // Step 4: Fetch bucket metadata for partitioned tables.
        if (!partitionedPaths.isEmpty()) {
            // Collect all partition IDs and their names across all partitioned tables.
            // partitionId -> (tablePath, partitionName)
            Map<Long, TablePath> partitionIdToTablePath = new HashMap<>();
            Map<Long, String> partitionIdToName = new HashMap<>();

            for (TablePath tablePath : partitionedPaths) {
                Map<Long, String> partitions = zkClient.getPartitionIdAndNames(tablePath);
                for (Map.Entry<Long, String> entry : partitions.entrySet()) {
                    partitionIdToTablePath.put(entry.getKey(), tablePath);
                    partitionIdToName.put(entry.getKey(), entry.getValue());
                }
            }

            if (!partitionIdToName.isEmpty()) {
                Map<Long, PartitionAssignment> partitionAssignments =
                        zkClient.getPartitionsAssignments(partitionIdToName.keySet());

                List<TableBucket> partitionBuckets = new ArrayList<>();
                partitionAssignments.forEach(
                        (partitionId, assignment) -> {
                            for (Integer bucketId : assignment.getBuckets()) {
                                partitionBuckets.add(
                                        new TableBucket(
                                                assignment.getTableId(), partitionId, bucketId));
                            }
                        });

                Map<TableBucket, LeaderAndIsr> leaderAndIsrs =
                        zkClient.getLeaderAndIsrs(partitionBuckets);

                for (Map.Entry<Long, PartitionAssignment> paEntry :
                        partitionAssignments.entrySet()) {
                    long partitionId = paEntry.getKey();
                    PartitionAssignment assignment = paEntry.getValue();
                    TablePath tablePath = partitionIdToTablePath.get(partitionId);
                    String partitionName = partitionIdToName.get(partitionId);
                    TableRegistration reg = matchedTables.get(tablePath);

                    for (Map.Entry<Integer, BucketAssignment> bucketEntry :
                            assignment.getBucketAssignments().entrySet()) {
                        int bucketId = bucketEntry.getKey();
                        List<Integer> replicas = bucketEntry.getValue().getReplicas();
                        TableBucket tb = new TableBucket(reg.tableId, partitionId, bucketId);
                        LeaderAndIsr leaderAndIsr = leaderAndIsrs.get(tb);

                        GenericRow row =
                                buildRow(
                                        tablePath.getDatabaseName(),
                                        tablePath.getTableName(),
                                        partitionName,
                                        bucketId,
                                        replicas,
                                        leaderAndIsr);

                        if (filterPredicate == null || filterPredicate.test(row)) {
                            checkRowLimit(enforceRowLimit, rows.size());
                            rows.add(projectRow(row, projectedFields));
                        }
                    }
                }
            }
        }

        return SystemViewProvider.serializeRows(rows, projectSchema(projectedFields), schemaId());
    }

    private static GenericRow buildRow(
            String databaseName,
            String tableName,
            @Nullable String partitionName,
            int bucketId,
            List<Integer> replicas,
            @Nullable LeaderAndIsr leaderAndIsr) {
        GenericRow row = new GenericRow(NUM_COLUMNS);
        row.setField(0, BinaryString.fromString(databaseName));
        row.setField(1, BinaryString.fromString(tableName));
        row.setField(2, partitionName != null ? BinaryString.fromString(partitionName) : null);
        row.setField(3, bucketId);

        if (leaderAndIsr != null) {
            int leader = leaderAndIsr.leader();
            row.setField(4, leader == LeaderAndIsr.NO_LEADER ? null : leader);
        } else {
            row.setField(4, null);
        }

        row.setField(5, BinaryString.fromString(joinInts(replicas)));
        row.setField(
                6,
                leaderAndIsr != null
                        ? BinaryString.fromString(joinInts(leaderAndIsr.isr()))
                        : null);
        row.setField(7, replicas.size());
        row.setField(8, leaderAndIsr != null ? leaderAndIsr.isr().size() : null);

        return row;
    }

    private static void checkRowLimit(boolean enforceRowLimit, int currentSize) {
        if (enforceRowLimit && currentSize >= MAX_ROWS_LIMIT) {
            throw new FlussRuntimeException(
                    "System view 'sys.table_buckets' result exceeds maximum row limit ("
                            + MAX_ROWS_LIMIT
                            + "). Please add a filter condition on table_name"
                            + " to narrow the query scope.");
        }
    }

    private static String joinInts(List<Integer> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(values.get(i));
        }
        return sb.toString();
    }

    private Schema projectSchema(@Nullable int[] projectedFields) {
        if (projectedFields == null) {
            return SCHEMA;
        }
        Schema.Builder builder = Schema.newBuilder();
        List<Schema.Column> columns = SCHEMA.getColumns();
        for (int idx : projectedFields) {
            Schema.Column col = columns.get(idx);
            builder.column(col.getName(), col.getDataType());
        }
        return builder.build();
    }

    private static InternalRow projectRow(GenericRow fullRow, @Nullable int[] projectedFields) {
        if (projectedFields == null) {
            return fullRow;
        }
        GenericRow projected = new GenericRow(projectedFields.length);
        for (int i = 0; i < projectedFields.length; i++) {
            projected.setField(i, fullRow.getField(projectedFields[i]));
        }
        return projected;
    }
}
