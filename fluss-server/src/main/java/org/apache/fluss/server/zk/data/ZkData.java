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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.types.Tuple2;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/** The data and path stored in ZooKeeper nodes (znodes). */
public final class ZkData {

    // ------------------------------------------------------------------------------------------
    // ZNodes under "/metadata/"
    // ------------------------------------------------------------------------------------------

    /**
     * The znode for databases. The znode path is:
     *
     * <p>/metadata/databases
     */
    public static final class DatabasesZNode {
        public static String path() {
            return "/metadata/databases";
        }
    }

    /**
     * The znode for a database. The znode path is:
     *
     * <p>/metadata/databases/[databaseName]
     */
    public static final class DatabaseZNode {
        public static String path(String databaseName) {
            return DatabasesZNode.path() + "/" + databaseName;
        }

        public static byte[] encode(DatabaseRegistration databaseRegistration) {
            return JsonSerdeUtils.writeValueAsBytes(
                    databaseRegistration, DatabaseRegistrationJsonSerde.INSTANCE);
        }

        public static DatabaseRegistration decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, DatabaseRegistrationJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode for tables. The znode path is:
     *
     * <p>/metadata/databases/[databaseName]/tables
     */
    public static final class TablesZNode {
        public static String path(String databaseName) {
            return DatabaseZNode.path(databaseName) + "/tables";
        }
    }

    /**
     * The znode for a table which stores the logical metadata of a table: schema (id), properties,
     * buckets, etc. The znode path is:
     *
     * <p>/metadata/databases/[databaseName]/tables/[tableName]
     */
    public static final class TableZNode {

        public static String path(TablePath tablePath) {
            return path(tablePath.getDatabaseName(), tablePath.getTableName());
        }

        public static String path(String databaseName, String tableName) {
            return TablesZNode.path(databaseName) + "/" + tableName;
        }

        /**
         * Extracts the database name and table name from the given zookeeper path. If the given
         * path is not a valid {@link TableZNode} path, returns null.
         */
        @Nullable
        public static TablePath parsePath(String zkPath) {
            String prefix = "/metadata/databases/";
            if (!zkPath.startsWith(prefix)) {
                return null;
            }
            String[] split = zkPath.substring(prefix.length()).split("/tables/");
            if (split.length != 2) {
                return null;
            }
            TablePath tablePath = TablePath.of(split[0], split[1]);
            return tablePath.isValid() ? tablePath : null;
        }

        public static byte[] encode(TableRegistration tableRegistration) {
            return JsonSerdeUtils.writeValueAsBytes(
                    tableRegistration, TableRegistrationJsonSerde.INSTANCE);
        }

        public static TableRegistration decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, TableRegistrationJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode for schemas of a table. The znode path is:
     *
     * <p>/metadata/databases/[databaseName]/tables/[tableName]/schemas
     */
    public static final class SchemasZNode {

        public static String path(TablePath tablePath) {
            return TableZNode.path(tablePath) + "/schemas";
        }
    }

    /**
     * The znode for a table which stores the schema information of a specific schema. The znode
     * path is:
     *
     * <p>/metadata/databases/[databaseName]/tables/[tableName]/schemas/[schemaId]
     */
    public static final class SchemaZNode {

        @Nullable
        public static Tuple2<TablePath, Integer> parsePath(String zkPath) {
            String splitter = "/schemas/";
            if (!zkPath.contains(splitter)) {
                return null;
            }

            String[] split = zkPath.split(splitter);
            if (split.length != 2) {
                return null;
            }
            TablePath tablePath = TableZNode.parsePath(split[0]);
            if (tablePath == null) {
                return null;
            }

            int schemaId;
            try {
                schemaId = Integer.parseInt(split[1]);
                return Tuple2.of(tablePath, schemaId);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        public static String path(TablePath tablePath, int schemaId) {
            return SchemasZNode.path(tablePath) + "/" + schemaId;
        }

        public static byte[] encode(Schema schema) {
            return schema.toJsonBytes();
        }

        public static Schema decode(byte[] json) {
            return Schema.fromJsonBytes(json);
        }
    }

    /**
     * The znode for a table which stores the partitions information. The znode path is:
     *
     * <p>/metadata/databases/[databaseName]/tables/[tableName]/partitions
     */
    public static final class PartitionsZNode {

        public static String path(TablePath tablePath) {
            return TableZNode.path(tablePath) + "/partitions";
        }
    }

    /**
     * The znode for a partition of a table. The znode path is:
     *
     * <p>/metadata/databases/[databaseName]/tables/[tableName]/partitions/[partitionName]
     */
    public static final class PartitionZNode {

        /**
         * Extracts the table path and the partition name from the given zookeeper path. If the
         * given path is not a valid {@link PartitionZNode} path, returns null. The partition name
         * of the returned {@link PhysicalTablePath} is will never be null.
         */
        @Nullable
        public static PhysicalTablePath parsePath(String zkPath) {
            String[] split = zkPath.split("/partitions/");
            if (split.length != 2) {
                return null;
            }
            TablePath tablePath = TableZNode.parsePath(split[0]);
            if (tablePath == null) {
                return null;
            }
            String partitionName = split[1];
            PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);
            if (physicalTablePath.isValid()) {
                return physicalTablePath;
            } else {
                return null;
            }
        }

        public static String path(TablePath tablePath, String partitionName) {
            return PartitionsZNode.path(tablePath) + "/" + partitionName;
        }

        public static byte[] encode(TablePartition partition) {
            return partition.toJsonBytes();
        }

        public static TablePartition decode(byte[] json) {
            return TablePartition.fromJsonBytes(json);
        }
    }

    /**
     * The znode used to generate a sequence unique id for a table. The znode path is:
     *
     * <p>/metadata/table_seqid
     */
    public static final class TableSequenceIdZNode {
        public static String path() {
            return "/metadata/table_seqid";
        }
    }

    /**
     * The znode for auto increment columns of a table. The znode path is:
     *
     * <p>/metadata/databases/[databaseName]/tables/[tableName]/auto_inc
     */
    public static final class AutoIncrementColumnsZNode {
        public static String path(TablePath tablePath) {
            return TableZNode.path(tablePath) + "/auto_inc";
        }
    }

    /**
     * The znode for auto increment column. The znode path is:
     *
     * <p>/metadata/databases/[databaseName]/tables/[tableName]/auto_inc/col_[columnId]
     */
    public static final class AutoIncrementColumnZNode {
        public static String path(TablePath tablePath, int columnId) {
            return AutoIncrementColumnsZNode.path(tablePath) + String.format("/col_%d", columnId);
        }
    }

    /**
     * The znode used to generate a sequence unique id for a partition. The znode path is:
     *
     * <p>/metadata/partition_seqid
     */
    public static final class PartitionSequenceIdZNode {
        public static String path() {
            return "/metadata/partition_seqid";
        }
    }

    /**
     * The znode used to generate a unique id for writer. The znode path is:
     *
     * <p>/metadata/writer_id
     */
    public static final class WriterIdZNode {
        public static String path() {
            return "/metadata/writer_id";
        }
    }

    // ------------------------------------------------------------------------------------------
    // ZNodes under "/coordinators/"
    // ------------------------------------------------------------------------------------------

    /**
     * The znode for alive coordinators. The znode path is:
     *
     * <p>/coordinators/ids
     */
    public static final class CoordinatorIdsZNode {
        public static String path() {
            return "/coordinators/ids";
        }
    }

    /**
     * The znode for a registered Coordinator information. The znode path is:
     *
     * <p>/coordinators/ids/[serverId]
     */
    public static final class CoordinatorIdZNode {
        public static String path(int serverId) {
            return CoordinatorIdsZNode.path() + "/" + serverId;
        }
    }

    /**
     * The znode for the coordinator leader election. The znode path is:
     *
     * <p>/coordinators/election
     */
    public static final class CoordinatorElectionZNode {
        public static String path() {
            return "/coordinators/election";
        }
    }

    /**
     * The znode for the active coordinator leader. The znode path is:
     *
     * <p>/coordinators/leader
     */
    public static final class CoordinatorLeaderZNode {
        public static String path() {
            return "/coordinators/active";
        }

        public static byte[] encode(CoordinatorAddress coordinatorAddress) {
            return JsonSerdeUtils.writeValueAsBytes(
                    coordinatorAddress, CoordinatorAddressJsonSerde.INSTANCE);
        }

        public static CoordinatorAddress decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, CoordinatorAddressJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode for the coordinator epoch. The znode path is:
     *
     * <p>/coordinators/epoch
     */
    public static final class CoordinatorEpochZNode {
        public static String path() {
            return "/coordinators/epoch";
        }

        public static byte[] encode(int epoch) {
            return String.valueOf(epoch).getBytes();
        }

        public static int decode(byte[] bytes) {
            return Integer.parseInt(new String(bytes));
        }
    }
    // ------------------------------------------------------------------------------------------
    // ZNodes under "/tabletservers/"
    // ------------------------------------------------------------------------------------------

    /**
     * The znode can be used to generate a sequence unique id for a TabletServer. The znode path is:
     *
     * <p>/tabletservers/seqid
     */
    public static final class ServerSequenceIdZNode {
        public static String path() {
            return "/tabletservers/seqid";
        }
    }

    /**
     * The znode for TabletServers. The znode path is:
     *
     * <p>/tabletservers/ids
     */
    public static final class ServerIdsZNode {
        public static String path() {
            return "/tabletservers/ids";
        }
    }

    /**
     * The znode for a registered TabletServer information. The znode path is:
     *
     * <p>/tabletservers/ids/[serverId]
     */
    public static final class ServerIdZNode {
        public static String path(int serverId) {
            return ServerIdsZNode.path() + "/" + serverId;
        }

        public static byte[] encode(TabletServerRegistration tsRegistration) {
            return JsonSerdeUtils.writeValueAsBytes(
                    tsRegistration, TabletServerRegistrationJsonSerde.INSTANCE);
        }

        public static TabletServerRegistration decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, TabletServerRegistrationJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode for table ids which list all the registered table ids. The znode path is:
     *
     * <p>/tabletservers/tables
     */
    public static final class TableIdsZNode {
        public static String path() {
            return "/tabletservers/tables";
        }
    }

    /**
     * The znode for table ids which list all the registered table ids. The znode path is:
     *
     * <p>/tabletservers/partitions
     */
    public static final class PartitionIdsZNode {
        public static String path() {
            return "/tabletservers/partitions";
        }
    }

    /**
     * The znode for a table id which stores the assignment and location of a table. The table id is
     * generated by {@link TableSequenceIdZNode} and is guaranteed to be unique in the cluster. The
     * znode path is:
     *
     * <p>/tabletservers/tables/[tableId]
     */
    public static final class TableIdZNode {
        public static String path(long tableId) {
            return TableIdsZNode.path() + "/" + tableId;
        }

        public static byte[] encode(TableAssignment tableAssignment) {
            return JsonSerdeUtils.writeValueAsBytes(
                    tableAssignment, TableAssignmentJsonSerde.INSTANCE);
        }

        public static TableAssignment decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, TableAssignmentJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode for a partition id which stores the assignment and location of the partition. The
     * partition id is generated by {@link TableSequenceIdZNode} and is guaranteed to be unique in
     * the cluster. The znode path is:
     *
     * <p>/tabletservers/partitions/[partitionId]
     */
    public static final class PartitionIdZNode {
        public static String path(long partitionId) {
            return PartitionIdsZNode.path() + "/" + partitionId;
        }

        public static byte[] encode(PartitionAssignment tableAssignment) {
            return JsonSerdeUtils.writeValueAsBytes(
                    tableAssignment, PartitionAssignmentJsonSerde.INSTANCE);
        }

        public static PartitionAssignment decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, PartitionAssignmentJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode for buckets of a table/partition.
     *
     * <p>For a table, the znode path is:
     *
     * <p>/tabletservers/tables/[tableId]/buckets
     *
     * <p>For a partition, the znode path is:
     *
     * <p>/tabletservers/partitions/[partitionId]/buckets
     */
    public static final class BucketIdsZNode {
        public static String pathOfTable(long tableId) {
            return TableIdZNode.path(tableId) + "/buckets";
        }

        public static String pathOfPartition(long partitionId) {
            return PartitionIdZNode.path(partitionId) + "/buckets";
        }

        public static String path(TableBucket tableBucket) {
            if (tableBucket.getPartitionId() != null) {
                return PartitionIdZNode.path(tableBucket.getPartitionId()) + "/buckets";
            } else {
                return TableIdZNode.path(tableBucket.getTableId()) + "/buckets";
            }
        }
    }

    /**
     * The znode for the leadership and isr information of a bucket of a table or partition.
     *
     * <p>For a table, the znode path is:
     *
     * <p>/tabletservers/tables/[tableId]/buckets/[bucketId]/leader_isr
     *
     * <p>For a partition, the znode path is:
     *
     * <p>/tabletservers/partitions/[partitionId]/buckets/[bucketId]/leader_isr
     */
    public static final class LeaderAndIsrZNode {
        public static String path(TableBucket tableBucket) {
            return BucketIdZNode.path(tableBucket) + "/leader_isr";
        }

        public static byte[] encode(LeaderAndIsr leaderAndIsr) {
            return JsonSerdeUtils.writeValueAsBytes(leaderAndIsr, LeaderAndIsrJsonSerde.INSTANCE);
        }

        public static LeaderAndIsr decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, LeaderAndIsrJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode for a bucket of a table or partition.
     *
     * <p>For a table, the znode path is:
     *
     * <p>/tabletservers/tables/[tableId]/buckets/[bucketId]
     *
     * <p>For a partition, the znode path is:
     *
     * <p>/tabletservers/partitions/[partitionId]/buckets/[bucketId]
     */
    public static final class BucketIdZNode {
        public static String path(TableBucket tableBucket) {
            return BucketIdsZNode.path(tableBucket) + "/" + tableBucket.getBucket();
        }
    }

    /**
     * The znode for the snapshots of a bucket of a table or partition.
     *
     * <p>For a table, the znode path is:
     *
     * <p>/tabletservers/tables/[tableId]/buckets/[bucketId]/snapshots
     *
     * <p>For a partition, the znode path is:
     *
     * <p>/tabletservers/partitions/[partitionId]/buckets/[bucketId]/snapshots
     */
    public static final class BucketSnapshotsZNode {
        public static String path(TableBucket tableBucket) {
            return BucketIdZNode.path(tableBucket) + "/snapshots";
        }
    }

    /**
     * The znode for the one snapshot of a bucket of a table or partition.
     *
     * <p>For a table, the znode path is:
     *
     * <p>/tabletservers/tables/[tableId]/buckets/[bucketId]/snapshots/[snapshotId]
     *
     * <p>For a partition, the znode path is:
     *
     * <p>/tabletservers/partitions/[partitionId]/buckets/[bucketId]/snapshots/[snapshotId]
     */
    public static final class BucketSnapshotIdZNode {

        public static String path(TableBucket tableBucket, long snapshotId) {
            return BucketSnapshotsZNode.path(tableBucket) + "/" + snapshotId;
        }

        public static byte[] encode(BucketSnapshot bucketSnapshot) {
            return JsonSerdeUtils.writeValueAsBytes(
                    bucketSnapshot, BucketSnapshotJsonSerde.INSTANCE);
        }

        public static BucketSnapshot decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, BucketSnapshotJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode used to generate a sequence unique id for a snapshot of a table bucket.
     *
     * <p>For bucket belongs to a table, the znode path is:
     *
     * <p>/tabletservers/tables/[tableId]/buckets/[bucketId]/snapshot_seqid
     *
     * <p>For bucket belongs to a table partition, the znode path is:
     *
     * <p>/tabletservers/partitions/[partitionId]/buckets/[bucketId]/snapshot_seqid
     */
    public static final class BucketSnapshotSequenceIdZNode {

        public static String path(TableBucket tableBucket) {
            return BucketIdZNode.path(tableBucket) + "/snapshot_seqid";
        }
    }

    /**
     * The znode for the remote logs' path of a table bucket. The znode path is:
     *
     * <p>/tabletservers/tables/[tableId]/buckets/[bucketId]/remote_logs
     */
    public static final class BucketRemoteLogsZNode {
        public static String path(TableBucket tableBucket) {
            return BucketIdZNode.path(tableBucket) + "/remote_logs";
        }

        public static byte[] encode(RemoteLogManifestHandle remoteLogManifestHandle) {
            return JsonSerdeUtils.writeValueAsBytes(
                    remoteLogManifestHandle, RemoteLogManifestHandleJsonSerde.INSTANCE);
        }

        public static RemoteLogManifestHandle decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, RemoteLogManifestHandleJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode for lake table snapshot information. The znode path is:
     *
     * <p>/tabletservers/tables/[tableId]/laketable
     *
     * <p>This znode stores {@link LakeTable} in:
     *
     * <ul>
     *   <li>Version 1 (legacy): Full snapshot data stored directly in ZK
     *   <li>Version 2 (current): A list of snapshot metadata, with metadata file path stored in ZK,
     *       actual data in remote file
     * </ul>
     */
    public static final class LakeTableZNode {
        /**
         * Returns the ZK path for the lake table znode of the given table.
         *
         * @param tableId the table ID
         * @return the ZK path
         */
        public static String path(long tableId) {
            return TableIdZNode.path(tableId) + "/laketable";
        }

        /**
         * Encodes a LakeTable to JSON bytes for storage in ZK.
         *
         * @param lakeTable the LakeTable to encode
         * @return the encoded bytes
         */
        public static byte[] encode(LakeTable lakeTable) {
            return JsonSerdeUtils.writeValueAsBytes(lakeTable, LakeTableJsonSerde.INSTANCE);
        }

        /**
         * Decodes JSON bytes from ZK to a LakeTable.
         *
         * <p>This method handles both version 1 (legacy) and version 2 (current) formats
         * automatically through {@link LakeTableJsonSerde}.
         *
         * @param json the JSON bytes from ZK
         * @return the decoded LakeTable
         */
        public static LakeTable decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, LakeTableJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode for server tags. The znode path is:
     *
     * <p>/tabletServers/server_tags
     */
    public static final class ServerTagsZNode {
        public static String path() {
            return "/tabletservers/server_tags";
        }

        public static byte[] encode(ServerTags serverTag) {
            return JsonSerdeUtils.writeValueAsBytes(serverTag, ServerTagsJsonSerde.INSTANCE);
        }

        public static ServerTags decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, ServerTagsJsonSerde.INSTANCE);
        }
    }

    // ------------------------------------------------------------------------------------------
    // ZNodes for ACL(Access Control List).
    // ------------------------------------------------------------------------------------------
    /**
     * The root znode for ACLs. This is the top-level node under which all resource-specific ACLs
     * are stored. The znode path is:
     *
     * <p>/fluss-acls
     */
    public static final class AclRootNode {
        public static String path() {
            return "/fluss-acls";
        }
    }

    /**
     * The znode for a specific resource's ACL. Each resource type and name maps to a unique ACL
     * znode under the root ACL node.
     *
     * <p>The znode path follows this structure:
     *
     * <p>/fluss-acls/[resourceType]/[resourceName]
     */
    public static final class ResourceAclNode {

        public static String path(ResourceType resourceType) {
            return AclRootNode.path() + "/" + resourceType;
        }

        /**
         * Returns the path of the ACL znode for a specific resource. The znode path is:
         *
         * <p>/fluss-acls/[resourceType]/[resourceName]
         */
        public static String path(Resource resource) {
            return AclRootNode.path() + "/" + resource.getType() + "/" + resource.getName();
        }

        public static byte[] encode(ResourceAcl resourceAcl) {
            return JsonSerdeUtils.writeValueAsBytes(resourceAcl, ResourceAclJsonSerde.INSTANCE);
        }

        public static ResourceAcl decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, ResourceAclJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode for tracking ACL changes in the system. This znode serves as a root node for all
     * ACL change notifications. The znode path is:
     *
     * <p>/fluss-acl-changes
     */
    public static final class AclChangesNode {
        public static String path() {
            return "/fluss-acl-changes";
        }
    }

    /**
     * The znode for individual ACL change notifications. Each notification is stored as a
     * sequential child node under the {@link AclChangesNode} with a prefix. The znode path follows
     * this structure:
     *
     * <p>/fluss-acl-changes/acl_changes_[sequenceNumber]
     */
    public static final class AclChangeNotificationNode {
        private static final String SEQUENT_NUMBER_PREFIX = "acl_changes_";
        private static final String RESOURCE_SEPARATOR = ":";

        public static String pathPrefix() {
            return AclChangesNode.path() + "/" + SEQUENT_NUMBER_PREFIX;
        }

        public static String prefix() {
            return SEQUENT_NUMBER_PREFIX;
        }

        public static byte[] encode(Resource resource) {
            return (resource.getType() + RESOURCE_SEPARATOR + resource.getName())
                    .getBytes(StandardCharsets.UTF_8);
        }

        public static Resource decode(byte[] json) {
            String resourceStr = new String(json, StandardCharsets.UTF_8);
            String[] split = resourceStr.split(RESOURCE_SEPARATOR);
            if (split.length == 2) {
                return Resource.of(split[0], split[1]);
            } else {
                throw new IllegalArgumentException(
                        "expected a string in format ResourceType:ResourceName but got "
                                + resourceStr);
            }
        }
    }

    /**
     * The znode for the dynamic configs. The znode path is:
     *
     * <p>/config
     */
    public static final class ConfigZNode {
        public static String path() {
            return "/config";
        }
    }

    /**
     * The znode for a specific config entity. The znode path is:
     *
     * <p>/config/[entityType]/[entityName]
     */
    public static final class ConfigEntityZNode {
        public static final String ENTITY_TYPE = "server";
        public static final String ENTITY_NAME = "global";

        public static String path() {
            return ConfigZNode.path() + "/" + ENTITY_TYPE + "/" + ENTITY_NAME;
        }

        public static byte[] encode(Map<String, String> properties) {
            return JsonSerdeUtils.writeValueAsBytes(properties, ConfigJsonSerde.INSTANCE);
        }

        public static Map<String, String> decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, ConfigJsonSerde.INSTANCE);
        }
    }

    /**
     * The znode for tracking dynamic config entity changes. This znode serves as a root node for
     * all config entity change notifications. The znode path is:
     *
     * <p>/config/changes
     */
    public static final class ConfigEntityChangeNotificationZNode {
        public static String path() {
            return ConfigZNode.path() + "/changes";
        }
    }

    /**
     * The znode for individual entity changes change notifications. Each notification is stored as
     * a sequential child node under the {@link ConfigEntityChangeNotificationZNode} with a prefix.
     * The znode path follows this structure:
     *
     * <p>/config/changes/config_changes_[sequenceNumber]
     */
    public static final class ConfigEntityChangeNotificationSequenceZNode {
        private static final String SEQUENT_NUMBER_PREFIX = "config_change_";

        public static String pathPrefix() {
            return ConfigEntityChangeNotificationZNode.path() + "/" + SEQUENT_NUMBER_PREFIX;
        }

        public static String prefix() {
            return SEQUENT_NUMBER_PREFIX;
        }

        public static byte[] encode() {
            return new byte[0];
        }
    }

    // ------------------------------------------------------------------------------------------
    // ZNodes under "/cluster/"
    // ------------------------------------------------------------------------------------------

    /**
     * The znode for rebalance. The znode path is:
     *
     * <p>/cluster/rebalance
     */
    public static final class RebalanceZNode {
        public static String path() {
            return "/cluster/rebalance";
        }

        public static byte[] encode(RebalanceTask rebalanceTask) {
            return JsonSerdeUtils.writeValueAsBytes(rebalanceTask, RebalanceTaskJsonSerde.INSTANCE);
        }

        public static RebalanceTask decode(byte[] json) {
            return JsonSerdeUtils.readValue(json, RebalanceTaskJsonSerde.INSTANCE);
        }
    }
}
