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

package org.apache.fluss.cli.sql.ast;

import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Container class for all Fluss-specific SQL statement AST nodes.
 *
 * <p>This class holds all statement node definitions as public static nested classes, allowing them
 * to be accessed from other packages while keeping them organized in a single file.
 */
public final class FlussStatementNodes {

    private FlussStatementNodes() {}

    /** SHOW DATABASES statement. */
    public static class ShowDatabasesStatement extends FlussStatement {
        public ShowDatabasesStatement(String originalSql) {
            super(originalSql);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowDatabases(this, context);
        }
    }

    /** SHOW DATABASE EXISTS statement. */
    public static class ShowDatabaseExistsStatement extends FlussStatement {
        private final String databaseName;

        public ShowDatabaseExistsStatement(String originalSql, String databaseName) {
            super(originalSql);
            this.databaseName = databaseName;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowDatabaseExists(this, context);
        }
    }

    /** SHOW DATABASE INFO statement. */
    public static class ShowDatabaseInfoStatement extends FlussStatement {
        private final String databaseName;

        public ShowDatabaseInfoStatement(String originalSql, String databaseName) {
            super(originalSql);
            this.databaseName = databaseName;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowDatabaseInfo(this, context);
        }
    }

    /** SHOW TABLES statement. */
    public static class ShowTablesStatement extends FlussStatement {
        private final String databaseName;

        public ShowTablesStatement(String originalSql, Optional<String> databaseName) {
            super(originalSql);
            this.databaseName = databaseName.orElse(null);
        }

        public Optional<String> getDatabaseName() {
            return Optional.ofNullable(databaseName);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowTables(this, context);
        }
    }

    /** SHOW TABLE EXISTS statement. */
    public static class ShowTableExistsStatement extends FlussStatement {
        private final TablePath tablePath;

        public ShowTableExistsStatement(String originalSql, TablePath tablePath) {
            super(originalSql);
            this.tablePath = tablePath;
        }

        public TablePath getTablePath() {
            return tablePath;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowTableExists(this, context);
        }
    }

    /** SHOW TABLE SCHEMA statement. */
    public static class ShowTableSchemaStatement extends FlussStatement {
        private final TablePath tablePath;
        private final Integer schemaId;

        public ShowTableSchemaStatement(
                String originalSql, TablePath tablePath, @Nullable Integer schemaId) {
            super(originalSql);
            this.tablePath = tablePath;
            this.schemaId = schemaId;
        }

        public TablePath getTablePath() {
            return tablePath;
        }

        public Optional<Integer> getSchemaId() {
            return Optional.ofNullable(schemaId);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowTableSchema(this, context);
        }
    }

    /** SHOW SERVERS statement. */
    public static class ShowServersStatement extends FlussStatement {
        public ShowServersStatement(String originalSql) {
            super(originalSql);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowServers(this, context);
        }
    }

    /** USE DATABASE statement. */
    public static class UseDatabaseStatement extends FlussStatement {
        private final String databaseName;

        public UseDatabaseStatement(String originalSql, String databaseName) {
            super(originalSql);
            this.databaseName = databaseName;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitUseDatabase(this, context);
        }
    }

    /** DESCRIBE TABLE statement. */
    public static class DescribeTableStatement extends FlussStatement {
        private final TablePath tablePath;

        public DescribeTableStatement(String originalSql, TablePath tablePath) {
            super(originalSql);
            this.tablePath = tablePath;
        }

        public TablePath getTablePath() {
            return tablePath;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitDescribeTable(this, context);
        }
    }

    /** SHOW CREATE TABLE statement. */
    public static class ShowCreateTableStatement extends FlussStatement {
        private final TablePath tablePath;

        public ShowCreateTableStatement(String originalSql, TablePath tablePath) {
            super(originalSql);
            this.tablePath = tablePath;
        }

        public TablePath getTablePath() {
            return tablePath;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowCreateTable(this, context);
        }
    }

    /** SHOW PARTITIONS statement. */
    public static class ShowPartitionsStatement extends FlussStatement {
        private final TablePath tablePath;
        private final PartitionSpec partitionSpec;

        public ShowPartitionsStatement(
                String originalSql, TablePath tablePath, @Nullable PartitionSpec partitionSpec) {
            super(originalSql);
            this.tablePath = tablePath;
            this.partitionSpec = partitionSpec;
        }

        public TablePath getTablePath() {
            return tablePath;
        }

        public Optional<PartitionSpec> getPartitionSpec() {
            return Optional.ofNullable(partitionSpec);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowPartitions(this, context);
        }
    }

    /** SHOW KV SNAPSHOTS statement. */
    public static class ShowKvSnapshotsStatement extends FlussStatement {
        private final TablePath tablePath;
        private final PartitionSpec partitionSpec;

        public ShowKvSnapshotsStatement(
                String originalSql, TablePath tablePath, Optional<PartitionSpec> partitionSpec) {
            super(originalSql);
            this.tablePath = tablePath;
            this.partitionSpec = partitionSpec.orElse(null);
        }

        public TablePath getTablePath() {
            return tablePath;
        }

        public Optional<PartitionSpec> getPartitionSpec() {
            return Optional.ofNullable(partitionSpec);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowKvSnapshots(this, context);
        }
    }

    /** SHOW KV SNAPSHOT METADATA statement. */
    public static class ShowKvSnapshotMetadataStatement extends FlussStatement {
        private final TablePath tablePath;
        private final int bucketId;
        private final long snapshotId;

        public ShowKvSnapshotMetadataStatement(
                String originalSql, TablePath tablePath, int bucketId, long snapshotId) {
            super(originalSql);
            this.tablePath = tablePath;
            this.bucketId = bucketId;
            this.snapshotId = snapshotId;
        }

        public TablePath getTablePath() {
            return tablePath;
        }

        public int getBucketId() {
            return bucketId;
        }

        public long getSnapshotId() {
            return snapshotId;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowKvSnapshotMetadata(this, context);
        }
    }

    /** SHOW LAKE SNAPSHOT statement. */
    public static class ShowLakeSnapshotStatement extends FlussStatement {
        private final TablePath tablePath;

        public ShowLakeSnapshotStatement(String originalSql, TablePath tablePath) {
            super(originalSql);
            this.tablePath = tablePath;
        }

        public TablePath getTablePath() {
            return tablePath;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowLakeSnapshot(this, context);
        }
    }

    /** SHOW OFFSETS statement. */
    public static class ShowOffsetsStatement extends FlussStatement {
        private final TablePath tablePath;
        private final PartitionSpec partitionSpec;
        private final List<Integer> buckets;
        private final OffsetSpec offsetSpec;

        public ShowOffsetsStatement(
                String originalSql,
                TablePath tablePath,
                Optional<PartitionSpec> partitionSpec,
                List<Integer> buckets,
                OffsetSpec offsetSpec) {
            super(originalSql);
            this.tablePath = tablePath;
            this.partitionSpec = partitionSpec.orElse(null);
            this.buckets = buckets;
            this.offsetSpec = offsetSpec;
        }

        public TablePath getTablePath() {
            return tablePath;
        }

        public Optional<PartitionSpec> getPartitionSpec() {
            return Optional.ofNullable(partitionSpec);
        }

        public List<Integer> getBuckets() {
            return buckets;
        }

        public OffsetSpec getOffsetSpec() {
            return offsetSpec;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowOffsets(this, context);
        }
    }

    /** SHOW ACLS statement. */
    public static class ShowAclsStatement extends FlussStatement {
        private final String resource;

        public ShowAclsStatement(String originalSql, Optional<String> resource) {
            super(originalSql);
            this.resource = resource.orElse(null);
        }

        public Optional<String> getResource() {
            return Optional.ofNullable(resource);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowAcls(this, context);
        }
    }

    /** CREATE ACL statement. */
    public static class CreateAclStatement extends FlussStatement {
        private final String resource;
        private final String principal;
        private final String permission;

        public CreateAclStatement(
                String originalSql, String resource, String principal, String permission) {
            super(originalSql);
            this.resource = resource;
            this.principal = principal;
            this.permission = permission;
        }

        public String getResource() {
            return resource;
        }

        public String getPrincipal() {
            return principal;
        }

        public String getPermission() {
            return permission;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitCreateAcl(this, context);
        }
    }

    /** DROP ACL statement. */
    public static class DropAclStatement extends FlussStatement {
        private final String resource;
        private final String principal;
        private final String permission;

        public DropAclStatement(
                String originalSql, String resource, String principal, String permission) {
            super(originalSql);
            this.resource = resource;
            this.principal = principal;
            this.permission = permission;
        }

        public String getResource() {
            return resource;
        }

        public String getPrincipal() {
            return principal;
        }

        public String getPermission() {
            return permission;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitDropAcl(this, context);
        }
    }

    /** SHOW CLUSTER CONFIGS statement. */
    public static class ShowClusterConfigsStatement extends FlussStatement {
        public ShowClusterConfigsStatement(String originalSql) {
            super(originalSql);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowClusterConfigs(this, context);
        }
    }

    /** ALTER CLUSTER CONFIGS statement. */
    public static class AlterClusterConfigsStatement extends FlussStatement {
        private final Map<String, String> configs;

        public AlterClusterConfigsStatement(String originalSql, Map<String, String> configs) {
            super(originalSql);
            this.configs = configs;
        }

        public Map<String, String> getConfigs() {
            return configs;
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitAlterClusterConfigs(this, context);
        }
    }

    /** REBALANCE CLUSTER statement. */
    public static class RebalanceClusterStatement extends FlussStatement {
        private final String databaseName;

        public RebalanceClusterStatement(String originalSql, Optional<String> databaseName) {
            super(originalSql);
            this.databaseName = databaseName.orElse(null);
        }

        public Optional<String> getDatabaseName() {
            return Optional.ofNullable(databaseName);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitRebalanceCluster(this, context);
        }
    }

    /** SHOW REBALANCE statement. */
    public static class ShowRebalanceStatement extends FlussStatement {
        private final String databaseName;

        public ShowRebalanceStatement(String originalSql, Optional<String> databaseName) {
            super(originalSql);
            this.databaseName = databaseName.orElse(null);
        }

        public Optional<String> getDatabaseName() {
            return Optional.ofNullable(databaseName);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitShowRebalance(this, context);
        }
    }

    /** CANCEL REBALANCE statement. */
    public static class CancelRebalanceStatement extends FlussStatement {
        private final String databaseName;

        public CancelRebalanceStatement(String originalSql, Optional<String> databaseName) {
            super(originalSql);
            this.databaseName = databaseName.orElse(null);
        }

        public Optional<String> getDatabaseName() {
            return Optional.ofNullable(databaseName);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitCancelRebalance(this, context);
        }
    }

    /** ALTER TABLE statement. */
    public static class AlterTableStatement extends FlussStatement {
        private final TablePath tablePath;
        private final String action;
        private final PartitionSpec partitionSpec;
        private final String specContent;

        public AlterTableStatement(
                String originalSql,
                TablePath tablePath,
                String action,
                Optional<PartitionSpec> partitionSpec,
                Optional<String> specContent) {
            super(originalSql);
            this.tablePath = tablePath;
            this.action = action;
            this.partitionSpec = partitionSpec.orElse(null);
            this.specContent = specContent.orElse(null);
        }

        public TablePath getTablePath() {
            return tablePath;
        }

        public String getAction() {
            return action;
        }

        public Optional<PartitionSpec> getPartitionSpec() {
            return Optional.ofNullable(partitionSpec);
        }

        public Optional<String> getSpecContent() {
            return Optional.ofNullable(specContent);
        }

        @Override
        public <R, C> R accept(FlussStatementVisitor<R, C> visitor, C context) {
            return visitor.visitAlterTable(this, context);
        }
    }
}
