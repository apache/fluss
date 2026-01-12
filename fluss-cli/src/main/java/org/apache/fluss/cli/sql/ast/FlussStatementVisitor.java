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

/** Visitor pattern for Fluss statement AST nodes. */
public interface FlussStatementVisitor<R, C> {
    R visitShowDatabases(FlussStatementNodes.ShowDatabasesStatement statement, C context);

    R visitShowDatabaseExists(FlussStatementNodes.ShowDatabaseExistsStatement statement, C context);

    R visitShowDatabaseInfo(FlussStatementNodes.ShowDatabaseInfoStatement statement, C context);

    R visitShowTables(FlussStatementNodes.ShowTablesStatement statement, C context);

    R visitShowTableExists(FlussStatementNodes.ShowTableExistsStatement statement, C context);

    R visitShowTableSchema(FlussStatementNodes.ShowTableSchemaStatement statement, C context);

    R visitShowServers(FlussStatementNodes.ShowServersStatement statement, C context);

    R visitUseDatabase(FlussStatementNodes.UseDatabaseStatement statement, C context);

    R visitDescribeTable(FlussStatementNodes.DescribeTableStatement statement, C context);

    R visitShowCreateTable(FlussStatementNodes.ShowCreateTableStatement statement, C context);

    R visitShowPartitions(FlussStatementNodes.ShowPartitionsStatement statement, C context);

    R visitShowKvSnapshots(FlussStatementNodes.ShowKvSnapshotsStatement statement, C context);

    R visitShowKvSnapshotMetadata(
            FlussStatementNodes.ShowKvSnapshotMetadataStatement statement, C context);

    R visitShowLakeSnapshot(FlussStatementNodes.ShowLakeSnapshotStatement statement, C context);

    R visitShowOffsets(FlussStatementNodes.ShowOffsetsStatement statement, C context);

    R visitShowAcls(FlussStatementNodes.ShowAclsStatement statement, C context);

    R visitCreateAcl(FlussStatementNodes.CreateAclStatement statement, C context);

    R visitDropAcl(FlussStatementNodes.DropAclStatement statement, C context);

    R visitShowClusterConfigs(FlussStatementNodes.ShowClusterConfigsStatement statement, C context);

    R visitAlterClusterConfigs(
            FlussStatementNodes.AlterClusterConfigsStatement statement, C context);

    R visitRebalanceCluster(FlussStatementNodes.RebalanceClusterStatement statement, C context);

    R visitShowRebalance(FlussStatementNodes.ShowRebalanceStatement statement, C context);

    R visitCancelRebalance(FlussStatementNodes.CancelRebalanceStatement statement, C context);

    R visitAlterTable(FlussStatementNodes.AlterTableStatement statement, C context);
}
