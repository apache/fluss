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

package org.apache.fluss.cli.sql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CalciteSqlParserTest {

    private CalciteSqlParser parser;

    @BeforeEach
    void setUp() {
        parser = new CalciteSqlParser();
    }

    @Test
    void testClassifyRawStatementMetadataCommands() {
        assertThat(parser.classifyRawStatement("SHOW SERVERS"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_SERVERS);
        assertThat(parser.classifyRawStatement("SHOW DATABASE EXISTS mydb"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_DATABASE_EXISTS);
        assertThat(parser.classifyRawStatement("SHOW DATABASE mydb"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_DATABASE_INFO);
        assertThat(parser.classifyRawStatement("SHOW TABLE EXISTS mydb.users"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_TABLE_EXISTS);
        assertThat(parser.classifyRawStatement("SHOW TABLE SCHEMA mydb.users"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_TABLE_SCHEMA);
        assertThat(parser.classifyRawStatement("USE mydb"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.USE_DATABASE);
        assertThat(parser.classifyRawStatement("SHOW PARTITIONS FROM mydb.users"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_PARTITIONS);
        assertThat(parser.classifyRawStatement("SHOW KV SNAPSHOTS FROM mydb.users"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_KV_SNAPSHOTS);
        assertThat(
                        parser.classifyRawStatement(
                                "SHOW KV SNAPSHOT METADATA FROM mydb.users BUCKET 0 SNAPSHOT 1"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_KV_SNAPSHOT_METADATA);
        assertThat(parser.classifyRawStatement("SHOW LAKE SNAPSHOT FROM mydb.users"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_LAKE_SNAPSHOT);
        assertThat(parser.classifyRawStatement("SHOW OFFSETS FROM mydb.users BUCKETS (0)"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_OFFSETS);
        assertThat(parser.classifyRawStatement("SHOW ACLS FILTER (resource='TABLE:db.tbl')"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_ACLS);
        assertThat(parser.classifyRawStatement("SHOW CLUSTER CONFIGS"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_CLUSTER_CONFIGS);
        assertThat(parser.classifyRawStatement("SHOW REBALANCE"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_REBALANCE);
    }

    @Test
    void testClassifyRawStatementClusterCommands() {
        assertThat(parser.classifyRawStatement("ALTER CLUSTER SET ('k'='v')"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.ALTER_CLUSTER_CONFIGS);
        assertThat(parser.classifyRawStatement("ALTER CLUSTER RESET ('k')"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.ALTER_CLUSTER_CONFIGS);
        assertThat(parser.classifyRawStatement("ALTER CLUSTER APPEND ('k'='v')"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.ALTER_CLUSTER_CONFIGS);
        assertThat(parser.classifyRawStatement("ALTER CLUSTER SUBTRACT ('k'='v')"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.ALTER_CLUSTER_CONFIGS);
        assertThat(
                        parser.classifyRawStatement(
                                "REBALANCE CLUSTER WITH GOALS (REPLICA_DISTRIBUTION)"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.REBALANCE_CLUSTER);
        assertThat(parser.classifyRawStatement("CANCEL REBALANCE"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.CANCEL_REBALANCE);
    }

    @Test
    void testClassifyRawStatementAclCommands() {
        assertThat(
                        parser.classifyRawStatement(
                                "CREATE ACL (resource_type='TABLE', resource_name='db.tbl', principal='User:alice', operation='READ', permission='ALLOW')"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.CREATE_ACL);
        assertThat(parser.classifyRawStatement("DROP ACL FILTER (resource='TABLE:db.tbl')"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.DROP_ACL);
    }

    @Test
    void testClassifyRawStatementAlterTable() {
        assertThat(parser.classifyRawStatement("ALTER TABLE mydb.users ADD COLUMN age INT"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.ALTER_TABLE);
        assertThat(parser.classifyRawStatement("ALTER TABLE IF EXISTS mydb.users SET ('k'='v')"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.ALTER_TABLE);
        assertThat(parser.classifyRawStatement("ALTER TABLE mydb.users DROP PARTITION (dt='x')"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.ALTER_TABLE);
    }

    @Test
    void testClassifyRawStatementCaseInsensitive() {
        assertThat(parser.classifyRawStatement("show servers"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_SERVERS);
        assertThat(parser.classifyRawStatement("show database exists mydb"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_DATABASE_EXISTS);
        assertThat(parser.classifyRawStatement("show table schema mydb.users"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_TABLE_SCHEMA);
        assertThat(parser.classifyRawStatement("use mydb"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.USE_DATABASE);
        assertThat(parser.classifyRawStatement("show offsets from mydb.users buckets (0)"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_OFFSETS);
        assertThat(parser.classifyRawStatement("alter cluster set with ('k'='v')"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.ALTER_CLUSTER_CONFIGS);
        assertThat(parser.classifyRawStatement("show acls"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.SHOW_ACLS);
    }

    @Test
    void testClassifyRawStatementUnknown() {
        assertThat(parser.classifyRawStatement("ANALYZE TABLE mydb.users"))
                .isEqualTo(CalciteSqlParser.SqlStatementType.UNKNOWN);
    }
}
