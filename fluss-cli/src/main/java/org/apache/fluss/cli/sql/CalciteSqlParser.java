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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlDropDatabase;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

import java.util.ArrayList;
import java.util.List;

/** Parses SQL statements using Apache Calcite. */
public class CalciteSqlParser {

    private final SqlParser.Config parserConfig;

    public CalciteSqlParser() {
        this.parserConfig =
                SqlParser.config()
                        .withCaseSensitive(false)
                        .withUnquotedCasing(org.apache.calcite.avatica.util.Casing.UNCHANGED)
                        .withQuotedCasing(org.apache.calcite.avatica.util.Casing.UNCHANGED)
                        .withParserFactory(
                                org.apache.flink.sql.parser.impl.FlinkSqlParserImpl.FACTORY);
    }

    public List<SqlNode> parse(String sql) throws Exception {
        List<SqlNode> nodes = new ArrayList<>();

        if (isShowOrDescribeStatement(sql)) {
            return nodes;
        }

        String processedSql = preprocessSql(sql);

        SqlParser parser = SqlParser.create(processedSql, parserConfig);
        SqlNode node = parser.parseStmt();

        if (node instanceof SqlNodeList) {
            SqlNodeList nodeList = (SqlNodeList) node;
            for (SqlNode n : nodeList) {
                if (n != null) {
                    nodes.add(n);
                }
            }
        } else {
            nodes.add(node);
        }

        return nodes;
    }

    public boolean isShowOrDescribeStatement(String sql) {
        String trimmed = sql.trim().toUpperCase();
        return trimmed.startsWith("SHOW ")
                || trimmed.startsWith("DESCRIBE ")
                || trimmed.startsWith("DESC ");
    }

    public SqlStatementType classifyShowOrDescribe(String sql) {
        String trimmed = sql.trim().toUpperCase();
        if (trimmed.startsWith("SHOW DATABASES") || trimmed.startsWith("SHOW SCHEMAS")) {
            return SqlStatementType.SHOW_DATABASES;
        } else if (trimmed.startsWith("SHOW DATABASE EXISTS")) {
            return SqlStatementType.SHOW_DATABASE_EXISTS;
        } else if (trimmed.startsWith("SHOW DATABASE ")) {
            return SqlStatementType.SHOW_DATABASE_INFO;
        } else if (trimmed.startsWith("SHOW TABLE EXISTS")) {
            return SqlStatementType.SHOW_TABLE_EXISTS;
        } else if (trimmed.startsWith("SHOW TABLE SCHEMA")) {
            return SqlStatementType.SHOW_TABLE_SCHEMA;
        } else if (trimmed.startsWith("SHOW TABLES")) {
            return SqlStatementType.SHOW_TABLES;
        } else if (trimmed.startsWith("SHOW SERVERS")) {
            return SqlStatementType.SHOW_SERVERS;
        } else if (trimmed.startsWith("SHOW CREATE TABLE")) {
            return SqlStatementType.SHOW_CREATE_TABLE;
        } else if (trimmed.startsWith("DESCRIBE ") || trimmed.startsWith("DESC ")) {
            return SqlStatementType.DESCRIBE_TABLE;
        }
        return SqlStatementType.UNKNOWN;
    }

    public SqlStatementType classifyRawStatement(String sql) {
        String trimmed = sql.trim();
        String upper = trimmed.toUpperCase();
        if (upper.startsWith("SHOW DATABASES") || upper.startsWith("SHOW SCHEMAS")) {
            return SqlStatementType.SHOW_DATABASES;
        } else if (upper.startsWith("SHOW DATABASE EXISTS")) {
            return SqlStatementType.SHOW_DATABASE_EXISTS;
        } else if (upper.startsWith("SHOW DATABASE ")) {
            return SqlStatementType.SHOW_DATABASE_INFO;
        } else if (upper.startsWith("SHOW TABLE EXISTS")) {
            return SqlStatementType.SHOW_TABLE_EXISTS;
        } else if (upper.startsWith("SHOW TABLE SCHEMA")) {
            return SqlStatementType.SHOW_TABLE_SCHEMA;
        } else if (upper.startsWith("SHOW TABLES")) {
            return SqlStatementType.SHOW_TABLES;
        } else if (upper.startsWith("SHOW SERVERS")) {
            return SqlStatementType.SHOW_SERVERS;
        } else if (upper.startsWith("USE ")) {
            return SqlStatementType.USE_DATABASE;
        } else if (upper.startsWith("SHOW CREATE TABLE")) {
            return SqlStatementType.SHOW_CREATE_TABLE;
        } else if (upper.startsWith("SHOW PARTITIONS")) {
            return SqlStatementType.SHOW_PARTITIONS;
        } else if (upper.startsWith("SHOW KV SNAPSHOTS")) {
            return SqlStatementType.SHOW_KV_SNAPSHOTS;
        } else if (upper.startsWith("SHOW KV SNAPSHOT METADATA")) {
            return SqlStatementType.SHOW_KV_SNAPSHOT_METADATA;
        } else if (upper.startsWith("SHOW LAKE SNAPSHOT")) {
            return SqlStatementType.SHOW_LAKE_SNAPSHOT;
        } else if (upper.startsWith("SHOW OFFSETS")) {
            return SqlStatementType.SHOW_OFFSETS;
        } else if (upper.startsWith("SHOW ACLS")) {
            return SqlStatementType.SHOW_ACLS;
        } else if (upper.startsWith("SHOW CLUSTER CONFIGS")) {
            return SqlStatementType.SHOW_CLUSTER_CONFIGS;
        } else if (upper.startsWith("SHOW REBALANCE")) {
            return SqlStatementType.SHOW_REBALANCE;
        } else if (upper.startsWith("CREATE ACL")) {
            return SqlStatementType.CREATE_ACL;
        } else if (upper.startsWith("DROP ACL")) {
            return SqlStatementType.DROP_ACL;
        } else if (upper.startsWith("ALTER CLUSTER SET")
                || upper.startsWith("ALTER CLUSTER RESET")
                || upper.startsWith("ALTER CLUSTER APPEND")
                || upper.startsWith("ALTER CLUSTER SUBTRACT")) {
            return SqlStatementType.ALTER_CLUSTER_CONFIGS;
        } else if (upper.startsWith("REBALANCE CLUSTER")) {
            return SqlStatementType.REBALANCE_CLUSTER;
        } else if (upper.startsWith("CANCEL REBALANCE")) {
            return SqlStatementType.CANCEL_REBALANCE;
        } else if (upper.startsWith("DESCRIBE ") || upper.startsWith("DESC ")) {
            return SqlStatementType.DESCRIBE_TABLE;
        } else if (upper.startsWith("ALTER TABLE")) {
            return SqlStatementType.ALTER_TABLE;
        }
        return SqlStatementType.UNKNOWN;
    }

    private String preprocessSql(String sql) {
        String processed = sql;
        processed = processed.trim();
        if (processed.endsWith(";")) {
            processed = processed.substring(0, processed.length() - 1).trim();
        }
        return processed;
    }

    public boolean isUpsertStatement(String originalSql) {
        return originalSql.trim().toUpperCase().startsWith("UPSERT");
    }

    public SqlStatementType getStatementType(SqlNode node) {
        if (node instanceof org.apache.calcite.sql.SqlOrderBy) {
            org.apache.calcite.sql.SqlOrderBy orderBy = (org.apache.calcite.sql.SqlOrderBy) node;
            return getStatementType(orderBy.query);
        } else if (node instanceof SqlCreateTable) {
            return SqlStatementType.CREATE_TABLE;
        } else if (node instanceof SqlDropTable) {
            return SqlStatementType.DROP_TABLE;
        } else if (node instanceof SqlCreateDatabase) {
            return SqlStatementType.CREATE_DATABASE;
        } else if (node instanceof SqlDropDatabase) {
            return SqlStatementType.DROP_DATABASE;
        } else if (node instanceof RichSqlInsert) {
            return SqlStatementType.INSERT;
        } else if (node instanceof org.apache.calcite.sql.SqlUpdate) {
            return SqlStatementType.UPDATE;
        } else if (node instanceof org.apache.calcite.sql.SqlDelete) {
            return SqlStatementType.DELETE;
        } else if (node instanceof org.apache.calcite.sql.SqlSelect) {
            return SqlStatementType.SELECT;
        }
        return SqlStatementType.UNKNOWN;
    }

    /** SQL statement type enum. */
    public enum SqlStatementType {
        CREATE_DATABASE,
        DROP_DATABASE,
        CREATE_TABLE,
        DROP_TABLE,
        INSERT,
        UPDATE,
        DELETE,
        SELECT,
        SHOW_DATABASES,
        SHOW_DATABASE_EXISTS,
        SHOW_DATABASE_INFO,
        SHOW_TABLES,
        SHOW_TABLE_EXISTS,
        SHOW_TABLE_SCHEMA,
        SHOW_SERVERS,
        USE_DATABASE,
        DESCRIBE_TABLE,
        SHOW_CREATE_TABLE,
        SHOW_PARTITIONS,
        ALTER_TABLE,
        SHOW_KV_SNAPSHOTS,
        SHOW_KV_SNAPSHOT_METADATA,
        SHOW_LAKE_SNAPSHOT,
        SHOW_OFFSETS,
        SHOW_ACLS,
        CREATE_ACL,
        DROP_ACL,
        SHOW_CLUSTER_CONFIGS,
        ALTER_CLUSTER_CONFIGS,
        REBALANCE_CLUSTER,
        SHOW_REBALANCE,
        CANCEL_REBALANCE,
        UNKNOWN
    }
}
