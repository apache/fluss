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

package org.apache.fluss.cli.sql.executor;

import org.apache.fluss.cli.config.ConnectionManager;
import org.apache.fluss.cli.util.SqlParserUtil;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.cluster.rebalance.GoalType;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.config.cluster.ConfigEntry;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Handles cluster-related SQL operations. */
public class ClusterExecutor {
    private static final Pattern ALTER_CLUSTER_SET_PATTERN =
            Pattern.compile(
                    "ALTER\\s+CLUSTER\\s+SET\\s+(?:WITH\\s+)?\\((.*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern ALTER_CLUSTER_RESET_PATTERN =
            Pattern.compile(
                    "ALTER\\s+CLUSTER\\s+RESET\\s+(?:WITH\\s+)?\\((.*)\\)",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern ALTER_CLUSTER_APPEND_PATTERN =
            Pattern.compile(
                    "ALTER\\s+CLUSTER\\s+APPEND\\s+(?:WITH\\s+)?\\((.*)\\)",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern ALTER_CLUSTER_SUBTRACT_PATTERN =
            Pattern.compile(
                    "ALTER\\s+CLUSTER\\s+SUBTRACT\\s+(?:WITH\\s+)?\\((.*)\\)",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern REBALANCE_CLUSTER_PATTERN =
            Pattern.compile(
                    "REBALANCE\\s+CLUSTER\\s+WITH\\s+GOALS\\s*\\((.*)\\)",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_REBALANCE_PATTERN =
            Pattern.compile("SHOW\\s+REBALANCE(\\s+ID\\s+(\\S+))?", Pattern.CASE_INSENSITIVE);
    private static final Pattern CANCEL_REBALANCE_PATTERN =
            Pattern.compile("CANCEL\\s+REBALANCE(\\s+ID\\s+(\\S+))?", Pattern.CASE_INSENSITIVE);

    private final ConnectionManager connectionManager;
    private final PrintWriter out;

    public ClusterExecutor(ConnectionManager connectionManager, PrintWriter out) {
        this.connectionManager = connectionManager;
        this.out = out;
    }

    public void executeShowClusterConfigs(
            org.apache.fluss.cli.sql.ast.FlussStatementNodes.ShowClusterConfigsStatement stmt)
            throws Exception {
        Admin admin = connectionManager.getConnection().getAdmin();
        Collection<ConfigEntry> configs = admin.describeClusterConfigs().get();

        out.println("Cluster Configs:");
        out.println("Key\tValue\tSource");
        for (ConfigEntry entry : configs) {
            out.println(
                    entry.key()
                            + "\t"
                            + (entry.value() == null ? "<unset>" : entry.value())
                            + "\t"
                            + entry.source());
        }
        out.flush();
    }

    public void executeAlterClusterConfigs(
            org.apache.fluss.cli.sql.ast.FlussStatementNodes.AlterClusterConfigsStatement stmt)
            throws Exception {
        String normalized = SqlParserUtil.stripTrailingSemicolon(stmt.getOriginalSql());
        Matcher setMatcher = ALTER_CLUSTER_SET_PATTERN.matcher(normalized);
        Matcher resetMatcher = ALTER_CLUSTER_RESET_PATTERN.matcher(normalized);
        Matcher appendMatcher = ALTER_CLUSTER_APPEND_PATTERN.matcher(normalized);
        Matcher subtractMatcher = ALTER_CLUSTER_SUBTRACT_PATTERN.matcher(normalized);

        List<AlterConfig> configs = new ArrayList<>();
        AlterConfigOpType opType;
        String content;

        if (setMatcher.find()) {
            opType = AlterConfigOpType.SET;
            content = setMatcher.group(1);
        } else if (resetMatcher.find()) {
            opType = AlterConfigOpType.DELETE;
            content = resetMatcher.group(1);
        } else if (appendMatcher.find()) {
            opType = AlterConfigOpType.APPEND;
            content = appendMatcher.group(1);
        } else if (subtractMatcher.find()) {
            opType = AlterConfigOpType.SUBTRACT;
            content = subtractMatcher.group(1);
        } else {
            throw new IllegalArgumentException(
                    "ALTER CLUSTER supports SET/RESET/APPEND/SUBTRACT with property list: "
                            + stmt.getOriginalSql());
        }

        if (opType == AlterConfigOpType.SET
                || opType == AlterConfigOpType.APPEND
                || opType == AlterConfigOpType.SUBTRACT) {
            Map<String, String> properties = SqlParserUtil.parseKeyValueMap(content);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                configs.add(new AlterConfig(entry.getKey(), entry.getValue(), opType));
            }
        } else {
            List<String> keys = SqlParserUtil.parseKeyList(content);
            for (String key : keys) {
                configs.add(new AlterConfig(key, null, opType));
            }
        }

        Admin admin = connectionManager.getConnection().getAdmin();
        admin.alterClusterConfigs(configs).get();
        out.println("Cluster configs updated successfully.");
        out.flush();
    }

    public void executeRebalance(
            org.apache.fluss.cli.sql.ast.FlussStatementNodes.RebalanceClusterStatement stmt)
            throws Exception {
        String normalized = SqlParserUtil.stripTrailingSemicolon(stmt.getOriginalSql());
        Matcher matcher = REBALANCE_CLUSTER_PATTERN.matcher(normalized);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "REBALANCE CLUSTER requires GOALS list: " + stmt.getOriginalSql());
        }

        List<String> goals = SqlParserUtil.parseKeyList(matcher.group(1));
        List<GoalType> goalTypes = new ArrayList<>();
        for (String goal : goals) {
            goalTypes.add(GoalType.fromName(goal));
        }

        Admin admin = connectionManager.getConnection().getAdmin();
        String rebalanceId = admin.rebalance(goalTypes).get();
        out.println("Rebalance started: " + rebalanceId);
        out.flush();
    }

    public void executeShowRebalance(
            org.apache.fluss.cli.sql.ast.FlussStatementNodes.ShowRebalanceStatement stmt)
            throws Exception {
        String normalized = SqlParserUtil.stripTrailingSemicolon(stmt.getOriginalSql());
        Matcher matcher = SHOW_REBALANCE_PATTERN.matcher(normalized);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid SHOW REBALANCE statement: " + stmt.getOriginalSql());
        }

        String rebalanceId = matcher.group(2);
        Admin admin = connectionManager.getConnection().getAdmin();
        Optional<RebalanceProgress> progress = admin.listRebalanceProgress(rebalanceId).get();
        if (progress.isPresent()) {
            out.println("Rebalance progress: " + progress.get());
        } else {
            out.println("No rebalance in progress.");
        }
        out.flush();
    }

    public void executeCancelRebalance(
            org.apache.fluss.cli.sql.ast.FlussStatementNodes.CancelRebalanceStatement stmt)
            throws Exception {
        String normalized = SqlParserUtil.stripTrailingSemicolon(stmt.getOriginalSql());
        Matcher matcher = CANCEL_REBALANCE_PATTERN.matcher(normalized);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Invalid CANCEL REBALANCE statement: " + stmt.getOriginalSql());
        }

        String rebalanceId = matcher.group(2);
        Admin admin = connectionManager.getConnection().getAdmin();
        admin.cancelRebalance(rebalanceId).get();
        out.println("Rebalance cancelled" + (rebalanceId == null ? "" : ": " + rebalanceId));
        out.flush();
    }
}
