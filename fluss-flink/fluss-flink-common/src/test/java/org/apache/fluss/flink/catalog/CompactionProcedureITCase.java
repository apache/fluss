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

package org.apache.fluss.flink.catalog;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.cluster.compaction.CompactionInfo;
import org.apache.fluss.cluster.compaction.CompactionProgress;
import org.apache.fluss.cluster.compaction.CompactionStatus;
import org.apache.fluss.cluster.compaction.CompactionType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.CompactionNotExistException;
import org.apache.fluss.flink.procedure.FlussProcedureContext;
import org.apache.fluss.flink.procedure.GetCompactionProcedure;
import org.apache.fluss.flink.procedure.ProcedureManager;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.testutils.common.MultiVersionTest;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** SQL integration tests for manual compaction procedures. */
@MultiVersionTest
public abstract class CompactionProcedureITCase extends FlinkTestBase {

    private TableEnvironment tEnv;

    @BeforeEach
    void createCatalog() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                "CREATE CATALOG compaction_catalog WITH ('type' = 'fluss', "
                        + "'bootstrap.servers' = '"
                        + bootstrapServers
                        + "')");
        tEnv.useCatalog("compaction_catalog");
        tEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void closeCatalog() {
        tEnv.useCatalog(TableConfigOptions.TABLE_CATALOG_NAME.defaultValue());
        tEnv.executeSql("DROP CATALOG compaction_catalog");
    }

    @Test
    void testCompactionScopesAndIdRediscovery() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE plain_compaction (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ('bucket.num' = '1')");
        tEnv.executeSql(
                "CREATE TABLE partitioned_compaction (id INT, region STRING, dt STRING, "
                        + "PRIMARY KEY (id, region, dt) NOT ENFORCED) PARTITIONED BY (region, dt) "
                        + "WITH ('bucket.num' = '6')");
        tEnv.executeSql("CREATE TABLE compaction_log (id INT) WITH ('bucket.num' = '1')");
        TablePath plain = TablePath.of(DEFAULT_DB, "plain_compaction");
        TablePath partitioned = TablePath.of(DEFAULT_DB, "partitioned_compaction");
        writeRows(conn, plain, Collections.singletonList(row(1, "value")), false);
        writeRows(conn, partitioned, Collections.singletonList(row(1, "eu", "20261021")), false);
        PartitionInfo partition = admin.listPartitionInfos(partitioned).get().get(0);
        String partitionName = partition.getPartitionName();
        assertThat(partitionName).isEqualTo("eu$20261021");
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(
                admin.getTableInfo(partitioned).get().getTableId(), partition.getPartitionId());

        String[] submissions = {
            "CALL sys.compact('MIGRATE')",
            "CALL sys.compact('" + DEFAULT_DB + "', 'RECLAIM')",
            "CALL sys.compact('" + DEFAULT_DB + "', 'plain_compaction', 'OPTIMIZE')",
            "CALL sys.compact('"
                    + DEFAULT_DB
                    + "', 'partitioned_compaction', '"
                    + partitionName
                    + "', 'MIGRATE')"
        };
        String[] types = {"MIGRATE", "RECLAIM", "OPTIMIZE", "MIGRATE"};
        for (int scope = 0; scope < submissions.length; scope++) {
            TableResult submitted = tEnv.executeSql(submissions[scope]);
            assertThat(submitted.getResolvedSchema().getColumnNames())
                    .containsExactly("operation_id");
            String operationId = (String) collect(submitted).get(0).getField(0);
            assertThat(operationId).isNotBlank();
            try {
                String[] queries = {
                    "CALL sys.list_compactions()",
                    "CALL sys.list_compactions('" + DEFAULT_DB + "')",
                    "CALL sys.list_compactions('"
                            + DEFAULT_DB
                            + "', '"
                            + (scope == 2 ? "plain_compaction" : "partitioned_compaction")
                            + "')"
                };
                for (String query : queries) {
                    assertThat(sql(query)).extracting(r -> r.getField(0)).contains(operationId);
                }
                if (scope != 2) {
                    assertThat(
                                    sql(
                                            "CALL sys.list_compactions('"
                                                    + DEFAULT_DB
                                                    + "', 'partitioned_compaction', '"
                                                    + partitionName
                                                    + "')"))
                            .extracting(r -> r.getField(0))
                            .contains(operationId);
                }
                Row summary = sql("CALL sys.get_compaction('" + operationId + "')").get(0);
                assertThat(summary.getField(0)).isEqualTo(operationId);
                assertThat(summary.getField(1)).isEqualTo(types[scope]);
                assertThat(summary.getField(3)).isEqualTo(scope == 0 ? null : DEFAULT_DB);
                assertThat(summary.getField(4))
                        .isEqualTo(
                                scope < 2
                                        ? null
                                        : scope == 2
                                                ? "plain_compaction"
                                                : "partitioned_compaction");
                assertThat(summary.getField(5)).isEqualTo(scope == 3 ? partitionName : null);
                assertThat(summary.getField(7)).isEqualTo(scope == 2 ? 1L : scope == 3 ? 6L : 7L);
                if (scope == 2) {
                    CompactionProgress completed =
                            waitValue(
                                    () -> {
                                        CompactionProgress progress =
                                                admin.getCompaction(operationId).get();
                                        return progress.info().status() == CompactionStatus.PENDING
                                                        || progress.info().status()
                                                                == CompactionStatus.RUNNING
                                                ? Optional.empty()
                                                : Optional.of(progress);
                                    },
                                    Duration.ofSeconds(30),
                                    "table compaction did not finish");
                    assertThat(completed.info().status()).isEqualTo(CompactionStatus.SUCCEEDED);
                    assertThat(completed.succeeded()).isEqualTo(1);
                }

            } finally {
                assertThat(sql("CALL sys.cancel_compaction('" + operationId + "')"))
                        .containsExactly(Row.of("success"));
            }
            sql("CALL sys.cancel_compaction('" + operationId + "')");
            CompactionProgress progress = admin.getCompaction(operationId).get();
            assertThat(progress.info().status())
                    .isIn(
                            CompactionStatus.SUCCEEDED,
                            CompactionStatus.FAILED,
                            CompactionStatus.CANCELLED);
            Row result = sql("CALL sys.get_compaction('" + operationId + "')").get(0);
            assertSummary(result, progress.info());
            assertThat(
                            Arrays.asList(
                                    result.getField(7),
                                    result.getField(8),
                                    result.getField(9),
                                    result.getField(10),
                                    result.getField(11),
                                    result.getField(12)))
                    .containsExactly(
                            progress.total(),
                            progress.pending(),
                            progress.running(),
                            progress.succeeded(),
                            progress.failed(),
                            progress.cancelled());
            assertThat(progress.total())
                    .isEqualTo(
                            progress.pending()
                                    + progress.running()
                                    + progress.succeeded()
                                    + progress.failed()
                                    + progress.cancelled());
            assertThat(result.getField(13)).isEqualTo(progress.lastProgressTime());
            assertThat((Long) result.getField(14)).isGreaterThanOrEqualTo(progress.observedTime());
            assertThat(result.getField(15)).isEqualTo(progress.persistedTime());
            assertThat(result.getField(16)).isEqualTo(progress.firstError().orElse(null));
            assertThat(result.getField(17)).isEqualTo("[]");
            Row listed =
                    sql("CALL sys.list_compactions()").stream()
                            .filter(r -> operationId.equals(r.getField(0)))
                            .findFirst()
                            .get();
            assertSummary(listed, progress.info());
        }
    }

    @Test
    void testGetCompactionCurrentExecutions() throws Exception {
        CompactionInfo info =
                new CompactionInfo(
                        "current-operation",
                        CompactionType.MIGRATE,
                        CompactionStatus.RUNNING,
                        100L,
                        null,
                        null,
                        null);
        List<CompactionProgress.Execution> currentExecutions =
                Arrays.asList(
                        new CompactionProgress.Execution(
                                7,
                                new TableBucket(11L, 17L, 2),
                                3,
                                "SNAPSHOT",
                                180L,
                                19L,
                                "snapshot \"ready\"\nwaiting for publication"),
                        new CompactionProgress.Execution(
                                8, new TableBucket(12L, 5), 4, "FLUSH", 190L, null, null));
        CompactionProgress progress =
                new CompactionProgress(
                        info,
                        9L,
                        3L,
                        2L,
                        4L,
                        0L,
                        0L,
                        currentExecutions,
                        "snapshot response unavailable",
                        200L,
                        300L,
                        250L);
        Admin responseAdmin = mock(Admin.class);
        when(responseAdmin.getCompaction(info.operationId()))
                .thenReturn(CompletableFuture.completedFuture(progress));
        GetCompactionProcedure procedure =
                (GetCompactionProcedure)
                        ProcedureManager.getProcedure(
                                        new FlussProcedureContext(
                                                responseAdmin,
                                                DEFAULT_DB,
                                                new Configuration(),
                                                Collections.emptyMap(),
                                                getClass().getClassLoader()),
                                        new ObjectPath("sys", "get_compaction"))
                                .get();

        Row[] results = procedure.call(null, info.operationId());
        assertThat(results).hasSize(1);
        String executionsJson = (String) results[0].getField(17);
        assertThat(results[0])
                .isEqualTo(
                        Row.of(
                                "current-operation",
                                "MIGRATE",
                                "RUNNING",
                                null,
                                null,
                                null,
                                100L,
                                9L,
                                3L,
                                2L,
                                4L,
                                0L,
                                0L,
                                200L,
                                300L,
                                250L,
                                "snapshot response unavailable",
                                executionsJson));
        JsonNode executions =
                JsonSerdeUtils.readValue(
                        executionsJson.getBytes(StandardCharsets.UTF_8), node -> node);
        assertThat(executions.isArray()).isTrue();
        assertThat(executions.size()).isEqualTo(currentExecutions.size());
        for (int i = 0; i < currentExecutions.size(); i++) {
            assertExecution(executions.get(i), currentExecutions.get(i));
        }
    }

    @Test
    void testInvalidTypeAndMissingOperation() {
        for (String type : Arrays.asList("UNKNOWN", "optimize", "")) {
            assertThatThrownBy(() -> sql("CALL sys.compact('" + type + "')"))
                    .rootCause()
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unknown compaction type");
        }
        assertThatThrownBy(() -> sql("CALL sys.get_compaction('missing-operation')"))
                .rootCause()
                .isInstanceOf(CompactionNotExistException.class);
        assertThatThrownBy(() -> sql("CALL sys.cancel_compaction('missing-operation')"))
                .rootCause()
                .isInstanceOf(CompactionNotExistException.class);
    }

    private static void assertExecution(JsonNode actual, CompactionProgress.Execution expected) {
        assertThat(actual.get("server_id").asInt()).isEqualTo(expected.serverId());
        assertThat(actual.get("table_id").asLong()).isEqualTo(expected.bucket().getTableId());
        if (expected.bucket().getPartitionId() == null) {
            assertThat(actual.get("partition_id").isNull()).isTrue();
        } else {
            assertThat(actual.get("partition_id").asLong())
                    .isEqualTo(expected.bucket().getPartitionId());
        }
        assertThat(actual.get("bucket_id").asInt()).isEqualTo(expected.bucket().getBucket());
        assertThat(actual.get("leader_epoch").asInt()).isEqualTo(expected.leaderEpoch());
        assertThat(actual.get("phase").asText()).isEqualTo(expected.phase());
        assertThat(actual.get("phase_start_time").asLong()).isEqualTo(expected.phaseStartTime());
        if (expected.snapshotId().isPresent()) {
            assertThat(actual.get("snapshot_id").asLong()).isEqualTo(expected.snapshotId().get());
        } else {
            assertThat(actual.get("snapshot_id").isNull()).isTrue();
        }
        assertThat(actual.get("reason").asText(null)).isEqualTo(expected.reason().orElse(null));
    }

    private static void assertSummary(Row row, CompactionInfo info) {
        assertThat(
                        Arrays.asList(
                                row.getField(0),
                                row.getField(1),
                                row.getField(2),
                                row.getField(3),
                                row.getField(4),
                                row.getField(5),
                                row.getField(6)))
                .containsExactly(
                        info.operationId(),
                        info.type().name(),
                        info.status().name(),
                        info.databaseName().orElse(null),
                        info.tablePath().map(TablePath::getTableName).orElse(null),
                        info.partitionName().orElse(null),
                        info.createTime());
    }

    private List<Row> sql(String statement) {
        return collect(tEnv.executeSql(statement));
    }

    private static List<Row> collect(TableResult result) {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            iterator.forEachRemaining(rows::add);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rows;
    }
}
