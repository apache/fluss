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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatisticsHeaders;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.catalog.CommonCatalogOptions.TABLE_CATALOG_STORE_KIND;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.awaitOperationTermination;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.fetchAllResults;
import static org.apache.flink.test.util.TestUtils.waitUntilAllTasksAreRunning;
import static org.assertj.core.api.Assertions.assertThat;

/** Test the support of Materialized Table. */
public abstract class MaterializedTableITCase {

    private static final String FILE_CATALOG_STORE = "file_store";

    static Configuration initClusterConf() {
        Configuration clusterConf = new Configuration();
        // use a small check interval to cleanup partitions quickly
        clusterConf.set(ConfigOptions.AUTO_PARTITION_CHECK_INTERVAL, Duration.ofSeconds(3));
        return clusterConf;
    }

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(initClusterConf())
                    .build();

    @RegisterExtension
    @Order(1)
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .build());

    @RegisterExtension
    @Order(2)
    protected static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    @RegisterExtension
    @Order(3)
    protected static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(
                    () ->
                            Executors.newCachedThreadPool(
                                    new ExecutorThreadFactory(
                                            "SqlGatewayService Test Pool",
                                            IgnoreExceptionHandler.INSTANCE)));

    @RegisterExtension
    @Order(4)
    protected static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    protected static SqlGatewayServiceImpl service;
    private static SessionEnvironment defaultSessionEnvironment;

    static final String CATALOG_NAME = "testcatalog";
    static final String DEFAULT_DB = FlinkCatalogOptions.DEFAULT_DATABASE.defaultValue();

    protected SessionHandle sessionHandle;
    protected RestClusterClient<?> restClusterClient;

    @BeforeAll
    static void setUp(@TempDir Path temporaryFolder) throws Exception {
        service = (SqlGatewayServiceImpl) SQL_GATEWAY_SERVICE_EXTENSION.getService();

        // initialize file catalog store path
        Path fileCatalogStore = temporaryFolder.resolve(FILE_CATALOG_STORE);
        Files.createDirectory(fileCatalogStore);
        Map<String, String> catalogStoreOptions = new HashMap<>();
        catalogStoreOptions.put(TABLE_CATALOG_STORE_KIND.key(), "file");
        catalogStoreOptions.put("table.catalog-store.file.path", fileCatalogStore.toString());

        defaultSessionEnvironment =
                SessionEnvironment.newBuilder()
                        .addSessionConfig(catalogStoreOptions)
                        .setSessionEndpointVersion(new EndpointVersion() {})
                        .build();
    }

    @BeforeEach
    void before(@InjectClusterClient RestClusterClient<?> injectClusterClient) {
        // initialize session handle, create paimon catalog and register it to catalog
        // store
        sessionHandle = initializeSession();
        // init rest cluster client
        restClusterClient = injectClusterClient;
    }

    @Test
    void testCreateMaterializedTableInContinuousMode() throws Exception {
        String materializedTableDDL =
                "CREATE MATERIALIZED TABLE shop_detail\n"
                        + " FRESHNESS = INTERVAL '3' SECOND\n"
                        + " AS SELECT \n"
                        + "  DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds,\n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  payment_amount_cents\n"
                        + " FROM datagenSource";
        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle,
                        materializedTableDDL,
                        -1,
                        new org.apache.flink.configuration.Configuration());
        awaitOperationTermination(service, sessionHandle, materializedTableHandle);

        // validate materialized table: schema, refresh mode, refresh status, refresh handler,
        // doesn't check the data because it generates randomly.
        ResolvedCatalogMaterializedTable actualMaterializedTable =
                (ResolvedCatalogMaterializedTable)
                        service.getTable(
                                sessionHandle,
                                ObjectIdentifier.of(CATALOG_NAME, DEFAULT_DB, "shop_detail"));

        // Expected schema
        ResolvedSchema expectedSchema =
                ResolvedSchema.of(
                        Arrays.asList(
                                Column.physical("ds", DataTypes.STRING()),
                                Column.physical("user_id", DataTypes.BIGINT().notNull()),
                                Column.physical("shop_id", DataTypes.BIGINT().notNull()),
                                Column.physical("payment_amount_cents", DataTypes.BIGINT())));

        assertThat(actualMaterializedTable.getResolvedSchema()).isEqualTo(expectedSchema);
        assertThat(actualMaterializedTable.getFreshness()).isEqualTo(Duration.ofSeconds(3));
        assertThat(actualMaterializedTable.getLogicalRefreshMode())
                .isEqualTo(CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC);
        assertThat(actualMaterializedTable.getRefreshMode())
                .isEqualTo(CatalogMaterializedTable.RefreshMode.CONTINUOUS);
        assertThat(actualMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);
        assertThat(actualMaterializedTable.getRefreshHandlerDescription()).isNotEmpty();
        assertThat(actualMaterializedTable.getSerializedRefreshHandler()).isNotEmpty();

        ContinuousRefreshHandler activeRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        actualMaterializedTable.getSerializedRefreshHandler(),
                        getClass().getClassLoader());

        waitUntilAllTasksAreRunning(
                restClusterClient, JobID.fromHexString(activeRefreshHandler.getJobId()));

        // verify the background job is running
        String describeJobDDL = String.format("DESCRIBE JOB '%s'", activeRefreshHandler.getJobId());
        OperationHandle describeJobHandle =
                service.executeStatement(
                        sessionHandle,
                        describeJobDDL,
                        -1,
                        new org.apache.flink.configuration.Configuration());
        awaitOperationTermination(service, sessionHandle, describeJobHandle);
        List<RowData> jobResults = fetchAllResults(service, sessionHandle, describeJobHandle);
        assertThat(jobResults.get(0).getString(2).toString()).isEqualTo("RUNNING");

        // get checkpoint interval
        long checkpointInterval =
                getCheckpointIntervalConfig(restClusterClient, activeRefreshHandler.getJobId());
        assertThat(checkpointInterval).isEqualTo(3 * 1000);

        // drop materialized table
        dropMaterializedTable(ObjectIdentifier.of(CATALOG_NAME, DEFAULT_DB, "shop_detail"));
    }

    @Test
    void testSuspendAndResumeMaterializedTableInContinuousMode(@TempDir Path temporaryPath)
            throws Exception {
        String materializedTableDDL =
                "CREATE MATERIALIZED TABLE shop_detail\n"
                        + " FRESHNESS = INTERVAL '3' SECOND\n"
                        + " AS SELECT \n"
                        + "  DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds,\n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  payment_amount_cents\n"
                        + " FROM datagenSource";
        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle,
                        materializedTableDDL,
                        -1,
                        new org.apache.flink.configuration.Configuration());
        awaitOperationTermination(service, sessionHandle, materializedTableHandle);

        ObjectIdentifier mtIdentifier =
                ObjectIdentifier.of(CATALOG_NAME, DEFAULT_DB, "shop_detail");
        // validate materialized table: refresh status, refresh handler,
        // doesn't check the data because it generates randomly.
        ResolvedCatalogMaterializedTable activeMaterializedTable =
                (ResolvedCatalogMaterializedTable) service.getTable(sessionHandle, mtIdentifier);

        assertThat(activeMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);

        ContinuousRefreshHandler activeRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        activeMaterializedTable.getSerializedRefreshHandler(),
                        getClass().getClassLoader());
        waitUntilAllTasksAreRunning(
                restClusterClient, JobID.fromHexString(activeRefreshHandler.getJobId()));

        // set up savepoint dir
        String savepointDir = temporaryPath.toString();
        String alterJobSavepointDDL =
                String.format(
                        "SET 'execution.checkpointing.savepoint-dir' = 'file://%s'", savepointDir);
        OperationHandle alterMaterializedTableSavepointHandle =
                service.executeStatement(
                        sessionHandle,
                        alterJobSavepointDDL,
                        -1,
                        new org.apache.flink.configuration.Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableSavepointHandle);

        // suspend materialized table
        String alterMaterializedTableSuspendDDL = "ALTER MATERIALIZED TABLE shop_detail SUSPEND";
        OperationHandle alterMaterializedTableSuspendHandle =
                service.executeStatement(
                        sessionHandle,
                        alterMaterializedTableSuspendDDL,
                        -1,
                        new org.apache.flink.configuration.Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableSuspendHandle);

        ResolvedCatalogMaterializedTable suspendMaterializedTable =
                (ResolvedCatalogMaterializedTable) service.getTable(sessionHandle, mtIdentifier);
        assertThat(suspendMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.SUSPENDED);

        // verify background job is stopped
        byte[] refreshHandler = suspendMaterializedTable.getSerializedRefreshHandler();
        ContinuousRefreshHandler suspendRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        refreshHandler, getClass().getClassLoader());
        String suspendJobId = suspendRefreshHandler.getJobId();

        // validate the job is finished
        String describeJobDDL = String.format("DESCRIBE JOB '%s'", suspendJobId);
        OperationHandle describeJobHandle =
                service.executeStatement(
                        sessionHandle,
                        describeJobDDL,
                        -1,
                        new org.apache.flink.configuration.Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableSuspendHandle);
        List<RowData> jobResults = fetchAllResults(service, sessionHandle, describeJobHandle);
        assertThat(jobResults.get(0).getString(2).toString()).isEqualTo("FINISHED");

        // verify savepoint is created
        assertThat(suspendRefreshHandler.getRestorePath()).isNotEmpty();
        String actualSavepointPath = suspendRefreshHandler.getRestorePath().get();

        // resume materialized table
        String alterMaterializedTableResumeDDL = "ALTER MATERIALIZED TABLE shop_detail RESUME";
        OperationHandle alterMaterializedTableResumeHandle =
                service.executeStatement(
                        sessionHandle,
                        alterMaterializedTableResumeDDL,
                        -1,
                        new org.apache.flink.configuration.Configuration());
        awaitOperationTermination(service, sessionHandle, alterMaterializedTableResumeHandle);

        ResolvedCatalogMaterializedTable resumedCatalogMaterializedTable =
                (ResolvedCatalogMaterializedTable) service.getTable(sessionHandle, mtIdentifier);
        assertThat(resumedCatalogMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);

        waitUntilAllTasksAreRunning(
                restClusterClient,
                JobID.fromHexString(
                        ContinuousRefreshHandlerSerializer.INSTANCE
                                .deserialize(
                                        resumedCatalogMaterializedTable
                                                .getSerializedRefreshHandler(),
                                        getClass().getClassLoader())
                                .getJobId()));

        // verify background job is running
        refreshHandler = resumedCatalogMaterializedTable.getSerializedRefreshHandler();
        ContinuousRefreshHandler resumeRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        refreshHandler, getClass().getClassLoader());
        String resumeJobId = resumeRefreshHandler.getJobId();
        String describeResumeJobDDL = String.format("DESCRIBE JOB '%s'", resumeJobId);
        OperationHandle describeResumeJobHandle =
                service.executeStatement(
                        sessionHandle,
                        describeResumeJobDDL,
                        -1,
                        new org.apache.flink.configuration.Configuration());
        awaitOperationTermination(service, sessionHandle, describeResumeJobHandle);
        jobResults = fetchAllResults(service, sessionHandle, describeResumeJobHandle);
        assertThat(jobResults.get(0).getString(2).toString()).isEqualTo("RUNNING");

        // verify resumed job is restored from savepoint
        Optional<String> actualRestorePath =
                getJobRestoreSavepointPath(restClusterClient, resumeJobId);
        assertThat(actualRestorePath).isNotEmpty();
        assertThat(actualRestorePath.get()).isEqualTo(actualSavepointPath);

        // drop the materialized table
        dropMaterializedTable(mtIdentifier);
    }

    public void dropMaterializedTable(ObjectIdentifier objectIdentifier) throws Exception {
        String dropMaterializedTableDDL =
                String.format(
                        "DROP MATERIALIZED TABLE %s", objectIdentifier.asSerializableString());
        OperationHandle dropMaterializedTableHandle =
                service.executeStatement(
                        sessionHandle,
                        dropMaterializedTableDDL,
                        -1,
                        new org.apache.flink.configuration.Configuration());
        awaitOperationTermination(service, sessionHandle, dropMaterializedTableHandle);
    }

    private long getCheckpointIntervalConfig(RestClusterClient<?> restClusterClient, String jobId)
            throws Exception {
        CheckpointConfigInfo checkpointConfigInfo =
                sendJobRequest(
                        restClusterClient,
                        CheckpointConfigHeaders.getInstance(),
                        EmptyRequestBody.getInstance(),
                        jobId);
        return RestMapperUtils.getStrictObjectMapper()
                .readTree(
                        RestMapperUtils.getStrictObjectMapper()
                                .writeValueAsString(checkpointConfigInfo))
                .get("interval")
                .asLong();
    }

    private Optional<String> getJobRestoreSavepointPath(
            RestClusterClient<?> restClusterClient, String jobId) throws Exception {
        CheckpointingStatistics checkpointingStatistics =
                sendJobRequest(
                        restClusterClient,
                        CheckpointingStatisticsHeaders.getInstance(),
                        EmptyRequestBody.getInstance(),
                        jobId);

        CheckpointingStatistics.RestoredCheckpointStatistics restoredCheckpointStatistics =
                checkpointingStatistics.getLatestCheckpoints().getRestoredCheckpointStatistics();
        return restoredCheckpointStatistics != null
                ? Optional.ofNullable(restoredCheckpointStatistics.getExternalPath())
                : Optional.empty();
    }

    private <M extends JobMessageParameters, R extends RequestBody, P extends ResponseBody>
            P sendJobRequest(
                    RestClusterClient<?> restClusterClient,
                    MessageHeaders<R, P, M> headers,
                    R requestBody,
                    String jobId)
                    throws Exception {
        M jobMessageParameters = headers.getUnresolvedMessageParameters();
        jobMessageParameters.jobPathParameter.resolve(JobID.fromHexString(jobId));

        return restClusterClient
                .sendRequest(headers, jobMessageParameters, requestBody)
                .get(5, TimeUnit.SECONDS);
    }

    private SessionHandle initializeSession() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        String bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        String catalogDDL =
                String.format(
                        "CREATE CATALOG IF NOT EXISTS %s\n"
                                + "WITH (\n"
                                + "  'type' = 'fluss',\n"
                                + "  'bootstrap.servers' = '%s'\n"
                                + "  )",
                        CATALOG_NAME, bootstrapServers);
        service.configureSession(sessionHandle, catalogDDL, -1);
        service.configureSession(sessionHandle, String.format("USE CATALOG %s", CATALOG_NAME), -1);

        // create source table
        String dataGenSource =
                "CREATE TEMPORARY TABLE datagenSource (\n"
                        + "  order_id BIGINT,\n"
                        + "  order_number VARCHAR(20),\n"
                        + "  user_id BIGINT NOT NULL,\n"
                        + "  shop_id BIGINT NOT NULL,\n"
                        + "  product_id BIGINT,\n"
                        + "  status BIGINT,\n"
                        + "  order_type BIGINT,\n"
                        + "  order_created_at TIMESTAMP NOT NULL,\n"
                        + "  payment_amount_cents BIGINT\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '10'\n"
                        + ")";
        service.configureSession(sessionHandle, dataGenSource, -1);
        return sessionHandle;
    }
}
