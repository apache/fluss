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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.awaitOperationTermination;
import static org.apache.flink.test.util.TestUtils.waitUntilAllTasksAreRunning;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for materialized table in Flink 2.2. */
public class Flink22MaterializedTableITCase extends MaterializedTableITCase {

    @Test
    void testAlterMaterializedTableAsQueryWithoutSchemaChangesInContinuousMode(
            @TempDir Path temporaryPath) throws Exception {
        String materializedTableDDL =
                "CREATE MATERIALIZED TABLE shop_detail_alter\n"
                        + " FRESHNESS = INTERVAL '3' SECOND\n"
                        + " AS SELECT \n"
                        + "  DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds,\n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  payment_amount_cents\n"
                        + " FROM datagenSource";
        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle, materializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, materializedTableHandle);

        ObjectIdentifier mtIdentifier =
                ObjectIdentifier.of(CATALOG_NAME, DEFAULT_DB, "shop_detail_alter");
        ResolvedCatalogMaterializedTable originMaterializedTable =
                (ResolvedCatalogMaterializedTable) service.getTable(sessionHandle, mtIdentifier);
        String originDefinitionQuery = originMaterializedTable.getDefinitionQuery();

        ContinuousRefreshHandler originRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        originMaterializedTable.getSerializedRefreshHandler(),
                        getClass().getClassLoader());
        waitUntilAllTasksAreRunning(
                restClusterClient, JobID.fromHexString(originRefreshHandler.getJobId()));

        // altering in continuous mode stops the old job with a savepoint, so a savepoint dir is
        // required.
        String savepointDir = temporaryPath.toString();
        String alterJobSavepointDDL =
                String.format(
                        "SET 'execution.checkpointing.savepoint-dir' = 'file://%s'", savepointDir);
        OperationHandle alterSavepointHandle =
                service.executeStatement(
                        sessionHandle, alterJobSavepointDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterSavepointHandle);

        // modify the definition query, keeping the same output schema so that no column evolution
        // is triggered.
        String alterMaterializedTableAsQueryDDL =
                "ALTER MATERIALIZED TABLE shop_detail_alter\n"
                        + " AS SELECT \n"
                        + "  DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds,\n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  payment_amount_cents\n"
                        + " FROM datagenSource\n"
                        + " WHERE order_id <> 12345";
        OperationHandle alterAsQueryHandle =
                service.executeStatement(
                        sessionHandle, alterMaterializedTableAsQueryDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterAsQueryHandle);

        // the definition query should have been updated in the catalog.
        ResolvedCatalogMaterializedTable alteredMaterializedTable =
                (ResolvedCatalogMaterializedTable) service.getTable(sessionHandle, mtIdentifier);
        assertThat(alteredMaterializedTable.getDefinitionQuery())
                .isNotEqualTo(originDefinitionQuery)
                .contains("12345");
        assertThat(alteredMaterializedTable.getRefreshStatus())
                .isEqualTo(CatalogMaterializedTable.RefreshStatus.ACTIVATED);

        // the new refresh job should be running.
        ContinuousRefreshHandler alteredRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        alteredMaterializedTable.getSerializedRefreshHandler(),
                        getClass().getClassLoader());
        waitUntilAllTasksAreRunning(
                restClusterClient, JobID.fromHexString(alteredRefreshHandler.getJobId()));

        dropMaterializedTable(mtIdentifier);
    }

    @Test
    void testAlterMaterializedTableAsQueryWithUnsupportedSchemaChanges(@TempDir Path temporaryPath)
            throws Exception {
        String materializedTableDDL =
                "CREATE MATERIALIZED TABLE shop_detail_unsupported\n"
                        + " FRESHNESS = INTERVAL '3' SECOND\n"
                        + " AS SELECT \n"
                        + "  DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds,\n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  payment_amount_cents\n"
                        + " FROM datagenSource";
        OperationHandle materializedTableHandle =
                service.executeStatement(
                        sessionHandle, materializedTableDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, materializedTableHandle);

        ObjectIdentifier mtIdentifier =
                ObjectIdentifier.of(CATALOG_NAME, DEFAULT_DB, "shop_detail_unsupported");
        ResolvedCatalogMaterializedTable originMaterializedTable =
                (ResolvedCatalogMaterializedTable) service.getTable(sessionHandle, mtIdentifier);
        assertThat(originMaterializedTable.getResolvedSchema().getColumnNames())
                .containsExactly("ds", "user_id", "shop_id", "payment_amount_cents");

        ContinuousRefreshHandler originRefreshHandler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        originMaterializedTable.getSerializedRefreshHandler(),
                        getClass().getClassLoader());
        waitUntilAllTasksAreRunning(
                restClusterClient, JobID.fromHexString(originRefreshHandler.getJobId()));

        String savepointDir = temporaryPath.toString();
        String alterJobSavepointDDL =
                String.format(
                        "SET 'execution.checkpointing.savepoint-dir' = 'file://%s'", savepointDir);
        OperationHandle alterSavepointHandle =
                service.executeStatement(
                        sessionHandle, alterJobSavepointDDL, -1, new Configuration());
        awaitOperationTermination(service, sessionHandle, alterSavepointHandle);

        // although appending a nullable column (status) at the end of the schema is the only
        // schema evolution flink supports for materialized tables, fluss rejects it because the
        // table alteration can only be applied to one of the following: table properties or table
        // schema
        String addColumnDDL =
                "ALTER MATERIALIZED TABLE shop_detail_unsupported\n"
                        + " AS SELECT \n"
                        + "  DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds,\n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  payment_amount_cents,\n"
                        + "  status\n"
                        + " FROM datagenSource";

        assertThatThrownBy(
                        () -> {
                            OperationHandle handle =
                                    service.executeStatement(
                                            sessionHandle, addColumnDDL, -1, new Configuration());
                            awaitOperationTermination(service, sessionHandle, handle);
                        })
                .hasStackTraceContaining(
                        "Table alteration can only be applied to one of the following: "
                                + "table properties or table schema");

        // dropping a column is rejected by flink at statement-compile time (before touching the
        // running job), so no savepoint dir is required.
        String dropColumnDDL =
                "ALTER MATERIALIZED TABLE shop_detail_unsupported\n"
                        + " AS SELECT \n"
                        + "  DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds,\n"
                        + "  user_id,\n"
                        + "  shop_id\n"
                        + " FROM datagenSource";
        assertThatThrownBy(
                        () -> {
                            OperationHandle handle =
                                    service.executeStatement(
                                            sessionHandle, dropColumnDDL, -1, new Configuration());
                            awaitOperationTermination(service, sessionHandle, handle);
                        })
                .hasStackTraceContaining("drop column is unsupported");

        // reordering columns is rejected as well.
        String reorderColumnDDL =
                "ALTER MATERIALIZED TABLE shop_detail_unsupported\n"
                        + " AS SELECT \n"
                        + "  DATE_FORMAT(order_created_at, 'yyyy-MM-dd') AS ds,\n"
                        + "  shop_id,\n"
                        + "  user_id,\n"
                        + "  payment_amount_cents\n"
                        + " FROM datagenSource";
        assertThatThrownBy(
                        () -> {
                            OperationHandle handle =
                                    service.executeStatement(
                                            sessionHandle,
                                            reorderColumnDDL,
                                            -1,
                                            new Configuration());
                            awaitOperationTermination(service, sessionHandle, handle);
                        })
                .hasStackTraceContaining("reordering columns are not supported");

        dropMaterializedTable(mtIdentifier);
    }
}
