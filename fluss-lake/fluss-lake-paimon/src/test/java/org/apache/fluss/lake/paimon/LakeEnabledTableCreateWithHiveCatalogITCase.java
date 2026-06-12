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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for create lake enabled table with Paimon Hive catalog. */
class LakeEnabledTableCreateWithHiveCatalogITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private static final String DATABASE = "fluss";
    private static final int BUCKET_NUM = 3;

    private static Catalog paimonCatalog;
    private static String warehousePath;
    private static String customTablePath;

    private Connection conn;
    private Admin admin;

    @AfterAll
    static void afterAll() throws Exception {
        if (paimonCatalog != null) {
            paimonCatalog.close();
            paimonCatalog = null;
        }
    }

    @BeforeEach
    protected void setup() {
        conn = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = conn.getAdmin();
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setString("datalake.format", "paimon");
        conf.setString("datalake.paimon.metastore", "hive");
        conf.setString("datalake.paimon.cache-enabled", "false");
        try {
            java.nio.file.Path baseDir = Files.createTempDirectory("fluss-testing-hms-paimon");
            warehousePath = baseDir.resolve("warehouse").toString();
            customTablePath = baseDir.resolve("custom_lake_table_path").toUri().toString();
            conf.setString(
                    "datalake.paimon.javax.jdo.option.ConnectionURL",
                    "jdbc:derby:memory:" + baseDir.resolve("metastore_db") + ";create=true");
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create hive catalog test path", e);
        }
        conf.setString("datalake.paimon.warehouse", warehousePath);
        conf.setString(
                "datalake.paimon.javax.jdo.option.ConnectionDriverName",
                "org.apache.derby.jdbc.EmbeddedDriver");
        conf.setString("datalake.paimon.datanucleus.schema.autoCreateAll", "true");
        conf.setString("datalake.paimon.hive.metastore.schema.verification", "false");

        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(extractLakeProperties(conf))));

        return conf;
    }

    @Test
    void testAlterPaimonPathRequiresLakePathOptions() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "lake_paimon_path_without_mapping");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .build())
                        .distributedBy(BUCKET_NUM, "c1", "c2")
                        .build();
        admin.createTable(tablePath, tableDescriptor, false).get();

        assertThatThrownBy(
                        () ->
                                admin.alterTable(
                                                tablePath,
                                                Collections.singletonList(
                                                        TableChange.set(
                                                                "paimon.path", customTablePath)),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining(
                        "'paimon.path' can only be altered together with lake table path options");

        List<TableChange> paimonPathWithLakePath =
                Arrays.asList(
                        TableChange.set("paimon.path", customTablePath),
                        TableChange.set(
                                ConfigOptions.TABLE_DATALAKE_TABLE_NAME.key(), "hms_lake_table"));
        admin.alterTable(tablePath, paimonPathWithLakePath, false).get();

        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getProperties().toMap())
                .containsEntry(ConfigOptions.TABLE_DATALAKE_TABLE_NAME.key(), "hms_lake_table");
        assertThat(tableInfo.toTableDescriptor().getCustomProperties())
                .containsEntry("paimon.path", customTablePath);
        assertThatThrownBy(
                        () -> paimonCatalog.getTable(Identifier.create(DATABASE, "hms_lake_table")))
                .isInstanceOf(Catalog.TableNotExistException.class);

        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")),
                        false)
                .get();

        tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getProperties().toMap())
                .containsEntry(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                .containsEntry(ConfigOptions.TABLE_DATALAKE_TABLE_NAME.key(), "hms_lake_table");
        assertThat(tableInfo.toTableDescriptor().getCustomProperties())
                .containsEntry("paimon.path", customTablePath);

        Table paimonTable = paimonCatalog.getTable(Identifier.create(DATABASE, "hms_lake_table"));
        assertThat(paimonTable.name()).isEqualTo("hms_lake_table");
        assertThat(
                        URI.create(
                                        ((AbstractCatalog) paimonCatalog)
                                                .getTableLocation(
                                                        Identifier.create(
                                                                DATABASE, "hms_lake_table"))
                                                .toString())
                                .getPath())
                .isEqualTo(URI.create(customTablePath).getPath());
        assertThat(URI.create(paimonTable.options().get("path")).getPath())
                .isEqualTo(URI.create(customTablePath).getPath());
    }
}
