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
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.LakeTableAlreadyExistException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.PAIMON_UNSETTABLE_OPTIONS;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for create lake enabled table with paimon as lake storage. */
class LakeEnabledTableCreateITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private static final String DATABASE = "fluss";

    private static Catalog paimonCatalog;
    private static final int BUCKET_NUM = 3;

    private Connection conn;
    private Admin admin;

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
        conf.setString("datalake.paimon.metastore", "filesystem");
        String warehousePath;
        try {
            warehousePath =
                    Files.createTempDirectory("fluss-testing-datalake-enabled")
                            .resolve("warehouse")
                            .toString();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create warehouse path");
        }
        conf.setString("datalake.paimon.warehouse", warehousePath);
        conf.setString("datalake.paimon.cache-enabled", "false");
        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(extractLakeProperties(conf))));

        return conf;
    }

    @Test
    void testCreateLakeEnabledTable() throws Exception {
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("k1", "v1");
        customProperties.put("paimon.file.format", "parquet");

        // test bucket key log table
        TableDescriptor logTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("log_c1", DataTypes.INT())
                                        .column("log_c2", DataTypes.STRING())
                                        .build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .distributedBy(BUCKET_NUM, "log_c1", "log_c2")
                        .comment("test comment")
                        .build();
        TablePath logTablePath = TablePath.of(DATABASE, "log_table");
        admin.createTable(logTablePath, logTable, false).get();
        Table paimonLogTable =
                paimonCatalog.getTable(Identifier.create(DATABASE, logTablePath.getTableName()));
        // check the gotten log table
        verifyPaimonTable(
                paimonLogTable,
                logTable,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            // for __bucket, __offset, __timestamp
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS()
                        },
                        new String[] {
                            "log_c1",
                            "log_c2",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        }),
                "log_c1,log_c2",
                BUCKET_NUM);

        TableDescriptor logNoBucketKeyTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("log_c1", DataTypes.INT())
                                        .column("log_c2", DataTypes.STRING())
                                        .build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .distributedBy(BUCKET_NUM)
                        .build();
        TablePath logNoBucketKeyTablePath = TablePath.of(DATABASE, "log_un_bucket_key_table");
        admin.createTable(logNoBucketKeyTablePath, logNoBucketKeyTable, false).get();
        paimonLogTable =
                paimonCatalog.getTable(
                        Identifier.create(DATABASE, logNoBucketKeyTablePath.getTableName()));
        verifyPaimonTable(
                paimonLogTable,
                logNoBucketKeyTable,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            // for __bucket, __offset, __timestamp
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS()
                        },
                        new String[] {
                            "log_c1",
                            "log_c2",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        }),
                null,
                BUCKET_NUM);

        // test pk table
        TableDescriptor pkTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("pk_c1", DataTypes.INT())
                                        .column("pk_c2", DataTypes.STRING())
                                        .primaryKey("pk_c1")
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .build();
        TablePath pkTablePath = TablePath.of(DATABASE, "pk_table");
        admin.createTable(pkTablePath, pkTable, false).get();
        Table paimonPkTable =
                paimonCatalog.getTable(Identifier.create(DATABASE, pkTablePath.getTableName()));
        verifyPaimonTable(
                paimonPkTable,
                pkTable,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT().notNull(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            // for __bucket, __offset, __timestamp
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS()
                        },
                        new String[] {
                            "pk_c1",
                            "pk_c2",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        }),
                "pk_c1",
                BUCKET_NUM);

        // test partitioned table
        TablePath partitionedTablePath = TablePath.of(DATABASE, "partitioned_table");
        TableDescriptor partitionedTableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .column("c3", DataTypes.STRING())
                                        .primaryKey("c1", "c3")
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .partitionedBy("c3")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .build();
        admin.createTable(partitionedTablePath, partitionedTableDescriptor, false).get();
        Table paimonPartitionedTable =
                paimonCatalog.getTable(
                        Identifier.create(DATABASE, partitionedTablePath.getTableName()));
        verifyPaimonTable(
                paimonPartitionedTable,
                partitionedTableDescriptor,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT().notNull(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            org.apache.paimon.types.DataTypes.STRING().notNull(),
                            // for __bucket, __offset, __timestamp
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS()
                        },
                        new String[] {
                            "c1",
                            "c2",
                            "c3",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        }),
                "c1",
                BUCKET_NUM);
    }

    @Test
    void testCreateLakeEnabledTableWithAllTypes() throws Exception {
        TableDescriptor logTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("log_c1", DataTypes.BOOLEAN())
                                        .column("log_c2", DataTypes.TINYINT())
                                        .column("log_c3", DataTypes.SMALLINT())
                                        .column("log_c4", DataTypes.INT())
                                        .column("log_c5", DataTypes.BIGINT())
                                        .column("log_c6", DataTypes.FLOAT())
                                        .column("log_c7", DataTypes.DOUBLE())
                                        .column("log_c8", DataTypes.DECIMAL(10, 2))
                                        .column("log_c9", DataTypes.CHAR(10))
                                        .column("log_c10", DataTypes.STRING())
                                        .column("log_c11", DataTypes.BYTES())
                                        .column("log_c12", DataTypes.BINARY(5))
                                        .column("log_c13", DataTypes.DATE())
                                        .column("log_c14", DataTypes.TIME())
                                        .column("log_c15", DataTypes.TIMESTAMP())
                                        .column("log_c16", DataTypes.TIMESTAMP_LTZ())
                                        .build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();
        TablePath logTablePath = TablePath.of(DATABASE, "log_all_type_table");
        admin.createTable(logTablePath, logTable, false).get();
        Table paimonLogTable =
                paimonCatalog.getTable(Identifier.create(DATABASE, logTablePath.getTableName()));
        verifyPaimonTable(
                paimonLogTable,
                logTable,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.BOOLEAN(),
                            org.apache.paimon.types.DataTypes.TINYINT(),
                            org.apache.paimon.types.DataTypes.SMALLINT(),
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.FLOAT(),
                            org.apache.paimon.types.DataTypes.DOUBLE(),
                            org.apache.paimon.types.DataTypes.DECIMAL(10, 2),
                            org.apache.paimon.types.DataTypes.CHAR(10),
                            org.apache.paimon.types.DataTypes.STRING(),
                            org.apache.paimon.types.DataTypes.BYTES(),
                            org.apache.paimon.types.DataTypes.BINARY(5),
                            org.apache.paimon.types.DataTypes.DATE(),
                            org.apache.paimon.types.DataTypes.TIME(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                            // for __bucket, __offset, __timestamp
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS()
                        },
                        new String[] {
                            "log_c1",
                            "log_c2",
                            "log_c3",
                            "log_c4",
                            "log_c5",
                            "log_c6",
                            "log_c7",
                            "log_c8",
                            "log_c9",
                            "log_c10",
                            "log_c11",
                            "log_c12",
                            "log_c13",
                            "log_c14",
                            "log_c15",
                            "log_c16",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        }),
                null,
                BUCKET_NUM);
    }

    @Test
    void testCreateLakeEnableTableWithUnsettablePaimonOptions() {
        Map<String, String> customProperties = new HashMap<>();

        for (String key : PAIMON_UNSETTABLE_OPTIONS) {
            customProperties.clear();
            customProperties.put(key, "v");

            TableDescriptor table =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("c1", DataTypes.INT())
                                            .column("c2", DataTypes.STRING())
                                            .build())
                            .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                            .customProperties(customProperties)
                            .distributedBy(BUCKET_NUM, "c1", "c2")
                            .build();
            TablePath tablePath = TablePath.of(DATABASE, "table_unsettable_paimon_option");
            assertThatThrownBy(() -> admin.createTable(tablePath, table, false).get())
                    .cause()
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessage(
                            String.format(
                                    "The Paimon option %s will be set automatically by Fluss and should not be set manually.",
                                    key));
        }
    }

    @Test
    void testCreateLakeEnableTableWithExistLakeTable() throws Exception {
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("k1", "v1");
        customProperties.put("paimon.file.format", "parquet");

        // test for existing lake table
        TableDescriptor td =
                createTableDescriptor(
                        2,
                        BUCKET_NUM,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        customProperties,
                        false);
        TablePath tablePath = TablePath.of(DATABASE, "log_table_with_exist_lake_table");
        admin.createTable(tablePath, td, false).get();
        // drop fluss table, lake table should still exist
        admin.dropTable(tablePath, false).get();
        // create the same fluss table again should be ok
        admin.createTable(tablePath, td, false).get();
        admin.dropTable(tablePath, false).get();

        // paimon table use dynamic bucket for fluss log table without bucket keys
        // so it should be ok to create the same fluss table with a new bucket num
        td = td.withBucketCount(BUCKET_NUM + 1);
        admin.createTable(tablePath, td, false).get();
        admin.dropTable(tablePath, false).get();

        // create log table with bucket keys will throw exception
        TableDescriptor logTableWithoutBucketKeys1 =
                createTableDescriptor(
                        2,
                        BUCKET_NUM,
                        Arrays.asList("c1", "c2"),
                        Collections.emptyList(),
                        customProperties,
                        false);
        assertThatThrownBy(
                        () -> admin.createTable(tablePath, logTableWithoutBucketKeys1, false).get())
                .cause()
                .isInstanceOf(LakeTableAlreadyExistException.class)
                .hasMessage(
                        "The table `fluss`.`log_table_with_exist_lake_table` already exists in Paimon catalog, but the table schema is not compatible. "
                                + "Existing schema: UpdateSchema{fields=[`c1` STRING, `c2` INT, `__bucket` INT, `__offset` BIGINT, `__timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE], partitionKeys=[], primaryKeys=[], options={bucket=-1, fluss.table.replication.factor=1, fluss.table.datalake.enabled=true, fluss.table.datalake.format=paimon, partition.legacy-name=false, file.format=parquet, fluss.k1=v1}, comment=null}, "
                                + "new schema: UpdateSchema{fields=[`c1` STRING, `c2` INT, `__bucket` INT, `__offset` BIGINT, `__timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE], partitionKeys=[], primaryKeys=[], options={bucket=3, fluss.table.replication.factor=1, fluss.table.datalake.enabled=true, fluss.table.datalake.format=paimon, partition.legacy-name=false, bucket-key=c1,c2, file.format=parquet, fluss.k1=v1}, comment=null}. "
                                + "Please first drop the table in Paimon catalog or use a new table name.");

        // create log table with different fields will throw exception
        TableDescriptor logTableWithoutBucketKeys2 =
                createTableDescriptor(
                        3,
                        BUCKET_NUM,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        customProperties,
                        false);
        assertThatThrownBy(
                        () -> admin.createTable(tablePath, logTableWithoutBucketKeys2, false).get())
                .cause()
                .isInstanceOf(LakeTableAlreadyExistException.class)
                .hasMessage(
                        "The table `fluss`.`log_table_with_exist_lake_table` already exists in Paimon catalog, but the table schema is not compatible. "
                                + "Existing schema: UpdateSchema{fields=[`c1` STRING, `c2` INT, `__bucket` INT, `__offset` BIGINT, `__timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE], partitionKeys=[], primaryKeys=[], options={bucket=-1, fluss.table.replication.factor=1, fluss.table.datalake.enabled=true, fluss.table.datalake.format=paimon, partition.legacy-name=false, file.format=parquet, fluss.k1=v1}, comment=null}, "
                                + "new schema: UpdateSchema{fields=[`c1` STRING, `c2` INT, `c3` STRING, `__bucket` INT, `__offset` BIGINT, `__timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE], partitionKeys=[], primaryKeys=[], options={bucket=-1, fluss.table.replication.factor=1, fluss.table.datalake.enabled=true, fluss.table.datalake.format=paimon, partition.legacy-name=false, file.format=parquet, fluss.k1=v1}, comment=null}. "
                                + "Please first drop the table in Paimon catalog or use a new table name.");

        // add an insignificant option to Paimon table will be ok
        Identifier paimonTablePath =
                Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
        SchemaChange schemaChange1 = SchemaChange.setOption("any.k1", "any.v1");
        paimonCatalog.alterTable(paimonTablePath, Collections.singletonList(schemaChange1), false);
        admin.createTable(tablePath, td, false).get();
        admin.dropTable(tablePath, false).get();

        // alter a Fluss option to Paimon table will throw exception
        SchemaChange schemaChange2 = SchemaChange.setOption("fluss.k1", "v2");
        paimonCatalog.alterTable(paimonTablePath, Collections.singletonList(schemaChange2), false);
        TableDescriptor finalTd = td;
        assertThatThrownBy(() -> admin.createTable(tablePath, finalTd, false).get())
                .cause()
                .isInstanceOf(LakeTableAlreadyExistException.class)
                .hasMessageContaining(
                        "The table `fluss`.`log_table_with_exist_lake_table` already exists in Paimon catalog, "
                                + "but the table schema is not compatible.");

        // reset fluss.k1 in Paimon
        SchemaChange schemaChange3 = SchemaChange.setOption("fluss.k1", "v1");
        paimonCatalog.alterTable(paimonTablePath, Collections.singletonList(schemaChange3), false);

        // add a new Paimon option (not specified in the Fluss table) to Paimon table will be ok
        SchemaChange schemaChange4 =
                SchemaChange.setOption(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "2");
        paimonCatalog.alterTable(paimonTablePath, Collections.singletonList(schemaChange4), false);
        admin.createTable(tablePath, finalTd, false).get();
        admin.dropTable(tablePath, false).get();

        // try to create a Fluss table specify a different value of exist Paimon option will throw
        // exception
        customProperties.put("paimon.snapshot.num-retained.min", "3");
        TableDescriptor td1 =
                createTableDescriptor(
                        2,
                        BUCKET_NUM,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        customProperties,
                        false);
        assertThatThrownBy(() -> admin.createTable(tablePath, td1, false).get())
                .cause()
                .isInstanceOf(LakeTableAlreadyExistException.class)
                .hasMessageContaining(
                        "The table `fluss`.`log_table_with_exist_lake_table` already exists in Paimon catalog, "
                                + "but the table schema is not compatible.");
    }

    @Test
    void testCreateLakeEnableTableWithExistNonEmptyLakeTable() throws Exception {
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("k1", "v1");
        customProperties.put("paimon.file.format", "parquet");

        // test for existing lake table with some data
        TableDescriptor td =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .build();
        TablePath tablePath = TablePath.of(DATABASE, "log_table_with_non_empty_lake_table");
        admin.createTable(tablePath, td, false).get();
        // drop fluss table, lake table should still exist
        admin.dropTable(tablePath, false).get();
        // create the same fluss table again should be ok
        admin.createTable(tablePath, td, false).get();
        admin.dropTable(tablePath, false).get();

        // write some data to the lake table
        writeData(
                paimonCatalog.getTable(
                        Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName())));
        assertThatThrownBy(() -> admin.createTable(tablePath, td, false).get())
                .cause()
                .isInstanceOf(LakeTableAlreadyExistException.class)
                .hasMessage(
                        "The table fluss.log_table_with_non_empty_lake_table already exists in Paimon catalog, and the table is not empty. Please first drop the table in Paimon catalog or use a new table name.");
    }

    @Test
    void testAlterLakeEnabledLogTable() throws Exception {
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("k1", "v1");
        customProperties.put("paimon.file.format", "parquet");

        // create log table with lake disabled
        TableDescriptor logTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("log_c1", DataTypes.INT())
                                        .column("log_c2", DataTypes.STRING())
                                        .build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, false)
                        .customProperties(customProperties)
                        .distributedBy(BUCKET_NUM, "log_c1", "log_c2")
                        .build();
        TablePath logTablePath = TablePath.of(DATABASE, "log_table_alter");
        admin.createTable(logTablePath, logTable, false).get();
        long tableId = admin.getTableInfo(logTablePath).get().getTableId();

        assertThatThrownBy(
                        () ->
                                paimonCatalog.getTable(
                                        Identifier.create(DATABASE, logTablePath.getTableName())))
                .isInstanceOf(Catalog.TableNotExistException.class);

        // verify LogTablet datalake status is initially disabled
        verifyLogTabletDataLakeEnabled(tableId, false);

        // enable lake
        TableChange.SetOption enableLake =
                TableChange.set(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        List<TableChange> changes = Collections.singletonList(enableLake);

        admin.alterTable(logTablePath, changes, false).get();

        // verify LogTablet datalake status is enabled
        verifyLogTabletDataLakeEnabled(tableId, true);

        Identifier paimonTablePath = Identifier.create(DATABASE, logTablePath.getTableName());
        Table enabledPaimonLogTable = paimonCatalog.getTable(paimonTablePath);

        Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        TableDescriptor updatedLogTable = logTable.withProperties(updatedProperties);
        // check the gotten log table
        verifyPaimonTable(
                enabledPaimonLogTable,
                updatedLogTable,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            // for __bucket, __offset, __timestamp
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS()
                        },
                        new String[] {
                            "log_c1",
                            "log_c2",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        }),
                "log_c1,log_c2",
                BUCKET_NUM);

        // disable lake table
        TableChange.SetOption disableLake =
                TableChange.set(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false");
        changes = Collections.singletonList(disableLake);
        admin.alterTable(logTablePath, changes, false).get();
        // paimon table should still exist although lake is disabled
        paimonCatalog.getTable(paimonTablePath);

        // verify LogTablet datalake status is disabled
        verifyLogTabletDataLakeEnabled(tableId, false);

        // try to enable lake table again
        enableLake = TableChange.set(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        changes = Collections.singletonList(enableLake);
        admin.alterTable(logTablePath, changes, false).get();

        // verify LogTablet datalake status is enabled again
        verifyLogTabletDataLakeEnabled(tableId, true);

        // write some data to the lake table
        writeData(paimonCatalog.getTable(paimonTablePath));
        Optional<Snapshot> snapshot = paimonCatalog.getTable(paimonTablePath).latestSnapshot();
        assertThat(snapshot).isNotEmpty();

        // disable lake table again
        changes = Collections.singletonList(disableLake);
        admin.alterTable(logTablePath, changes, false).get();

        // verify LogTablet datalake status is disabled again
        verifyLogTabletDataLakeEnabled(tableId, false);

        // try to enable lake table again, the snapshot should not change
        changes = Collections.singletonList(enableLake);
        admin.alterTable(logTablePath, changes, false).get();
        assertThat(paimonCatalog.getTable(paimonTablePath).latestSnapshot()).isEqualTo(snapshot);

        // verify LogTablet datalake status is enabled
        verifyLogTabletDataLakeEnabled(tableId, true);
    }

    @Test
    void testThrowExceptionWhenConflictWithSystemColumn() {
        for (String systemColumn :
                Arrays.asList(BUCKET_COLUMN_NAME, OFFSET_COLUMN_NAME, TIMESTAMP_COLUMN_NAME)) {
            TableDescriptor logTable =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("log_c1", DataTypes.INT())
                                            .column(systemColumn, DataTypes.STRING())
                                            .build())
                            .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                            .build();
            TablePath logTablePath = TablePath.of(DATABASE, "log_conflict_table");
            assertThatThrownBy(() -> admin.createTable(logTablePath, logTable, false).get())
                    .cause()
                    .isInstanceOf(InvalidTableException.class)
                    .hasMessage(
                            "Column "
                                    + systemColumn
                                    + " conflicts with a system column name of paimon table, please rename the column.");
        }
    }

    @Test
    void testAlterLakeEnabledTableProperties() throws Exception {
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("k1", "v1");
        customProperties.put("paimon.file.format", "parquet");

        // create table
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(customProperties)
                        .distributedBy(BUCKET_NUM, "c1", "c2")
                        .build();
        TablePath tablePath = TablePath.of(DATABASE, "alter_table");
        admin.createTable(tablePath, tableDescriptor, false).get();
        Table paimonTable =
                paimonCatalog.getTable(Identifier.create(DATABASE, tablePath.getTableName()));
        verifyPaimonTable(
                paimonTable,
                tableDescriptor,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            // for __bucket, __offset, __timestamp
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS()
                        },
                        new String[] {
                            "c1",
                            "c2",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        }),
                "c1,c2",
                BUCKET_NUM);

        // test alter table properties
        List<TableChange> tableChanges =
                Arrays.asList(TableChange.reset("k1"), TableChange.set("k2", "v2"));
        admin.alterTable(tablePath, tableChanges, false).get();
        paimonTable = paimonCatalog.getTable(Identifier.create(DATABASE, tablePath.getTableName()));
        customProperties.remove("k1");
        customProperties.put("k2", "v2");
        tableDescriptor =
                tableDescriptor.withProperties(tableDescriptor.getProperties(), customProperties);
        verifyPaimonTable(
                paimonTable,
                tableDescriptor,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            // for __bucket, __offset, __timestamp
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS()
                        },
                        new String[] {
                            "c1",
                            "c2",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        }),
                "c1,c2",
                BUCKET_NUM);

        // test alter paimon properties, should throw exception
        tableChanges = Collections.singletonList(TableChange.set("paimon.bucket", "10"));
        List<TableChange> finalTableChanges = tableChanges;
        assertThatThrownBy(() -> admin.alterTable(tablePath, finalTableChanges, false).get())
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessage(
                        "Property 'paimon.bucket' is not supported to alter which is for datalake table.");

        // test alter table if lake table not exists
        paimonCatalog.dropTable(Identifier.create(DATABASE, tablePath.getTableName()), true);
        tableChanges = Collections.singletonList(TableChange.set("k3", "v3"));
        List<TableChange> finalTableChanges1 = tableChanges;
        assertThatThrownBy(() -> admin.alterTable(tablePath, finalTableChanges1, false).get())
                .cause()
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining(
                        "Lake table doesn't exist for lake-enabled table "
                                + tablePath
                                + ", which shouldn't be happened. Please check if the lake table was deleted manually.");

        // alter a not exist table when ignoreIfNotExists = true is ok
        admin.alterTable(TablePath.of(DATABASE, "not_exist_table"), tableChanges, true).get();
    }

    @Test
    void testEnableLakeTableAfterAlterTableProperties() throws Exception {
        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("k1", "v1");
        customProperties.put("paimon.file.format", "parquet");

        // create table
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, false)
                        .customProperties(customProperties)
                        .distributedBy(BUCKET_NUM, "c1", "c2")
                        .build();
        TablePath tablePath = TablePath.of(DATABASE, "enable_lake_table_after_alter_properties");
        admin.createTable(tablePath, tableDescriptor, false).get();
        // paimon table should not exist because lake table is disable
        assertThatThrownBy(
                        () ->
                                paimonCatalog.getTable(
                                        Identifier.create(DATABASE, tablePath.getTableName())))
                .isInstanceOf(Catalog.TableNotExistException.class)
                .hasMessage(String.format("Table %s does not exist.", tablePath));

        // alter fluss table properties
        List<TableChange> tableChanges =
                Arrays.asList(TableChange.reset("k1"), TableChange.set("k2", "v2"));
        admin.alterTable(tablePath, tableChanges, false).get();
        // enable lake table should be ok
        TableChange.SetOption enableLake =
                TableChange.set(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        admin.alterTable(tablePath, Collections.singletonList(enableLake), false).get();
        Table paimonTable =
                paimonCatalog.getTable(Identifier.create(DATABASE, tablePath.getTableName()));
        customProperties.remove("k1");
        customProperties.put("k2", "v2");
        Map<String, String> newProperties = new HashMap<>(tableDescriptor.getProperties());
        newProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        tableDescriptor = tableDescriptor.withProperties(newProperties, customProperties);
        verifyPaimonTable(
                paimonTable,
                tableDescriptor,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            // for __bucket, __offset, __timestamp
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS()
                        },
                        new String[] {
                            "c1",
                            "c2",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        }),
                "c1,c2",
                BUCKET_NUM);

        // disable lake table
        TableChange.SetOption disableLake =
                TableChange.set(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false");
        admin.alterTable(tablePath, Collections.singletonList(disableLake), false).get();

        // alter fluss table properties when lake table is disabled
        tableChanges = Collections.singletonList(TableChange.set("k2", "v22"));
        admin.alterTable(tablePath, tableChanges, false).get();

        // enable lake table again should be ok, even though the table properties have changed
        admin.alterTable(tablePath, Collections.singletonList(enableLake), false).get();
    }

    @Test
    void testAlterLakeEnabledTableSchema() throws Exception {
        // create table
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperties(new HashMap<>())
                        .distributedBy(BUCKET_NUM, "c1", "c2")
                        .build();
        TablePath tablePath = TablePath.of(DATABASE, "alter_table_schema");
        admin.createTable(tablePath, tableDescriptor, false).get();
        Table paimonTable =
                paimonCatalog.getTable(Identifier.create(DATABASE, tablePath.getTableName()));
        verifyPaimonTable(
                paimonTable,
                tableDescriptor,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            // for __bucket, __offset, __timestamp
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS()
                        },
                        new String[] {
                            "c1",
                            "c2",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        }),
                "c1,c2",
                BUCKET_NUM);

        // test alter table schema
        List<TableChange> tableChanges =
                Collections.singletonList(
                        TableChange.addColumn(
                                "c3",
                                DataTypes.INT(),
                                "c3 comment",
                                TableChange.ColumnPosition.last()));

        admin.alterTable(tablePath, tableChanges, false).get();

        Table alteredPaimonTable =
                paimonCatalog.getTable(Identifier.create(DATABASE, tablePath.getTableName()));
        // Verify the new column c3 with comment was added to Paimon table
        RowType alteredRowType = alteredPaimonTable.rowType();
        assertThat(alteredRowType.getFieldCount()).isEqualTo(6);
        assertThat(alteredRowType.getFieldNames())
                .containsExactly(
                        "c1",
                        "c2",
                        "c3",
                        BUCKET_COLUMN_NAME,
                        OFFSET_COLUMN_NAME,
                        TIMESTAMP_COLUMN_NAME);
        // Verify c3 column has the correct type and comment
        assertThat(alteredRowType.getField("c3").type())
                .isEqualTo(org.apache.paimon.types.DataTypes.INT());
        assertThat(alteredRowType.getField("c3").description()).isEqualTo("c3 comment");
    }

    @Test
    void testEnableLakeTableWithLegacySystemTimestampColumn() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "timestamp_precision_compat");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("c1", DataTypes.INT()).build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();

        admin.createTable(tablePath, tableDescriptor, false).get();

        Identifier paimonIdentifier = Identifier.create(DATABASE, tablePath.getTableName());

        // alter to TIMESTAMP_WITH_LOCAL_TIME_ZONE to mock the legacy behavior
        paimonCatalog.alterTable(
                paimonIdentifier,
                SchemaChange.updateColumnType(
                        TIMESTAMP_COLUMN_NAME,
                        org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()),
                false);

        // disable data lake
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false")),
                        false)
                .get();
        assertThat(admin.getTableInfo(tablePath).get().getTableConfig().isDataLakeEnabled())
                .isFalse();

        // enable data lake again, should still enable it
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")),
                        false)
                .get();
        assertThat(admin.getTableInfo(tablePath).get().getTableConfig().isDataLakeEnabled())
                .isTrue();
    }

    private void verifyPaimonTable(
            Table paimonTable,
            TableDescriptor flussTable,
            RowType expectedRowType,
            @Nullable String expectedBucketKey,
            int bucketNum) {
        // check pk
        if (!flussTable.hasPrimaryKey()) {
            assertThat(paimonTable.primaryKeys()).isEmpty();
        } else {
            assertThat(paimonTable.primaryKeys())
                    .isEqualTo(flussTable.getSchema().getPrimaryKey().get().getColumnNames());
        }
        // check partitioned key
        assertThat(paimonTable.partitionKeys()).isEqualTo(flussTable.getPartitionKeys());

        // check bucket num
        Options options = Options.fromMap(paimonTable.options());
        assertThat(options.get(CoreOptions.BUCKET))
                .isEqualTo(
                        expectedBucketKey == null
                                ? CoreOptions.BUCKET.defaultValue().intValue()
                                : bucketNum);
        assertThat(options.get(CoreOptions.BUCKET_KEY)).isEqualTo(expectedBucketKey);

        // check table properties
        Map<String, String> expectedProperties = new HashMap<>();

        Stream.concat(
                        flussTable.getProperties().entrySet().stream(),
                        flussTable.getCustomProperties().entrySet().stream())
                .forEach(
                        e -> {
                            String k = e.getKey();
                            String v = e.getValue();
                            if (k.startsWith("paimon.")) {
                                expectedProperties.put(k.substring("paimon.".length()), v);
                            } else {
                                expectedProperties.put("fluss." + k, v);
                            }
                        });
        assertThat(paimonTable.options()).containsAllEntriesOf(expectedProperties);

        // now, check schema
        RowType paimonRowType = paimonTable.rowType();
        assertThat(paimonRowType).isEqualTo(expectedRowType);

        assertThat(paimonTable.comment()).isEqualTo(flussTable.getComment());
    }

    private void verifyLogTabletDataLakeEnabled(long tableId, boolean isDataLakeEnabled) {
        for (int bucket = 0; bucket < BUCKET_NUM; bucket++) {
            TableBucket tb = new TableBucket(tableId, bucket);
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
                        assertThat(replica.getLogTablet().isDataLakeEnabled())
                                .isEqualTo(isDataLakeEnabled);
                    });
        }
    }

    private TableDescriptor createTableDescriptor(
            int columnNum,
            int bucketNum,
            List<String> bucketKeys,
            List<String> partitionKeys,
            Map<String, String> customProperties,
            boolean withPrimaryKeys) {
        Schema.Builder builder = Schema.newBuilder();
        for (int i = 1; i <= columnNum; i++) {
            if (i % 2 == 0) {
                builder.column("c" + i, DataTypes.INT());
            } else {
                builder.column("c" + i, DataTypes.STRING());
            }
        }
        if (withPrimaryKeys) {
            builder.primaryKey("c1");
        }

        return TableDescriptor.builder()
                .schema(builder.build())
                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                .customProperties(customProperties)
                .distributedBy(bucketNum, bucketKeys)
                .partitionedBy(partitionKeys)
                .build();
    }

    private void writeData(Table table) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {

            for (int i = 0; i < 10; i++) {
                GenericRow row =
                        GenericRow.of(
                                i,
                                BinaryString.fromString("row-" + i),
                                0,
                                (long) i,
                                Timestamp.fromEpochMillis(System.currentTimeMillis()));
                write.write(row);
            }

            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
        }
        assertThat(table.latestSnapshot()).isNotEmpty();
    }
}
