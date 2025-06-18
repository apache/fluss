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

package com.alibaba.fluss.lake.paimon;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static com.alibaba.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
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
                            org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()
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
                            org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()
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
                            org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()
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
                            org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()
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
                            org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()
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
    }
}
