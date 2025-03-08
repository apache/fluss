/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.sink;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.lakehouse.paimon.sink.NewTablesAddedPaimonListener.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.lakehouse.paimon.sink.NewTablesAddedPaimonListener.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link NewTablesAddedPaimonListener}. */
class NewTableAddedPaimonListenerTest {

    private static final String DATABASE = "fluss";

    private static final int BUCKET_NUM = 3;

    @Test
    void testNewTableAdded(@TempDir Path tmpDir) throws Exception {
        // first init paimon catalog
        Map<String, String> paimonConf = new HashMap<>();
        paimonConf.put("type", "filesystem");
        paimonConf.put("warehouse", tmpDir.toString());
        Catalog paimonCatalog =
                CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(paimonConf)));

        // create a new table added listener
        NewTablesAddedPaimonListener newTableAddedPaimonListener =
                new NewTablesAddedPaimonListener(Configuration.fromMap(paimonConf));

        Map<String, String> customProperties = new HashMap<>();
        customProperties.put("k1", "v1");
        customProperties.put("k2", "v2");
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

        // test log no bucket key table
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

        Identifier paimonLogTableId = Identifier.create(DATABASE, logTablePath.getTableName());
        Identifier paimonLogUnBucketKeyTableId =
                Identifier.create(DATABASE, logNoBucketKeyTablePath.getTableName());
        Identifier paimonPkTableId = Identifier.create(DATABASE, pkTablePath.getTableName());

        // then, add the two tables
        long currentMillis = System.currentTimeMillis();
        newTableAddedPaimonListener.onNewTablesAdded(
                Arrays.asList(
                        TableInfo.of(logTablePath, 1L, 1, logTable, currentMillis, currentMillis),
                        TableInfo.of(pkTablePath, 2L, 1, pkTable, currentMillis, currentMillis),
                        TableInfo.of(
                                logNoBucketKeyTablePath,
                                3L,
                                1,
                                logNoBucketKeyTable,
                                currentMillis,
                                currentMillis)));

        Table paimonPkTable = paimonCatalog.getTable(paimonPkTableId);
        // check the gotten pk table
        verifyPaimonTable(
                paimonPkTable,
                pkTable,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT().notNull(),
                            org.apache.paimon.types.DataTypes.STRING()
                        },
                        new String[] {"pk_c1", "pk_c2"}),
                "pk_c1",
                BUCKET_NUM,
                customProperties);

        Table paimonLogTable = paimonCatalog.getTable(paimonLogTableId);
        // check the gotten log table
        verifyPaimonTable(
                paimonLogTable,
                logTable,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            // for __offset, __timestamp
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()
                        },
                        new String[] {
                            "log_c1", "log_c2", OFFSET_COLUMN_NAME, TIMESTAMP_COLUMN_NAME
                        }),
                "log_c1,log_c2",
                BUCKET_NUM,
                customProperties);

        Table paimonLogNoBucketKeyTable = paimonCatalog.getTable(paimonLogUnBucketKeyTableId);

        verifyPaimonTable(
                paimonLogNoBucketKeyTable,
                logNoBucketKeyTable,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            // for __offset, __timestamp
                            org.apache.paimon.types.DataTypes.BIGINT(),
                            org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()
                        },
                        new String[] {
                            "log_c1", "log_c2", OFFSET_COLUMN_NAME, TIMESTAMP_COLUMN_NAME
                        }),
                null,
                BUCKET_NUM,
                customProperties);

        // create partitioned table
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

        // then, add the partitioned table
        newTableAddedPaimonListener.onNewTablesAdded(
                Collections.singletonList(
                        TableInfo.of(
                                partitionedTablePath,
                                4L,
                                1,
                                partitionedTableDescriptor,
                                currentMillis,
                                currentMillis)));

        Identifier paimonPartitionedTableId =
                Identifier.create(DATABASE, partitionedTablePath.getTableName());

        Table paimonPartitionedTable = paimonCatalog.getTable(paimonPartitionedTableId);
        // check the gotten partitioned table
        verifyPaimonTable(
                paimonPartitionedTable,
                partitionedTableDescriptor,
                RowType.of(
                        new DataType[] {
                            org.apache.paimon.types.DataTypes.INT().notNull(),
                            org.apache.paimon.types.DataTypes.STRING(),
                            org.apache.paimon.types.DataTypes.STRING().notNull()
                        },
                        new String[] {"c1", "c2", "c3"}),
                "c1",
                BUCKET_NUM,
                customProperties);
    }

    private void verifyPaimonTable(
            Table paimonTable,
            TableDescriptor flussTable,
            RowType expectedRowType,
            String expectedBucketKey,
            int bucketNum,
            Map<String, String> expectedCustomProperties) {
        // check pk
        if (!paimonTable.primaryKeys().isEmpty()) {
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

        // check custom properties
        assertThat(paimonTable.options()).containsAllEntriesOf(expectedCustomProperties);

        // now, check schema
        RowType paimonRowType = paimonTable.rowType();
        assertThat(paimonRowType).isEqualTo(expectedRowType);
    }
}
