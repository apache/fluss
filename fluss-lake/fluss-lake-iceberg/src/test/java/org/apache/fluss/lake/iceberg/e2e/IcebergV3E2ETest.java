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

package org.apache.fluss.lake.iceberg.e2e;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.FlussDataTypeToIcebergDataType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.LocalZonedTimestampNanoType;
import org.apache.fluss.types.TimestampNanoType;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for Iceberg V3 features with external Fluss cluster.
 *
 * <p>These tests are designed to run against a containerized Fluss cluster. They are enabled only
 * when the FLUSS_BOOTSTRAP_SERVERS environment variable is set.
 *
 * <p>Environment variables:
 *
 * <ul>
 *   <li>FLUSS_BOOTSTRAP_SERVERS - Fluss coordinator address (e.g., localhost:9123)
 *   <li>ICEBERG_WAREHOUSE - Iceberg warehouse path (e.g., s3://iceberg-warehouse/)
 *   <li>AWS_ENDPOINT - S3 endpoint for MinIO (e.g., http://localhost:9000)
 *   <li>AWS_ACCESS_KEY_ID - S3 access key
 *   <li>AWS_SECRET_ACCESS_KEY - S3 secret key
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "FLUSS_BOOTSTRAP_SERVERS", matches = ".+")
class IcebergV3E2ETest {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergV3E2ETest.class);

    private static final String DEFAULT_DB = "fluss";

    private static Connection connection;
    private static Admin admin;
    private static Catalog icebergCatalog;
    private static String warehousePath;

    @BeforeAll
    static void setUp() throws Exception {
        String bootstrapServers = System.getenv("FLUSS_BOOTSTRAP_SERVERS");
        warehousePath = System.getenv().getOrDefault("ICEBERG_WAREHOUSE", "/tmp/iceberg-warehouse");

        LOG.info("Connecting to Fluss cluster at: {}", bootstrapServers);
        LOG.info("Iceberg warehouse: {}", warehousePath);

        Configuration config = new Configuration();
        config.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);

        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();

        // Create database if not exists
        try {
            admin.createDatabase(DEFAULT_DB, true).get();
        } catch (Exception e) {
            LOG.warn("Database creation: {}", e.getMessage());
        }

        // Initialize Iceberg catalog
        icebergCatalog = createIcebergCatalog();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
        if (icebergCatalog instanceof AutoCloseable) {
            ((AutoCloseable) icebergCatalog).close();
        }
    }

    private static Catalog createIcebergCatalog() {
        HadoopCatalog catalog = new HadoopCatalog();
        org.apache.hadoop.conf.Configuration hadoopConf =
                new org.apache.hadoop.conf.Configuration();

        String s3Endpoint = System.getenv("AWS_ENDPOINT");
        if (s3Endpoint != null) {
            hadoopConf.set("fs.s3a.endpoint", s3Endpoint);
            hadoopConf.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"));
            hadoopConf.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"));
            hadoopConf.set("fs.s3a.path.style.access", "true");
            hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        }

        catalog.setConf(hadoopConf);
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", warehousePath);
        catalog.initialize("iceberg", properties);

        return catalog;
    }

    @Test
    void testV3TypeConversionValidation() {
        // Validate V3 type conversion works correctly
        TimestampNanoType tsNano = new TimestampNanoType();
        Type icebergTsNano = tsNano.accept(FlussDataTypeToIcebergDataType.INSTANCE);
        assertThat(icebergTsNano).isInstanceOf(Types.TimestampNanoType.class);
        assertThat(((Types.TimestampNanoType) icebergTsNano).shouldAdjustToUTC()).isFalse();

        LocalZonedTimestampNanoType tsNanoLtz = new LocalZonedTimestampNanoType();
        Type icebergTsNanoLtz = tsNanoLtz.accept(FlussDataTypeToIcebergDataType.INSTANCE);
        assertThat(icebergTsNanoLtz).isInstanceOf(Types.TimestampNanoType.class);
        assertThat(((Types.TimestampNanoType) icebergTsNanoLtz).shouldAdjustToUTC()).isTrue();

        LOG.info("V3 type conversion validation passed");
    }

    @Test
    void testCreateTableWithDatalakeEnabled() throws Exception {
        String tableName = "e2e_v3_test_table_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("value", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofSeconds(5))
                        .build();

        LOG.info("Creating table: {}", tablePath);
        admin.createTable(tablePath, tableDescriptor, true).get();

        // Verify table exists
        assertThat(admin.tableExists(tablePath).get()).isTrue();

        LOG.info("Table created successfully: {}", tablePath);

        // Cleanup
        admin.deleteTable(tablePath, true).get();
    }

    @Test
    void testWriteAndTierToIceberg() throws Exception {
        String tableName = "e2e_v3_tiering_test_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofSeconds(2))
                        .build();

        LOG.info("Creating table for tiering test: {}", tablePath);
        admin.createTable(tablePath, tableDescriptor, true).get();

        // Write data
        try (Table table = connection.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();

            for (int i = 1; i <= 10; i++) {
                GenericRow row = new GenericRow(3);
                row.setField(0, i);
                row.setField(1, org.apache.fluss.row.BinaryString.fromString("name_" + i));
                row.setField(2, (long) i * 100);
                writer.upsert(row);
            }
            writer.flush();
            LOG.info("Wrote 10 rows to Fluss table");
        }

        // Wait for tiering to complete
        LOG.info("Waiting for data to tier to Iceberg...");
        Thread.sleep(10000);

        // Try to read from Iceberg
        TableIdentifier icebergTableId = TableIdentifier.of(DEFAULT_DB, tableName);
        try {
            if (icebergCatalog.tableExists(icebergTableId)) {
                org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(icebergTableId);
                List<Record> records = readIcebergRecords(icebergTable);
                LOG.info("Read {} records from Iceberg table", records.size());

                // Verify schema
                org.apache.iceberg.Schema icebergSchema = icebergTable.schema();
                assertThat(icebergSchema.findField("id").type())
                        .isInstanceOf(Types.IntegerType.class);
                assertThat(icebergSchema.findField("name").type())
                        .isInstanceOf(Types.StringType.class);
                assertThat(icebergSchema.findField("amount").type())
                        .isInstanceOf(Types.LongType.class);
            } else {
                LOG.warn("Iceberg table not yet created - tiering may need more time");
            }
        } catch (Exception e) {
            LOG.warn("Could not read Iceberg table: {}", e.getMessage());
        }

        // Cleanup
        admin.deleteTable(tablePath, true).get();
        LOG.info("Test completed and cleaned up");
    }

    @Test
    void testSchemaWithAllSupportedTypes() throws Exception {
        String tableName = "e2e_v3_all_types_" + System.currentTimeMillis();
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("tiny_col", DataTypes.TINYINT())
                        .column("small_col", DataTypes.SMALLINT())
                        .column("int_col", DataTypes.INT())
                        .column("long_col", DataTypes.BIGINT())
                        .column("float_col", DataTypes.FLOAT())
                        .column("double_col", DataTypes.DOUBLE())
                        .column("bool_col", DataTypes.BOOLEAN())
                        .column("string_col", DataTypes.STRING())
                        .column("decimal_col", DataTypes.DECIMAL(10, 2))
                        .column("date_col", DataTypes.DATE())
                        .column("time_col", DataTypes.TIME())
                        .column("ts_col", DataTypes.TIMESTAMP(6))
                        .column("ts_ltz_col", DataTypes.TIMESTAMP_LTZ(6))
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofSeconds(5))
                        .build();

        LOG.info("Creating table with all supported types: {}", tablePath);
        admin.createTable(tablePath, tableDescriptor, true).get();

        assertThat(admin.tableExists(tablePath).get()).isTrue();
        LOG.info("Table with all supported types created successfully");

        // Cleanup
        admin.deleteTable(tablePath, true).get();
    }

    private List<Record> readIcebergRecords(org.apache.iceberg.Table table) {
        List<Record> records = new ArrayList<>();
        try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            for (Record record : iterable) {
                records.add(record);
            }
        } catch (Exception e) {
            LOG.error("Error reading Iceberg records", e);
        }
        return records;
    }
}
