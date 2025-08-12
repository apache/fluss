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

package com.alibaba.fluss.lake.lance;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.util.Arrays;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for create lake enabled table with lance as lake storage. */
class LakeEnabledTableCreateITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private static Configuration lanceConf;

    private static final String DATABASE = "fluss";

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
        lanceConf = new Configuration();
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.LANCE);
        conf.setString("datalake.format", "lance");
        String warehousePath;
        try {
            warehousePath =
                    Files.createTempDirectory("fluss-testing-datalake-enabled")
                            .resolve("warehouse")
                            .toString();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create warehouse path");
        }
        conf.setString("datalake.lance.warehouse", warehousePath);
        lanceConf.setString("warehouse", warehousePath);
        return conf;
    }

    @Test
    void testLogTable() throws Exception {
        // test bucket key log table
        TableDescriptor logTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("log_c1", DataTypes.INT())
                                        .column("log_c2", DataTypes.STRING())
                                        .column("log_c3", DataTypes.TINYINT())
                                        .column("log_c4", DataTypes.SMALLINT())
                                        .column("log_c5", DataTypes.BIGINT())
                                        .column("log_c6", DataTypes.BOOLEAN())
                                        .column("log_c7", DataTypes.FLOAT())
                                        .column("log_c8", DataTypes.DOUBLE())
                                        .column("log_c9", DataTypes.CHAR(1))
                                        .column("log_c10", DataTypes.BINARY(10))
                                        .column("log_c11", DataTypes.BYTES())
                                        .column("log_c12", DataTypes.DECIMAL(10, 2))
                                        .column("log_c13", DataTypes.DATE())
                                        .column("log_c14", DataTypes.TIME())
                                        .column("log_c15", DataTypes.TIMESTAMP_LTZ())
                                        .column("log_c16", DataTypes.TIMESTAMP())
                                        .build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .distributedBy(BUCKET_NUM, "log_c1", "log_c2")
                        .build();
        TablePath logTablePath = TablePath.of(DATABASE, "log_table");
        admin.createTable(logTablePath, logTable, false).get();
        LanceConfig config = LanceConfig.from(lanceConf.toMap(), DATABASE, "log_table");

        // check the gotten log table
        Field logC1 = new Field("log_c1", FieldType.nullable(new ArrowType.Int(4 * 8, true)), null);
        Field logC2 = new Field("log_c2", FieldType.nullable(new ArrowType.Utf8()), null);
        Field logC3 = new Field("log_c3", FieldType.nullable(new ArrowType.Int(8, true)), null);
        Field logC4 = new Field("log_c4", FieldType.nullable(new ArrowType.Int(2 * 8, true)), null);
        Field logC5 = new Field("log_c5", FieldType.nullable(new ArrowType.Int(8 * 8, true)), null);
        Field logC6 = new Field("log_c6", FieldType.nullable(new ArrowType.Bool()), null);
        Field logC7 =
                new Field(
                        "log_c7",
                        FieldType.nullable(
                                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                        null);
        Field logC8 =
                new Field(
                        "log_c8",
                        FieldType.nullable(
                                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                        null);
        Field logC9 = new Field("log_c9", FieldType.nullable(new ArrowType.Utf8()), null);
        Field logC10 =
                new Field("log_c10", FieldType.nullable(new ArrowType.FixedSizeBinary(10)), null);
        Field logC11 = new Field("log_c11", FieldType.nullable(new ArrowType.Binary()), null);
        Field logC12 = new Field("log_c12", FieldType.nullable(new ArrowType.Decimal(10, 2)), null);
        Field logC13 =
                new Field("log_c13", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
        Field logC14 =
                new Field(
                        "log_c14",
                        FieldType.nullable(new ArrowType.Time(TimeUnit.SECOND, 32)),
                        null);
        Field logC15 =
                new Field(
                        "log_c15",
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                        null);
        Field logC16 =
                new Field(
                        "log_c16",
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                        null);

        // for __bucket, __offset, __timestamp
        Field logC17 =
                new Field(
                        BUCKET_COLUMN_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field logC18 =
                new Field(
                        OFFSET_COLUMN_NAME, FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field logC19 =
                new Field(
                        TIMESTAMP_COLUMN_NAME,
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                        null);

        org.apache.arrow.vector.types.pojo.Schema expectedSchema =
                new org.apache.arrow.vector.types.pojo.Schema(
                        Arrays.asList(
                                logC1, logC2, logC3, logC4, logC5, logC6, logC7, logC8, logC9,
                                logC10, logC11, logC12, logC13, logC14, logC15, logC16, logC17,
                                logC18, logC19));
        assertThat(expectedSchema).isEqualTo(LanceDatasetAdapter.getSchema(config).get());
    }

    @Test
    void testPrimaryKeyTable() throws Exception {
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
                        .build();
        TablePath pkTablePath = TablePath.of(DATABASE, "pk_table");
        assertThatThrownBy(() -> admin.createTable(pkTablePath, pkTable, false).get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessage("Currently, we don't support tiering a primary key table to Lance");
    }
}
