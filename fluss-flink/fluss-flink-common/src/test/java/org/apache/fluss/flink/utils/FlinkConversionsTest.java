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

package org.apache.fluss.flink.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.catalog.TestSchemaResolver;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.fluss.flink.FlinkConnectorOptions.BUCKET_KEY;
import static org.apache.fluss.flink.FlinkConnectorOptions.BUCKET_NUMBER;
import static org.apache.fluss.flink.utils.CatalogTableTestUtils.addOptions;
import static org.apache.fluss.flink.utils.CatalogTableTestUtils.checkEqualsIgnoreSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkConversions}. */
class FlinkConversionsTest {

    @Test
    void testTypeConversion() {
        // create a list with all fluss data types
        List<DataType> flussTypes =
                Arrays.asList(
                        DataTypes.BOOLEAN().copy(false),
                        DataTypes.TINYINT().copy(false),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.CHAR(1),
                        DataTypes.STRING(),
                        DataTypes.DECIMAL(10, 2),
                        DataTypes.BINARY(10),
                        DataTypes.BYTES(),
                        DataTypes.DATE(),
                        DataTypes.TIME(),
                        DataTypes.TIMESTAMP(),
                        DataTypes.TIMESTAMP_LTZ(),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.STRING().copy(false)),
                                DataTypes.FIELD("b", DataTypes.INT())));

        // flink types
        List<org.apache.flink.table.types.DataType> flinkTypes =
                Arrays.asList(
                        org.apache.flink.table.api.DataTypes.BOOLEAN().notNull(),
                        org.apache.flink.table.api.DataTypes.TINYINT().notNull(),
                        org.apache.flink.table.api.DataTypes.SMALLINT(),
                        org.apache.flink.table.api.DataTypes.INT(),
                        org.apache.flink.table.api.DataTypes.BIGINT(),
                        org.apache.flink.table.api.DataTypes.FLOAT(),
                        org.apache.flink.table.api.DataTypes.DOUBLE(),
                        org.apache.flink.table.api.DataTypes.CHAR(1),
                        org.apache.flink.table.api.DataTypes.STRING(),
                        org.apache.flink.table.api.DataTypes.DECIMAL(10, 2),
                        org.apache.flink.table.api.DataTypes.BINARY(10),
                        org.apache.flink.table.api.DataTypes.BYTES(),
                        org.apache.flink.table.api.DataTypes.DATE(),
                        org.apache.flink.table.api.DataTypes.TIME(),
                        org.apache.flink.table.api.DataTypes.TIMESTAMP(),
                        org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ(),
                        org.apache.flink.table.api.DataTypes.ARRAY(
                                org.apache.flink.table.api.DataTypes.STRING()),
                        org.apache.flink.table.api.DataTypes.MAP(
                                org.apache.flink.table.api.DataTypes.INT(),
                                org.apache.flink.table.api.DataTypes.STRING()),
                        org.apache.flink.table.api.DataTypes.ROW(
                                org.apache.flink.table.api.DataTypes.FIELD(
                                        "a",
                                        org.apache.flink.table.api.DataTypes.STRING().notNull()),
                                org.apache.flink.table.api.DataTypes.FIELD(
                                        "b", org.apache.flink.table.api.DataTypes.INT())));

        // test from fluss types to flink types
        List<org.apache.flink.table.types.DataType> actualFlinkTypes = new ArrayList<>();
        for (DataType flussDataType : flussTypes) {
            actualFlinkTypes.add(FlinkConversions.toFlinkType(flussDataType));
        }
        assertThat(actualFlinkTypes).isEqualTo(flinkTypes);

        // test from flink types to fluss types
        List<DataType> actualFlussTypes = new ArrayList<>();
        for (org.apache.flink.table.types.DataType flinkDataType : flinkTypes) {
            actualFlussTypes.add(FlinkConversions.toFlussType(flinkDataType));
        }
        assertThat(actualFlussTypes).isEqualTo(flussTypes);

        // test conversion for data types not supported in Fluss
        assertThatThrownBy(() -> FlinkConversions.toFlussType(VARCHAR(10)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported data type: %s", VARCHAR(10).getLogicalType());

        assertThatThrownBy(() -> FlinkConversions.toFlussType(VARBINARY(10)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported data type: %s", VARBINARY(10).getLogicalType());
    }

    @Test
    void testTableConversion() {
        // test convert flink table to fluss table
        ResolvedExpression computeColExpr =
                new ResolvedExpressionMock(
                        org.apache.flink.table.api.DataTypes.TIMESTAMP(3), () -> "orig_ts");
        ResolvedExpression watermarkExpr =
                new ResolvedExpressionMock(
                        org.apache.flink.table.api.DataTypes.TIMESTAMP(3), () -> "orig_ts");
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical(
                                        "order_id",
                                        org.apache.flink.table.api.DataTypes.STRING().notNull()),
                                Column.physical(
                                        "orig_ts",
                                        org.apache.flink.table.api.DataTypes.TIMESTAMP()),
                                Column.computed("compute_ts", computeColExpr)),
                        Collections.singletonList(WatermarkSpec.of("orig_ts", watermarkExpr)),
                        UniqueConstraint.primaryKey(
                                "PK_order_id", Collections.singletonList("order_id")));
        Map<String, String> options = new HashMap<>();
        options.put("k1", "v1");
        options.put("k2", "v2");
        CatalogTable flinkTable =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                        "test comment",
                        Collections.emptyList(),
                        options);

        // check the converted table
        TableDescriptor flussTable =
                FlinkConversions.toFlussTable(new ResolvedCatalogTable(flinkTable, schema));
        String expectFlussTableString =
                "TableDescriptor{schema=("
                        + "order_id STRING NOT NULL,"
                        + "orig_ts TIMESTAMP(6),"
                        + "CONSTRAINT PK_order_id PRIMARY KEY (order_id)"
                        + "), comment='test comment', partitionKeys=[], "
                        + "tableDistribution={bucketKeys=[order_id] bucketCount=null}, "
                        + "properties={}, "
                        + "customProperties={schema.watermark.0.strategy.expr=orig_ts, "
                        + "schema.2.expr=orig_ts, schema.2.data-type=TIMESTAMP(3), "
                        + "schema.watermark.0.rowtime=orig_ts, "
                        + "schema.watermark.0.strategy.data-type=TIMESTAMP(3), "
                        + "k1=v1, k2=v2, "
                        + "schema.2.name=compute_ts}}";
        assertThat(flussTable.toString()).isEqualTo(expectFlussTableString);

        // test convert fluss table to flink table
        TablePath tablePath = TablePath.of("db", "table");
        long currentMillis = System.currentTimeMillis();
        TableInfo tableInfo =
                TableInfo.of(
                        tablePath,
                        1L,
                        1,
                        flussTable.withBucketCount(1),
                        currentMillis,
                        currentMillis);
        // get the converted flink table
        CatalogTable convertedFlinkTable = FlinkConversions.toFlinkTable(tableInfo);

        // resolve it and check
        TestSchemaResolver resolver = new TestSchemaResolver();
        resolver.addExpression("compute_ts", computeColExpr);
        resolver.addExpression("orig_ts", watermarkExpr);
        // check the resolved schema
        assertThat(resolver.resolve(convertedFlinkTable.getUnresolvedSchema())).isEqualTo(schema);
        // check the converted flink table is equal to the origin flink table
        // need to put bucket key option
        Map<String, String> bucketKeyOption = new HashMap<>();
        bucketKeyOption.put(BUCKET_KEY.key(), "order_id");
        bucketKeyOption.put(BUCKET_NUMBER.key(), "1");
        CatalogTable expectedTable = addOptions(flinkTable, bucketKeyOption);
        checkEqualsIgnoreSchema(convertedFlinkTable, expectedTable);
    }

    @Test
    void testTableConversionWithOptions() {
        Map<String, String> options = new HashMap<>();
        // forward table option & enum type
        options.put(ConfigOptions.TABLE_LOG_FORMAT.key(), "indexed");
        // forward client memory option
        options.put(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE.key(), "64mb");
        // forward client duration option
        options.put(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT.key(), "32s");

        ResolvedSchema schema =
                new ResolvedSchema(
                        Collections.singletonList(
                                Column.physical(
                                        "order_id",
                                        org.apache.flink.table.api.DataTypes.STRING().notNull())),
                        Collections.emptyList(),
                        null);
        CatalogTable flinkTable =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                        "test comment",
                        Collections.emptyList(),
                        options);

        TableDescriptor flussTable =
                FlinkConversions.toFlussTable(new ResolvedCatalogTable(flinkTable, schema));

        assertThat(flussTable.getProperties())
                .containsEntry(ConfigOptions.TABLE_LOG_FORMAT.key(), "indexed");
        assertThat(flussTable.getCustomProperties())
                .containsEntry(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE.key(), "64mb")
                .containsEntry(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT.key(), "32s");
    }

    @Test
    void testOptionConversions() {
        ConfigOption<?> flinkOption = FlinkConversions.toFlinkOption(ConfigOptions.TABLE_KV_FORMAT);
        assertThat(flinkOption)
                .isEqualTo(
                        org.apache.flink.configuration.ConfigOptions.key(
                                        ConfigOptions.TABLE_KV_FORMAT.key())
                                .enumType(KvFormat.class)
                                .defaultValue(KvFormat.COMPACTED)
                                .withDescription(ConfigOptions.TABLE_KV_FORMAT.description()));

        flinkOption = FlinkConversions.toFlinkOption(ConfigOptions.CLIENT_REQUEST_TIMEOUT);
        assertThat(flinkOption)
                .isEqualTo(
                        org.apache.flink.configuration.ConfigOptions.key(
                                        ConfigOptions.CLIENT_REQUEST_TIMEOUT.key())
                                .stringType()
                                .defaultValue("30 s")
                                .withDescription(
                                        ConfigOptions.CLIENT_REQUEST_TIMEOUT.description()));

        flinkOption =
                FlinkConversions.toFlinkOption(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE);
        assertThat(flinkOption)
                .isEqualTo(
                        org.apache.flink.configuration.ConfigOptions.key(
                                        ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE.key())
                                .stringType()
                                .defaultValue("64 mb")
                                .withDescription(
                                        ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE
                                                .description()));
    }
}
