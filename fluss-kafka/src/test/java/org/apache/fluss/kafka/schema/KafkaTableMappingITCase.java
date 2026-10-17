/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.schema;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.AlterTableRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbAddColumn;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.json.DataTypeJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.server.coordinator.CoordinatorServer.DEFAULT_DATABASE;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateDatabaseRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDatabaseExistsRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newTableExistsRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies Kafka DDL contracts through real coordinator RPC and persisted metadata. */
class KafkaTableMappingITCase {

    private static final Configuration CONFIG = new Configuration();

    static {
        CONFIG.set(ConfigOptions.KAFKA_ENABLED, true);
        CONFIG.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "kafka_default");
        CONFIG.setString("kafka.database", "legacy_database");
    }

    @RegisterExtension
    public static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(1).setClusterConf(CONFIG).build();

    private CoordinatorGateway gateway;

    @BeforeAll
    static void verifyConfiguredDatabaseCreatedOnStartup() throws Exception {
        assertThat(
                        CLUSTER.newCoordinatorClient()
                                .databaseExists(newDatabaseExistsRequest("kafka_default"))
                                .get()
                                .isExists())
                .isTrue();
    }

    @BeforeEach
    void setup() {
        gateway = CLUSTER.newCoordinatorClient();
    }

    @Test
    void testConfiguredKafkaDatabaseAndQualifiedAccess() throws Exception {
        for (String database : Arrays.asList("kafka", "legacy_database")) {
            assertThat(gateway.databaseExists(newDatabaseExistsRequest(database)).get().isExists())
                    .isFalse();
        }
        assertThat(
                        gateway.databaseExists(newDatabaseExistsRequest(DEFAULT_DATABASE))
                                .get()
                                .isExists())
                .isTrue();
        KafkaTopicMapper mapper = new KafkaTopicMapper(CONFIG);
        TablePath sales = mapper.toTablePath("sales.orders");
        TablePath warehouse = mapper.toTablePath("warehouse.orders");
        long salesId = createTable(CLUSTER, sales, enabled().build());
        long warehouseId = createTable(CLUSTER, warehouse, enabled().build());
        assertThat(salesId).isNotEqualTo(warehouseId);
        assertThat(mapper.toTablePath("orders")).isEqualTo(TablePath.of("kafka_default", "orders"));
        // The cluster fixture removes custom databases between tests; recreate this table's
        // database through the usual DDL helper after checking startup creation in @BeforeAll.
        createTable(CLUSTER, mapper.toTablePath("orders"), enabled().build());
        assertThat(
                        gateway.databaseExists(newDatabaseExistsRequest("kafka_default"))
                                .get()
                                .isExists())
                .isTrue();
        assertThat(KafkaTableSchema.isEnabled(stored(mapper.toTablePath("orders")))).isTrue();
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "sales");
        KafkaTopicMapper aliasMapper = new KafkaTopicMapper(config);
        assertThat(info(aliasMapper.toTablePath("orders")).getTableId())
                .isEqualTo(info(sales).getTableId());
        assertThat(aliasMapper.toTopicName(sales)).isEqualTo("sales.orders");
    }

    @ParameterizedTest
    @ValueSource(strings = {"flat", "nested"})
    void testJsonCreateAndEnableRejectUnsupportedBusinessSchema(String layout) throws Exception {
        gateway.createDatabase(newCreateDatabaseRequest("ddl_json", true)).get();
        TablePath path = TablePath.of("ddl_json", layout + "_invalid");
        DataType invalid =
                DataTypes.ARRAY(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "attributes",
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))));
        TableDescriptor descriptor = jsonTable(layout, invalid).build();
        assertThatThrownBy(
                        () ->
                                gateway.createTable(newCreateTableRequest(path, descriptor, false))
                                        .get())
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(gateway.tableExists(newTableExistsRequest(path)).get().isExists()).isFalse();
        assertThat(CLUSTER.getZooKeeperClient().getCurrentSchemaId(path)).isZero();

        createTable(
                CLUSTER,
                path,
                TableDescriptor.builder(descriptor)
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, false)
                        .build());
        GetTableInfoResponse before = info(path);
        assertThatThrownBy(() -> set(path, Collections.singletonMap("table.kafka.enabled", "true")))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertUnchanged(path, before);
    }

    @ParameterizedTest
    @ValueSource(strings = {"flat", "nested"})
    void testJsonComplexSchemaRoundtripAndEnable(String layout) throws Exception {
        TablePath path = TablePath.of("ddl_json", layout + "_complex");
        DataType type =
                DataTypes.ARRAY(
                        DataTypes.MAP(
                                DataTypes.STRING(),
                                DataTypes.ROW(
                                        DataTypes.FIELD("amount", DataTypes.DECIMAL(18, 2)),
                                        DataTypes.FIELD("time", DataTypes.TIMESTAMP_LTZ(3)),
                                        DataTypes.FIELD("_kafka_key", DataTypes.BYTES()))));
        TableDescriptor descriptor =
                jsonTable(layout, type).property(ConfigOptions.TABLE_KAFKA_ENABLED, false).build();
        createTable(CLUSTER, path, descriptor);
        GetTableInfoResponse before = info(path);
        set(path, Collections.singletonMap("table.kafka.enabled", "true"));
        TableDescriptor actual = stored(path);
        assertThat(actual.getSchema()).isEqualTo(descriptor.getSchema());
        assertThat(info(path).getSchemaId()).isEqualTo(before.getSchemaId());
        Map<String, String> expected = new HashMap<>(kafkaProperties(descriptor));
        expected.put("table.kafka.enabled", "true");
        assertThat(kafkaProperties(actual)).isEqualTo(expected);
        addColumn(path, "_kafka_rescue", DataTypes.STRING());
        assertThat(info(path).getSchemaId()).isGreaterThan(before.getSchemaId());
        assertThat(stored(path).getSchema().getRowType().getTypeAt(0))
                .isEqualTo(descriptor.getSchema().getRowType().getTypeAt(0));
        assertThat(KafkaTableSchema.resolve(stored(path)).rescuePosition()).isEqualTo(1);
        assertThat(kafkaProperties(stored(path))).isEqualTo(expected);
    }

    @Test
    void testJsonSchemaAlterRejectsUnsupportedNestedMapAtomically() throws Exception {
        TablePath path = TablePath.of("ddl_json", "alter_business_type");
        createTable(CLUSTER, path, jsonTable("flat", DataTypes.STRING()).build());
        DataType invalid =
                DataTypes.ARRAY(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "attributes",
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))));
        GetTableInfoResponse before = info(path);
        assertThatThrownBy(() -> addColumn(path, "bad", invalid))
                .hasCauseInstanceOf(InvalidTableException.class)
                .hasStackTraceContaining("only supports STRING map keys");
        assertUnchanged(path, before);
        addColumn(
                path, "good", DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT())));
        assertThat(info(path).getSchemaId()).isGreaterThan(before.getSchemaId());
        set(path, Collections.singletonMap("table.kafka.enabled", "false"));
        addColumn(path, "ordinary", invalid);
        GetTableInfoResponse disabled = info(path);
        assertThat(stored(path).getSchema().getRowType().getFieldNames()).contains("ordinary");
        assertThatThrownBy(() -> set(path, Collections.singletonMap("table.kafka.enabled", "true")))
                .hasCauseInstanceOf(InvalidTableException.class)
                .hasStackTraceContaining("only supports STRING map keys");
        assertUnchanged(path, disabled);
    }

    private void assertUnchanged(TablePath path, GetTableInfoResponse before) throws Exception {
        GetTableInfoResponse after = info(path);
        assertThat(after.getTableJson()).isEqualTo(before.getTableJson());
        assertThat(after.getSchemaId()).isEqualTo(before.getSchemaId());
    }

    private static TableDescriptor.Builder jsonTable(String layout, DataType businessType) {
        Schema schema =
                "flat".equals(layout)
                        ? Schema.newBuilder().column("payload", businessType).build()
                        : Schema.newBuilder()
                                .column(
                                        "_kafka_value",
                                        DataTypes.ROW(DataTypes.FIELD("payload", businessType)))
                                .build();
        TableDescriptor.Builder builder =
                enabled()
                        .schema(schema)
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "parse_json");
        // Omitted flat layout must remain omitted through CREATE and ALTER.
        if ("nested".equals(layout)) {
            builder.property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT, layout);
        }
        return builder;
    }

    @Test
    void testOnlyRequestedKafkaPropertiesArePersisted() throws Exception {
        TablePath path = TablePath.of("ddl", "roundtrip");
        Schema schema =
                Schema.newBuilder()
                        .column("_kafka_key", DataTypes.BYTES())
                        .column(
                                "payload",
                                DataTypes.ROW(DataTypes.FIELD("nested", DataTypes.STRING())))
                        .column(
                                "_kafka_headers",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()))
                        .column("_kafka_rescue", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                enabled()
                        .schema(schema)
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "parse_json")
                        .build();
        createTable(CLUSTER, path, descriptor);
        TableDescriptor stored = stored(path);
        assertThat(stored.getSchema()).isEqualTo(schema);
        assertThat(kafkaProperties(stored)).isEqualTo(kafkaProperties(descriptor));
        assertThat(new KafkaTopicSchemaResolver().resolve(stored).valueProjection().positions())
                .containsExactly(1);
    }

    @Test
    void testEnableDisableAndRejectInvalidProperties() throws Exception {
        TablePath path = TablePath.of("ddl", "enable_disable");
        TableDescriptor disabled =
                enabled().property(ConfigOptions.TABLE_KAFKA_ENABLED, false).build();
        createTable(CLUSTER, path, disabled);
        TableDescriptor before = stored(path);
        assertThat(KafkaTableSchema.isEnabled(before)).isFalse();
        set(path, Collections.singletonMap("table.kafka.enabled", "true"));
        TableDescriptor enabled = stored(path);
        assertThat(KafkaTableSchema.isEnabled(enabled)).isTrue();
        Map<String, String> expected = new HashMap<>(before.getProperties());
        expected.put("table.kafka.enabled", "true");
        assertThat(enabled.getProperties()).isEqualTo(expected);
        assertThat(enabled.getSchema()).isEqualTo(before.getSchema());
        Map<String, String> invalid = new HashMap<>();
        invalid.put("table.kafka.convert.value.type", "string");
        invalid.put("table.kafka.convert.key.type", "binary");
        assertThatThrownBy(() -> set(path, invalid))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(stored(path)).isEqualTo(enabled);
        assertThatThrownBy(() -> reset(path, "table.kafka.convert.value.type"))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(stored(path)).isEqualTo(enabled);
        reset(path, "table.kafka.enabled");
        assertThat(KafkaTableSchema.isEnabled(stored(path))).isFalse();
        assertThat(stored(path).getProperties()).doesNotContainKey("table.kafka.enabled");
        // Disabled tables can stage incompatible Kafka conversion properties without Kafka
        // validation.
        set(path, Collections.singletonMap("table.kafka.convert.value.type", "string"));
        TableDescriptor staged = stored(path);
        assertThatThrownBy(() -> set(path, Collections.singletonMap("table.kafka.enabled", "true")))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(stored(path)).isEqualTo(staged);
    }

    @Test
    void testSchemaAlterValidatesBeforePersistenceAndCanBeDisabled() throws Exception {
        TablePath path = TablePath.of("ddl", "schema_change");
        createTable(CLUSTER, path, enabled().build());
        GetTableInfoResponse before = info(path);
        assertThatThrownBy(() -> addColumn(path, "_kafka_headers", DataTypes.STRING()))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(info(path).getTableJson()).isEqualTo(before.getTableJson());
        assertThat(info(path).getSchemaId()).isEqualTo(before.getSchemaId());
        addColumn(path, "_kafka_headers", DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()));
        assertThat(KafkaTableSchema.resolve(stored(path)).headersPosition()).isEqualTo(1);
        assertThat(info(path).getSchemaId()).isGreaterThan(before.getSchemaId());
        addColumn(path, "_kafka_metadata", metadataType());
        assertThat(KafkaTableSchema.resolve(stored(path)).metadataPosition()).isEqualTo(2);
        TableDescriptor beforeFailedRescue = stored(path);
        assertThatThrownBy(() -> addColumn(path, "_kafka_rescue", DataTypes.INT()))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(stored(path)).isEqualTo(beforeFailedRescue);
        set(path, Collections.singletonMap("table.kafka.enabled", "false"));
        addColumn(path, "unmapped_business_field", DataTypes.INT());
        assertThat(stored(path).getSchema().getRowType().getFieldNames())
                .contains("unmapped_business_field");
        assertThatThrownBy(() -> set(path, Collections.singletonMap("table.kafka.enabled", "true")))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(KafkaTableSchema.isEnabled(stored(path))).isFalse();
    }

    @Test
    void testMetadataCanBeCreatedAndEnabledWithoutOffset() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("_kafka_metadata", metadataType())
                        .column("_kafka_value", DataTypes.BYTES())
                        .build();
        TableDescriptor descriptor = enabled().schema(schema).build();
        TablePath created = TablePath.of("ddl", "metadata_create");
        createTable(CLUSTER, created, descriptor);
        assertThat(stored(created).getSchema()).isEqualTo(schema);

        TablePath altered = TablePath.of("ddl", "metadata_enable");
        createTable(
                CLUSTER,
                altered,
                TableDescriptor.builder(descriptor)
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, false)
                        .build());
        int schemaId = info(altered).getSchemaId();
        set(altered, Collections.singletonMap("table.kafka.enabled", "true"));
        assertThat(info(altered).getSchemaId()).isEqualTo(schemaId);
        assertThat(stored(altered).getSchema()).isEqualTo(schema);
        assertThat(KafkaTableSchema.resolve(stored(altered)).metadataPosition()).isZero();
    }

    @Test
    void testMetadataOffsetFieldRejectedOnCreateAndEnable() throws Exception {
        gateway.createDatabase(newCreateDatabaseRequest("ddl", true)).get();
        TablePath path = TablePath.of("ddl", "metadata_with_offset");
        Schema schema =
                Schema.newBuilder()
                        .column("_kafka_value", DataTypes.BYTES())
                        .column(
                                "_kafka_metadata",
                                DataTypes.ROW(
                                        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                                        DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                                        DataTypes.FIELD("partition", DataTypes.INT()),
                                        DataTypes.FIELD("offset", DataTypes.BIGINT())))
                        .build();
        assertThatThrownBy(
                        () ->
                                gateway.createTable(
                                                newCreateTableRequest(
                                                        path,
                                                        enabled().schema(schema).build(),
                                                        false))
                                        .get())
                .hasCauseInstanceOf(InvalidTableException.class)
                .hasStackTraceContaining(
                        "ROW<timestamp TIMESTAMP_LTZ(3), timestamp_type INT, partition INT>");
        assertThat(gateway.tableExists(newTableExistsRequest(path)).get().isExists()).isFalse();
        createTable(
                CLUSTER,
                path,
                enabled()
                        .schema(schema)
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, false)
                        .build());
        TableDescriptor before = stored(path);
        assertThatThrownBy(() -> set(path, Collections.singletonMap("table.kafka.enabled", "true")))
                .hasCauseInstanceOf(InvalidTableException.class)
                .hasStackTraceContaining(
                        "ROW<timestamp TIMESTAMP_LTZ(3), timestamp_type INT, partition INT>");
        assertThat(stored(path)).isEqualTo(before);
    }

    @Test
    void testCanonicalNameCheckedOnCreateAndEnable() throws Exception {
        String database = String.join("", Collections.nCopies(128, "d"));
        TablePath path = TablePath.of(database, String.join("", Collections.nCopies(128, "t")));
        gateway.createDatabase(newCreateDatabaseRequest(database, true)).get();
        assertThatThrownBy(
                        () ->
                                gateway.createTable(
                                                newCreateTableRequest(
                                                        path, enabled().build(), false))
                                        .get())
                .hasCauseInstanceOf(InvalidTableException.class)
                .hasStackTraceContaining("Kafka topic");
        createTable(
                CLUSTER,
                path,
                enabled().property(ConfigOptions.TABLE_KAFKA_ENABLED, false).build());
        TableDescriptor before = stored(path);
        assertThatThrownBy(() -> set(path, Collections.singletonMap("table.kafka.enabled", "true")))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(stored(path)).isEqualTo(before);
    }

    private static DataType metadataType() {
        return DataTypes.ROW(
                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                DataTypes.FIELD("partition", DataTypes.INT()));
    }

    private GetTableInfoResponse info(TablePath path) throws Exception {
        return gateway.getTableInfo(newGetTableInfoRequest(path)).get();
    }

    private TableDescriptor stored(TablePath path) throws Exception {
        return TableDescriptor.fromJsonBytes(info(path).getTableJson());
    }

    private void set(TablePath path, Map<String, String> properties) throws Exception {
        gateway.alterTable(
                        newAlterTableRequest(
                                path,
                                properties,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();
    }

    private void reset(TablePath path, String property) throws Exception {
        gateway.alterTable(
                        newAlterTableRequest(
                                path,
                                Collections.emptyMap(),
                                Collections.singletonList(property),
                                Collections.emptyList(),
                                false))
                .get();
    }

    private void addColumn(TablePath path, String name, DataType type) throws Exception {
        PbAddColumn column =
                new PbAddColumn()
                        .setColumnName(name)
                        .setDataTypeJson(
                                JsonSerdeUtils.writeValueAsBytes(type, DataTypeJsonSerde.INSTANCE))
                        .setColumnPositionType(0);
        AlterTableRequest request =
                newAlterTableRequest(
                        path,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        Collections.singletonList(column),
                        false);
        gateway.alterTable(request).get();
    }

    private static Map<String, String> kafkaProperties(TableDescriptor table) {
        Map<String, String> properties = new HashMap<>();
        table.getProperties()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith("table.kafka.")) {
                                properties.put(key, value);
                            }
                        });
        return properties;
    }

    private static TableDescriptor.Builder enabled() {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("_kafka_value", DataTypes.BYTES()).build())
                .distributedBy(1)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "binary");
    }
}
