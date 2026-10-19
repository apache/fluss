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

package org.apache.fluss.kafka;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KafkaValueConversionLayout;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies DDL-discovered JSON writes and native Arrow readback. */
class KafkaJsonProduceITCase {
    private static final String DATABASE = "fluss";
    private static final byte[] KEY = new byte[] {(byte) 0xff, 1};
    private static final String FORGED_METADATA =
            "\"_kafka_metadata\":{\"timestamp\":0,\"timestamp_type\":-1,\"partition\":99}";
    private static final RowType SCALAR_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT())
                    .field("amount", DataTypes.DECIMAL(30, 10))
                    .field("received_at", DataTypes.TIMESTAMP(6))
                    .field("emitted_at", DataTypes.TIMESTAMP_LTZ(6))
                    .field("float_value", DataTypes.FLOAT())
                    .field("double_value", DataTypes.DOUBLE())
                    .field("optional", DataTypes.STRING())
                    .field("rounded_float", DataTypes.FLOAT())
                    .build();

    private static final RowType NUMBER_TYPE =
            RowType.builder()
                    .field("float_value", DataTypes.FLOAT())
                    .field("double_value", DataTypes.DOUBLE())
                    .field("rounded_float", DataTypes.FLOAT())
                    .field("amount", DataTypes.DECIMAL(30, 10))
                    .build();
    private static final RowType CUSTOMER_TYPE =
            RowType.builder()
                    .field("name", DataTypes.STRING())
                    .field(KafkaTableSchema.KEY_COLUMN, DataTypes.INT())
                    .field(KafkaTableSchema.VALUE_COLUMN, DataTypes.INT())
                    .field(KafkaTableSchema.HEADERS_COLUMN, DataTypes.INT())
                    .field(KafkaTableSchema.METADATA_COLUMN, DataTypes.INT())
                    .field(KafkaTableSchema.RESCUE_COLUMN, DataTypes.INT())
                    .build();
    private static final RowType COMPLEX_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT())
                    .field("customer", CUSTOMER_TYPE)
                    .field("measurements", NUMBER_TYPE)
                    .field("float_values", DataTypes.ARRAY(DataTypes.FLOAT()))
                    .field("double_values", DataTypes.ARRAY(DataTypes.DOUBLE()))
                    .field("float_attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.FLOAT()))
                    .field(
                            "double_attributes",
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE()))
                    .field("items", DataTypes.ARRAY(NUMBER_TYPE))
                    .field("attributes", DataTypes.MAP(DataTypes.STRING(), NUMBER_TYPE))
                    .build();

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @ParameterizedTest
    @CsvSource({"flat", "nested"})
    void testComplexJsonRoundTripPreservesNumericValuesAndMetadata(String layout) throws Exception {
        TablePath path = TablePath.of(DATABASE, "json_complex_" + layout);
        int metadataPosition = layout.equals("nested") ? 3 : COMPLEX_TYPE.getFieldCount() + 2;
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(COMPLEX_TYPE, layout, true, false, 2));
            try (KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(2);
                String[] negativeZeros = {"-0", "-0.0", "-0e0"};
                for (int index = 0; index < negativeZeros.length; index++) {
                    assertThat(
                                    producer.send(
                                                    new ProducerRecord<>(
                                                            path.toString(),
                                                            1,
                                                            123L + index,
                                                            KEY,
                                                            bytes(
                                                                    complexJson(
                                                                            index,
                                                                            negativeZeros[index]))))
                                            .get(30, TimeUnit.SECONDS)
                                            .offset())
                            .isEqualTo(index);
                }
                read(
                        connection,
                        path,
                        1,
                        negativeZeros.length,
                        (row, index) -> {
                            assertThat(row.getBytes(0)).isEqualTo(KEY);
                            assertHeaders(row.getMap(metadataPosition - 1), false);
                            assertMetadata(row, metadataPosition, 123L + index, 0, 1);
                            InternalRow value =
                                    layout.equals("nested")
                                            ? row.getRow(1, COMPLEX_TYPE.getFieldCount())
                                            : row;
                            int offset = layout.equals("nested") ? 0 : 1;
                            assertThat(value.getInt(offset)).isEqualTo(index);
                            InternalRow customer =
                                    value.getRow(offset + 1, CUSTOMER_TYPE.getFieldCount());
                            assertThat(customer.getString(0).toString()).isEqualTo("Alice");
                            // Reserved system names are ordinary declared business fields below
                            // root.
                            for (int field = 1; field < CUSTOMER_TYPE.getFieldCount(); field++) {
                                assertThat(customer.getInt(field)).isEqualTo(field);
                            }
                            assertNumbers(value.getRow(offset + 2, NUMBER_TYPE.getFieldCount()));
                            assertThat(value.getArray(offset + 3).size()).isEqualTo(2);
                            assertNegativeZero(value.getArray(offset + 3).getFloat(0));
                            assertThat(value.getArray(offset + 3).getFloat(1))
                                    .isEqualTo(1.0000001F);
                            assertThat(value.getArray(offset + 4).size()).isEqualTo(1);
                            assertNegativeZero(value.getArray(offset + 4).getDouble(0));
                            InternalMap floats = value.getMap(offset + 5);
                            assertThat(floats.size()).isEqualTo(2);
                            assertThat(floats.keyArray().getString(0).toString()).isEqualTo("zero");
                            assertNegativeZero(floats.valueArray().getFloat(0));
                            assertThat(floats.keyArray().getString(1).toString())
                                    .isEqualTo("rounded");
                            assertThat(floats.valueArray().getFloat(1)).isEqualTo(1.0000001F);
                            InternalMap doubles = value.getMap(offset + 6);
                            assertThat(doubles.size()).isEqualTo(1);
                            assertNegativeZero(doubles.valueArray().getDouble(0));
                            assertThat(value.getArray(offset + 7).size()).isEqualTo(1);
                            assertNumbers(
                                    value.getArray(offset + 7)
                                            .getRow(0, NUMBER_TYPE.getFieldCount()));
                            InternalMap attributes = value.getMap(offset + 8);
                            assertThat(attributes.size()).isEqualTo(1);
                            assertThat(attributes.keyArray().getString(0).toString())
                                    .isEqualTo("dynamic");
                            assertNumbers(
                                    attributes.valueArray().getRow(0, NUMBER_TYPE.getFieldCount()));
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"flat,false", "flat,true", "nested,false", "nested,true"})
    void testInvalidComplexPartitionIsAtomicAndDoesNotBlockOtherPartitions(
            String layout, boolean withRescue) throws Exception {
        TablePath path =
                TablePath.of(DATABASE, "json_complex_rejections_" + layout + "_" + withRescue);
        int metadataPosition = layout.equals("nested") ? 3 : COMPLEX_TYPE.getFieldCount() + 2;
        List<String> invalidValues =
                new ArrayList<>(
                        Arrays.asList(
                                "{\"id\":1,\"customer\":{\"name\":\"Alice\",\"unknown\":1}}",
                                "{\"id\":1,\"customer\":{\"name\":[]}}",
                                "{\"id\":1,\"customer\":{\"name\":true}}",
                                "{\"id\":1,\"customer\":{\"name\":1}}",
                                "{\"id\":1,\"customer\":[]}",
                                "{\"id\":1,\"float_values\":[1,\"bad\"]}",
                                "{\"id\":1,\"double_values\":[1,\"bad\"]}",
                                "{\"id\":1,\"float_values\":{}}",
                                "{\"id\":1,\"float_attributes\":[]}",
                                "{\"id\":1,\"double_attributes\":{\"bad\":\"bad\"}}",
                                "{\"id\":1,\"items\":[{\"float_value\":\"bad\"}]}",
                                "{\"id\":1,\"items\":[{\"unknown\":1}]}",
                                "{\"id\":1,\"attributes\":{\"bad\":{\"amount\":\"bad\"}}}",
                                "{\"id\":1,\"attributes\":{\"bad\":{\"unknown\":1}}}",
                                // The escaped surrogate must not be encoded as '?' and overwrite
                                // that key.
                                "{\"id\":1,\"float_attributes\":{\"?\":1,\"\\ud800\":2}}",
                                "{\"id\":1,\"double_attributes\":{\"?\":1,\"\\udc00\":2}}"));
        if (withRescue) {
            invalidValues.removeIf(value -> value.contains("\"unknown\""));
        }
        invalidValues.add("{\"id\":1,\"measurements\":{\"float_value\":1e40}}");
        invalidValues.add(
                "{\"id\":1,\"measurements\":{\"float_value\":0,\"double_value\":0,\"rounded_float\":0,\"amount\":1.12345678901}}");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(COMPLEX_TYPE, layout, true, withRescue, 2));
            ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
            try (Socket socket = new Socket(node.host(), node.port());
                    KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(2);
                for (int index = 0; index < invalidValues.size(); index++) {
                    String name = index % 2 == 0 ? path.toString() : path.getTableName();
                    ProduceResponse response =
                            produce(
                                    socket,
                                    name,
                                    MemoryRecords.withRecords(
                                            Compression.NONE,
                                            record(complexJson(1000, "-0")),
                                            record(invalidValues.get(index))),
                                    MemoryRecords.withRecords(
                                            Compression.NONE, record(complexJson(index, "-0e0"))));
                    PartitionProduceResponse rejected = partition(response, name, 0);
                    assertThat(rejected.errorCode())
                            .as(
                                    "rejection for %s with rescue=%s",
                                    invalidValues.get(index), withRescue)
                            .isEqualTo(Errors.INVALID_RECORD.code());
                    assertThat(rejected.baseOffset()).isEqualTo(-1L);
                    PartitionProduceResponse accepted = partition(response, name, 1);
                    assertThat(accepted.errorCode()).isEqualTo(Errors.NONE.code());
                    assertThat(accepted.baseOffset()).isEqualTo(index);
                }
                assertThat(
                                producer.send(
                                                new ProducerRecord<>(
                                                        path.toString(),
                                                        0,
                                                        789L,
                                                        KEY,
                                                        bytes(complexJson(-1, "-0.0"))))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset())
                        .isZero();
                read(
                        connection,
                        path,
                        0,
                        1,
                        (row, index) -> {
                            assertThat(valueId(row, layout, COMPLEX_TYPE)).isEqualTo(-1);
                            assertMetadata(row, metadataPosition, 789L, 0, 0);
                            if (withRescue) {
                                assertThat(row.isNullAt(metadataPosition + 1)).isTrue();
                            }
                        });
                read(
                        connection,
                        path,
                        1,
                        invalidValues.size(),
                        (row, index) -> {
                            assertThat(valueId(row, layout, COMPLEX_TYPE)).isEqualTo(index);
                            assertMetadata(row, metadataPosition, 123L, 0, 1);
                            if (withRescue) {
                                assertThat(row.isNullAt(metadataPosition + 1)).isTrue();
                            }
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"flat,false", "flat,true", "nested,false", "nested,true"})
    void testScalarJsonAliasesAndOptionalSystemColumnsRoundTrip(
            String layout, boolean withSystemColumns) throws Exception {
        TablePath path = TablePath.of(DATABASE, "json_scalars_" + layout + "_" + withSystemColumns);
        TableDescriptor descriptor =
                descriptor(SCALAR_TYPE, layout, withSystemColumns, withSystemColumns, 1);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor);
            try (KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(1);
                assertThat(producer.partitionsFor(path.getTableName())).hasSize(1);
                RecordMetadata first =
                        producer.send(
                                        new ProducerRecord<>(
                                                path.toString(),
                                                0,
                                                123L,
                                                KEY,
                                                bytes(scalarJson(7, false)),
                                                Arrays.asList(
                                                        new RecordHeader("source", bytes("old")),
                                                        new RecordHeader(
                                                                "source", new byte[] {1, 2}),
                                                        new RecordHeader("nullable", null))))
                                .get(30, TimeUnit.SECONDS);
                assertThat(first.topic()).isEqualTo(path.toString());
                assertThat(first.offset()).isZero();
                // A bare alias and the full name append to the same native table and bucket.
                RecordMetadata second =
                        producer.send(
                                        new ProducerRecord<byte[], byte[]>(
                                                path.getTableName(),
                                                0,
                                                456L,
                                                null,
                                                bytes(scalarJson(8, true))))
                                .get(30, TimeUnit.SECONDS);
                assertThat(second.topic()).isEqualTo(path.getTableName());
                assertThat(second.offset()).isEqualTo(1L);
                read(
                        connection,
                        path,
                        0,
                        2,
                        (row, index) -> {
                            int valuePosition = withSystemColumns ? 1 : 0;
                            InternalRow value =
                                    layout.equals("nested")
                                            ? row.getRow(valuePosition, SCALAR_TYPE.getFieldCount())
                                            : row;
                            int fieldOffset = layout.equals("nested") ? 0 : valuePosition;
                            assertScalars(value, fieldOffset, 7 + index);
                            if (withSystemColumns) {
                                if (index == 0) {
                                    assertThat(row.getBytes(0)).isEqualTo(KEY);
                                } else {
                                    assertThat(row.isNullAt(0)).isTrue();
                                }
                                int headersPosition =
                                        valuePosition
                                                + (layout.equals("nested")
                                                        ? 1
                                                        : SCALAR_TYPE.getFieldCount());
                                assertHeaders(row.getMap(headersPosition), index == 0);
                                assertMetadata(
                                        row, headersPosition + 1, index == 0 ? 123L : 456L, 0, 0);
                                assertThat(row.isNullAt(headersPosition + 2)).isTrue();
                            }
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "flat,scalar",
        "nested,scalar",
        "flat,row",
        "nested,row",
        "flat,array",
        "nested,array",
        "flat,map",
        "nested,map"
    })
    void testNullValuesPersistWithSystemColumnsAndContinuousOffsets(String layout, String fieldType)
            throws Exception {
        TablePath path = TablePath.of(DATABASE, "json_null_values_" + layout + "_" + fieldType);
        // Null Kafka values populate nullable flat fields or the nullable nested value container.
        RowType valueType =
                RowType.builder()
                        .field("id", nullValueBusinessType(fieldType).copy(true))
                        .field("label", DataTypes.STRING())
                        .build();
        Header[] headers = {
            new RecordHeader("source", bytes("old")),
            new RecordHeader("source", new byte[] {1, 2}),
            new RecordHeader("nullable", null)
        };
        // Nullable flat fields also accept explicit JSON null and missing fields.
        SimpleRecord[] records = new SimpleRecord[layout.equals("flat") ? 7 : 5];
        for (int index = 0; index < records.length; index++) {
            String json =
                    index == 2 || index == 4
                            ? "{\"id\":"
                                    + nullValueBusinessJson(fieldType, index)
                                    + ",\"label\":\"v"
                                    + index
                                    + "\"}"
                            : null;
            if (index == 5) {
                json = "{\"id\":null,\"label\":null,\"extra\":{\"index\":5}}";
            } else if (index == 6) {
                json = "{\"extra\":{\"index\":6}}";
            } else if (index == 2) {
                json = "{\"extra\":{\"index\":2}," + json.substring(1);
            }
            records[index] =
                    new SimpleRecord(
                            (index + 1) * 10L,
                            index % 2 == 0 ? KEY : null,
                            json == null ? null : bytes(json),
                            headers);
        }
        MemoryRecords[] batches = {
            MemoryRecords.withRecords(Compression.NONE, records[0], records[1]),
            MemoryRecords.withRecords(
                    Compression.NONE, Arrays.copyOfRange(records, 2, records.length))
        };
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(valueType, layout, true, true, 2));
            ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
            try (Socket socket = new Socket(node.host(), node.port())) {
                for (int batch = 0; batch < batches.length; batch++) {
                    String topic = batch == 0 ? path.toString() : path.getTableName();
                    ProduceResponse response =
                            produce(socket, topic, batches[batch], batches[batch]);
                    for (int partition = 0; partition < 2; partition++) {
                        PartitionProduceResponse result = partition(response, topic, partition);
                        assertThat(result.errorCode()).isEqualTo(Errors.NONE.code());
                        assertThat(result.baseOffset()).isEqualTo(batch == 0 ? 0L : 2L);
                    }
                }
                for (int partition = 0; partition < 2; partition++) {
                    int expectedPartition = partition;
                    read(
                            connection,
                            path,
                            partition,
                            records.length,
                            (row, index) -> {
                                if (index % 2 == 0) {
                                    assertThat(row.getBytes(0)).isEqualTo(KEY);
                                } else {
                                    assertThat(row.isNullAt(0)).isTrue();
                                }
                                if (index == 2 || index == 4) {
                                    InternalRow value =
                                            layout.equals("nested") ? row.getRow(1, 2) : row;
                                    int start = layout.equals("nested") ? 0 : 1;
                                    assertNullValueBusinessId(value, start, fieldType, index);
                                    assertThat(value.getString(start + 1).toString())
                                            .isEqualTo("v" + index);
                                } else {
                                    assertThat(row.isNullAt(1)).isTrue();
                                    if (layout.equals("flat")) {
                                        assertThat(row.isNullAt(2)).isTrue();
                                    }
                                }
                                int headersPosition = layout.equals("nested") ? 2 : 3;
                                assertHeaders(row.getMap(headersPosition), true);
                                assertMetadata(
                                        row,
                                        headersPosition + 1,
                                        (index + 1) * 10L,
                                        0,
                                        expectedPartition);
                                if (index == 2 || index == 5 || index == 6) {
                                    assertThat(row.getString(headersPosition + 2).toString())
                                            .isEqualTo("{\"extra\":{\"index\":" + index + "}}");
                                } else {
                                    // A following null or known-only value must not reuse rescue.
                                    assertThat(row.isNullAt(headersPosition + 2)).isTrue();
                                }
                            });
                }
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "flat,scalar,false",
        "nested,scalar,false",
        "flat,row,false",
        "nested,row,false",
        "flat,array,false",
        "nested,array,false",
        "flat,map,false",
        "nested,map,false",
        "flat,scalar,true",
        "nested,scalar,true",
        "flat,row,true",
        "nested,row,true",
        "flat,array,true",
        "nested,array,true",
        "flat,map,true",
        "nested,map,true"
    })
    void testNotNullBusinessFieldsAreRejectedAtTableCreation(
            String layout, String fieldType, boolean withRescue) throws Exception {
        TablePath path =
                TablePath.of(
                        DATABASE,
                        "json_required_value_" + layout + "_" + fieldType + "_" + withRescue);
        RowType valueType =
                RowType.builder(false)
                        .field("id", nullValueBusinessType(fieldType).copy(false))
                        .build();
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            assertThatThrownBy(
                            () ->
                                    create(
                                            admin,
                                            path,
                                            descriptor(valueType, layout, true, withRescue, 2)))
                    .hasCauseInstanceOf(InvalidTableException.class)
                    .hasStackTraceContaining("nullable fields");
        }
    }

    @ParameterizedTest
    @CsvSource({"flat,false", "flat,true", "nested,false", "nested,true"})
    void testInvalidPartitionIsAtomicAndDoesNotBlockOtherPartitions(
            String layout, boolean withRescue) throws Exception {
        TablePath path = TablePath.of(DATABASE, "json_rejections_" + layout + "_" + withRescue);
        RowType valueType = RowType.builder().field("id", DataTypes.INT()).build();
        List<String> invalidValues =
                new ArrayList<>(
                        Arrays.asList(
                                "",
                                "{",
                                "null",
                                "{\"id\":\"bad\"}",
                                "{\"id\":[]}",
                                "{\"id\":1} {}",
                                "{\"id\":1,\"id\":2}"));
        if (!withRescue) {
            invalidValues.add("{\"id\":1,\"unknown\":2}");
            for (String reserved :
                    new String[] {
                        KafkaTableSchema.KEY_COLUMN,
                        KafkaTableSchema.HEADERS_COLUMN,
                        KafkaTableSchema.METADATA_COLUMN,
                        KafkaTableSchema.RESCUE_COLUMN,
                        KafkaTableSchema.VALUE_COLUMN
                    }) {
                // This also rejects the erroneous nested payload wrapper {"_kafka_value":{...}}.
                String value =
                        reserved.equals(KafkaTableSchema.METADATA_COLUMN)
                                ? "{\"timestamp\":0,\"timestamp_type\":1,\"partition\":99}"
                                : "{\"id\":2}";
                invalidValues.add("{\"id\":1,\"" + reserved + "\":" + value + "}");
            }
        }
        invalidValues.add("{" + FORGED_METADATA + ",\"id\":\"bad\"}");
        invalidValues.add("{\"id\":2147483648}");
        invalidValues.add("{\"id\":1,\"_kafka_rescue\":\"\\ud800\"}");
        invalidValues.add("{\"id\":1,\"extra\":{\"x\":1,\"x\":2}}");
        StringBuilder oversized = new StringBuilder("{\"id\":1,\"extra\":[");
        for (int i = 0; i < 10000; i++) {
            if (i > 0) {
                oversized.append(',');
            }
            oversized.append('0');
        }
        invalidValues.add(oversized.append(",0]}").toString());
        String deep = "0";
        for (int i = 0; i < 67; i++) {
            deep = "[" + deep + "]";
        }
        invalidValues.add("{\"id\":1,\"extra\":" + deep + "}");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(valueType, layout, true, withRescue, 2));
            ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
            try (Socket socket = new Socket(node.host(), node.port());
                    KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(2);
                for (int index = 0; index < invalidValues.size(); index++) {
                    String name = index % 2 == 0 ? path.toString() : path.getTableName();
                    ProduceResponse response =
                            produce(
                                    socket,
                                    name,
                                    MemoryRecords.withRecords(
                                            Compression.NONE,
                                            record("{\"id\":1000}"),
                                            record(invalidValues.get(index))),
                                    MemoryRecords.withRecords(
                                            Compression.NONE, record("{\"id\":" + index + "}")));
                    assertThat(response.data().responses()).hasSize(1);
                    assertThat(response.data().responses().find(name).partitionResponses())
                            .hasSize(2);
                    PartitionProduceResponse rejected = partition(response, name, 0);
                    assertThat(rejected.errorCode())
                            .as(
                                    "rejection for %s with rescue=%s",
                                    invalidValues.get(index), withRescue)
                            .isEqualTo(Errors.INVALID_RECORD.code());
                    assertThat(rejected.baseOffset()).isEqualTo(-1L);
                    PartitionProduceResponse accepted = partition(response, name, 1);
                    assertThat(accepted.errorCode()).isEqualTo(Errors.NONE.code());
                    assertThat(accepted.baseOffset()).isEqualTo(index);
                }
                // Known-field and JSON validation failures remain atomic with rescue enabled.
                // A later successful write must be the first actual append to the rejected bucket.
                assertThat(
                                producer.send(
                                                new ProducerRecord<>(
                                                        path.toString(),
                                                        0,
                                                        123L,
                                                        KEY,
                                                        bytes("{\"id\":-1}")))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset())
                        .isZero();
                read(
                        connection,
                        path,
                        0,
                        1,
                        (row, index) -> {
                            assertThat(valueId(row, layout)).isEqualTo(-1);
                            assertMetadata(row, 3, 123L, 0, 0);
                        });
                read(
                        connection,
                        path,
                        1,
                        invalidValues.size(),
                        (row, index) -> {
                            assertThat(valueId(row, layout)).isEqualTo(index);
                            assertMetadata(row, 3, 123L, 0, 1);
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"flat,false", "flat,true", "nested,false", "nested,true"})
    void testBatchMetadataRoundTripWithAndWithoutTimestamps(String layout, boolean withRescue)
            throws Exception {
        TablePath path = TablePath.of(DATABASE, "json_metadata_" + layout + "_" + withRescue);
        RowType valueType = RowType.builder().field("id", DataTypes.INT()).build();
        // Magic v2 batches carry CREATE_TIME or LOG_APPEND_TIME even without a timestamp.
        TimestampType[] types = {
            TimestampType.CREATE_TIME,
            TimestampType.LOG_APPEND_TIME,
            TimestampType.CREATE_TIME,
            TimestampType.LOG_APPEND_TIME
        };
        long[] expectedTimestamps = {0L, 456L, 123L, 123L, -1L, -1L, -1L, -1L};
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(valueType, layout, true, withRescue, 2));
            ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
            try (Socket socket = new Socket(node.host(), node.port())) {
                for (int batch = 0; batch < types.length; batch++) {
                    long firstTimestamp = batch < 2 ? 0L : RecordBatch.NO_TIMESTAMP;
                    long secondTimestamp = batch < 2 ? 456L : RecordBatch.NO_TIMESTAMP;
                    long appendTimestamp = batch == 1 ? 123L : RecordBatch.NO_TIMESTAMP;
                    MemoryRecords records =
                            timestampedRecords(
                                    types[batch],
                                    appendTimestamp,
                                    new SimpleRecord(
                                            firstTimestamp,
                                            KEY,
                                            bytes(idJson(batch * 2, withRescue))),
                                    new SimpleRecord(
                                            secondTimestamp,
                                            KEY,
                                            bytes(idJson(batch * 2 + 1, withRescue))));
                    String topic = batch % 2 == 0 ? path.toString() : path.getTableName();
                    ProduceResponse response = produce(socket, topic, records, records);
                    for (int partition = 0; partition < 2; partition++) {
                        PartitionProduceResponse result = partition(response, topic, partition);
                        assertThat(result.errorCode()).isEqualTo(Errors.NONE.code());
                        assertThat(result.baseOffset()).isEqualTo(batch * 2L);
                    }
                }
                for (int partition = 0; partition < 2; partition++) {
                    int expectedPartition = partition;
                    read(
                            connection,
                            path,
                            partition,
                            expectedTimestamps.length,
                            (row, index) -> {
                                assertThat(valueId(row, layout)).isEqualTo(index);
                                assertMetadata(
                                        row,
                                        3,
                                        expectedTimestamps[index],
                                        (index / 2) % 2,
                                        expectedPartition);
                                if (withRescue) {
                                    assertThat(row.getString(4).toString())
                                            .isEqualTo("{" + FORGED_METADATA + "}");
                                }
                            });
                }
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"flat,false", "flat,true", "nested,false", "nested,true"})
    void testMissingMetadataTimestampPersistsAsNull(String layout, boolean withRescue)
            throws Exception {
        TablePath path =
                TablePath.of(DATABASE, "json_required_timestamp_" + layout + "_" + withRescue);
        RowType valueType = RowType.builder().field("id", DataTypes.INT()).build();
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(valueType, layout, true, withRescue, 2));
            ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
            try (Socket socket = new Socket(node.host(), node.port());
                    KafkaProducer<byte[], byte[]> producer = producer()) {
                ProduceResponse response =
                        produce(
                                socket,
                                path.toString(),
                                timestampedRecords(
                                        TimestampType.CREATE_TIME,
                                        RecordBatch.NO_TIMESTAMP,
                                        record(idJson(1000, withRescue)),
                                        new SimpleRecord(
                                                RecordBatch.NO_TIMESTAMP,
                                                KEY,
                                                bytes(idJson(1001, withRescue)))),
                                MemoryRecords.withRecords(Compression.NONE, record("{\"id\":1}")));
                PartitionProduceResponse missingTimestamp = partition(response, path.toString(), 0);
                assertThat(missingTimestamp.errorCode()).isEqualTo(Errors.NONE.code());
                assertThat(missingTimestamp.baseOffset()).isZero();
                PartitionProduceResponse accepted = partition(response, path.toString(), 1);
                assertThat(accepted.errorCode()).isEqualTo(Errors.NONE.code());
                assertThat(accepted.baseOffset()).isZero();
                assertThat(
                                producer.send(
                                                new ProducerRecord<>(
                                                        path.getTableName(),
                                                        0,
                                                        789L,
                                                        KEY,
                                                        bytes("{\"id\":-1}")))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset())
                        .isEqualTo(2L);
                read(
                        connection,
                        path,
                        0,
                        3,
                        (row, index) -> {
                            if (index == 0) {
                                assertThat(valueId(row, layout)).isEqualTo(1000);
                                assertMetadata(row, 3, 123L, 0, 0);
                            } else if (index == 1) {
                                assertThat(valueId(row, layout)).isEqualTo(1001);
                                assertMetadata(row, 3, RecordBatch.NO_TIMESTAMP, 0, 0);
                            } else {
                                assertThat(valueId(row, layout)).isEqualTo(-1);
                                assertMetadata(row, 3, 789L, 0, 0);
                            }
                        });
                read(
                        connection,
                        path,
                        1,
                        1,
                        (row, index) -> {
                            assertThat(valueId(row, layout)).isEqualTo(1);
                            assertMetadata(row, 3, 123L, 0, 1);
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    private static String idJson(int id, boolean forgedMetadata) {
        return "{\"id\":" + id + (forgedMetadata ? "," + FORGED_METADATA : "") + "}";
    }

    private static void assertMetadata(
            InternalRow row, int position, long timestamp, int timestampType, int partition) {
        assertThat(row.isNullAt(position)).isFalse();
        InternalRow metadata = row.getRow(position, 3);
        if (timestamp == RecordBatch.NO_TIMESTAMP) {
            assertThat(metadata.isNullAt(0)).isTrue();
        } else {
            assertThat(metadata.getTimestampLtz(0, 3).getEpochMillisecond()).isEqualTo(timestamp);
        }
        assertThat(metadata.getInt(1)).isEqualTo(timestampType);
        assertThat(metadata.getInt(2)).isEqualTo(partition);
    }

    @ParameterizedTest
    @CsvSource({
        "flat,false,false",
        "flat,false,true",
        "flat,true,false",
        "flat,true,true",
        "nested,false,false",
        "nested,false,true",
        "nested,true,false",
        "nested,true,true"
    })
    void testRescueRoundTripWithReorderedSystemColumns(
            String layout, boolean withSystemColumns, boolean rescueFirst) throws Exception {
        TablePath path =
                TablePath.of(
                        DATABASE,
                        "json_rescue_" + layout + "_" + withSystemColumns + "_" + rescueFirst);
        TableDescriptor descriptor =
                reorderedRescueDescriptor(layout, withSystemColumns, rescueFirst);
        KafkaTableSchema contract = KafkaTableSchema.resolve(descriptor);
        ObjectMapper mapper = new ObjectMapper();
        List<String> payloads = new ArrayList<>();
        List<JsonNode> expected = new ArrayList<>();
        String extra = "{\"values\":[1,null,2]}";
        String[] rescueValues = {"null", "123", "\"text\"", "{\"a\":[1,null]}"};
        String[] zeros = {"-0", "-0.0", "-0e0", "-0"};
        for (int i = 0; i < rescueValues.length; i++) {
            String json =
                    complexJson(i, zeros[i])
                            .replace(
                                    "\"amount\":12345678901234567890.1234567890}",
                                    "\"amount\":12345678901234567890.1234567890,\"extra\":"
                                            + extra
                                            + "}");
            String collisions =
                    "\"_kafka_key\":{\"id\":2},\"_kafka_headers\":null,"
                            + FORGED_METADATA
                            + ",\"_kafka_value\":{\"id\":2},"
                            + "\"_kafka_rescue\":"
                            + rescueValues[i];
            payloads.add(json.substring(0, json.length() - 1) + "," + collisions + "}");
            expected.add(
                    mapper.readTree(
                            "{"
                                    + collisions
                                    + ",\"measurements\":{\"extra\":"
                                    + extra
                                    + "},\"items\":[{\"extra\":"
                                    + extra
                                    + "}],\"attributes\":{\"dynamic\":{\"extra\":"
                                    + extra
                                    + "}}}"));
        }
        payloads.add(complexJson(4, "-0e0"));
        expected.add(null);
        // These were rejection cases in the inherited no-capture implementation.
        payloads.add("{\"id\":1,\"unknown\":2}");
        expected.add(mapper.readTree("{\"unknown\":2}"));
        payloads.add("{\"id\":1,\"customer\":{\"name\":\"Alice\",\"unknown\":1}}");
        expected.add(mapper.readTree("{\"customer\":{\"unknown\":1}}"));
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor);
            try (KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(2);
                for (int i = 0; i < payloads.size(); i++) {
                    assertThat(
                                    producer.send(
                                                    new ProducerRecord<>(
                                                            path.toString(),
                                                            1,
                                                            123L,
                                                            KEY,
                                                            bytes(payloads.get(i)),
                                                            Arrays.asList(
                                                                    new RecordHeader(
                                                                            "source",
                                                                            new byte[] {1, 2}),
                                                                    new RecordHeader(
                                                                            "nullable", null))))
                                            .get(30, TimeUnit.SECONDS)
                                            .offset())
                            .isEqualTo(i);
                }
                read(
                        connection,
                        path,
                        1,
                        payloads.size(),
                        (physical, index) -> {
                            InternalRow value = jsonBusinessRow(physical, contract);
                            assertThat(value.getInt(0)).isEqualTo(index < 5 ? index : 1);
                            if (index < 5) {
                                InternalRow customer =
                                        value.getRow(1, CUSTOMER_TYPE.getFieldCount());
                                assertThat(customer.getString(0).toString()).isEqualTo("Alice");
                                for (int field = 1;
                                        field < CUSTOMER_TYPE.getFieldCount();
                                        field++) {
                                    assertThat(customer.getInt(field)).isEqualTo(field);
                                }
                                assertNumbers(value.getRow(2, NUMBER_TYPE.getFieldCount()));
                                assertNumbers(
                                        value.getArray(7).getRow(0, NUMBER_TYPE.getFieldCount()));
                                assertNumbers(
                                        value.getMap(8)
                                                .valueArray()
                                                .getRow(0, NUMBER_TYPE.getFieldCount()));
                            }
                            if (expected.get(index) == null) {
                                assertThat(physical.isNullAt(contract.rescuePosition())).isTrue();
                            } else {
                                try {
                                    assertThat(
                                                    mapper.readTree(
                                                            physical.getString(
                                                                            contract
                                                                                    .rescuePosition())
                                                                    .toString()))
                                            .isEqualTo(expected.get(index));
                                } catch (java.io.IOException e) {
                                    throw new AssertionError(e);
                                }
                            }
                            if (withSystemColumns) {
                                assertThat(physical.getBytes(contract.keyPosition()))
                                        .isEqualTo(KEY);
                                assertHeaders(physical.getMap(contract.headersPosition()), true);
                                assertMetadata(physical, contract.metadataPosition(), 123L, 0, 1);
                            }
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    private static InternalRow jsonBusinessRow(InternalRow row, KafkaTableSchema contract) {
        if (contract.valueConversionLayout() == KafkaValueConversionLayout.NESTED) {
            return row.getRow(
                    contract.valueProjection().positionAt(0), COMPLEX_TYPE.getFieldCount());
        }
        GenericRow business = new GenericRow(COMPLEX_TYPE.getFieldCount());
        for (int i = 0; i < business.getFieldCount(); i++) {
            business.setField(
                    i,
                    InternalRow.createFieldGetter(
                                    COMPLEX_TYPE.getTypeAt(i),
                                    contract.valueProjection().positionAt(i))
                            .getFieldOrNull(row));
        }
        return business;
    }

    private static TableDescriptor reorderedRescueDescriptor(
            String layout, boolean withSystemColumns, boolean rescueFirst) {
        List<Schema.Column> columns =
                new ArrayList<>(
                        descriptor(COMPLEX_TYPE, layout, withSystemColumns, false, 2)
                                .getSchema()
                                .getColumns());
        Schema.Builder schema = Schema.newBuilder();
        int rescuePosition = rescueFirst ? 0 : columns.size() / 2 + columns.size() % 2;
        for (int i = 0; i <= columns.size(); i++) {
            if (i == rescuePosition) {
                schema.column(KafkaTableSchema.RESCUE_COLUMN, DataTypes.STRING());
            }
            if (i < columns.size()) {
                schema.column(columns.get(i).getName(), columns.get(i).getDataType());
            }
        }
        return TableDescriptor.builder()
                .schema(schema.build())
                .distributedBy(2)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "parse_json")
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(), layout)
                .build();
    }

    private static void assertNegativeZero(float value) {
        assertThat(Float.floatToRawIntBits(value)).isEqualTo(Float.floatToRawIntBits(-0.0F));
    }

    private static void assertNegativeZero(double value) {
        assertThat(Double.doubleToRawLongBits(value)).isEqualTo(Double.doubleToRawLongBits(-0.0D));
    }

    private static void assertNumbers(InternalRow row) {
        assertNegativeZero(row.getFloat(0));
        assertNegativeZero(row.getDouble(1));
        assertThat(row.getFloat(2)).isEqualTo(1.0000001F);
        assertThat(row.getDecimal(3, 30, 10).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("12345678901234567890.1234567890"));
    }

    private static String complexJson(int id, String negativeZero) {
        String numbers =
                "{\"float_value\":"
                        + negativeZero
                        + ",\"double_value\":"
                        + negativeZero
                        + ",\"rounded_float\":1.0000000596046448,"
                        + "\"amount\":12345678901234567890.1234567890}";
        return "{\"id\":"
                + id
                + ",\"customer\":{\"name\":\"Alice\",\"_kafka_key\":1,\"_kafka_value\":2,"
                + "\"_kafka_headers\":3,\"_kafka_metadata\":4,\"_kafka_rescue\":5},"
                + "\"measurements\":"
                + numbers
                + ",\"float_values\":["
                + negativeZero
                + ",1.0000000596046448],\"double_values\":["
                + negativeZero
                + "],\"float_attributes\":{\"zero\":"
                + negativeZero
                + ",\"rounded\":1.0000000596046448},\"double_attributes\":{\"zero\":"
                + negativeZero
                + "},\"items\":["
                + numbers
                + "],\"attributes\":{\"dynamic\":"
                + numbers
                + "}}";
    }

    private static int valueId(InternalRow row, String layout, RowType valueType) {
        return layout.equals("nested")
                ? row.getRow(1, valueType.getFieldCount()).getInt(0)
                : row.getInt(1);
    }

    private static int valueId(InternalRow row, String layout) {
        return layout.equals("nested") ? row.getRow(1, 1).getInt(0) : row.getInt(1);
    }

    private static void assertScalars(InternalRow row, int start, int id) {
        assertThat(row.getInt(start)).isEqualTo(id);
        assertThat(row.getDecimal(start + 1, 30, 10).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("12345678901234567890.1234567890"));
        assertThat(row.getTimestampNtz(start + 2, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.parse("1969-12-31T23:59:59.999999"));
        assertThat(row.getTimestampLtz(start + 3, 6).toInstant())
                .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
        assertThat(Float.floatToRawIntBits(row.getFloat(start + 4)))
                .isEqualTo(Float.floatToRawIntBits(-0.0F));
        assertThat(Double.doubleToRawLongBits(row.getDouble(start + 5)))
                .isEqualTo(Double.doubleToRawLongBits(-0.0D));
        assertThat(row.isNullAt(start + 6)).isTrue();
        assertThat(row.getFloat(start + 7)).isEqualTo(1.0000001F);
    }

    private static void assertHeaders(InternalMap headers, boolean populated) {
        if (!populated) {
            assertThat(headers.size()).isZero();
            return;
        }
        Map<String, byte[]> values = new HashMap<>();
        for (int index = 0; index < headers.size(); index++) {
            values.put(
                    headers.keyArray().getString(index).toString(),
                    headers.valueArray().isNullAt(index)
                            ? null
                            : headers.valueArray().getBytes(index));
        }
        assertThat(values).containsOnlyKeys("source", "nullable");
        assertThat(values.get("source")).containsExactly(1, 2);
        assertThat(values.get("nullable")).isNull();
    }

    private static DataType nullValueBusinessType(String fieldType) {
        switch (fieldType) {
            case "scalar":
                return DataTypes.INT();
            case "row":
                return DataTypes.ROW(DataTypes.FIELD("value", DataTypes.INT()));
            case "array":
                return DataTypes.ARRAY(DataTypes.INT());
            case "map":
                return DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
            default:
                throw new IllegalArgumentException("Unexpected business field type: " + fieldType);
        }
    }

    private static String nullValueBusinessJson(String fieldType, int id) {
        switch (fieldType) {
            case "scalar":
                return Integer.toString(id);
            case "row":
            case "map":
                return "{\"value\":" + id + "}";
            case "array":
                return "[" + id + "]";
            default:
                throw new IllegalArgumentException("Unexpected business field type: " + fieldType);
        }
    }

    private static void assertNullValueBusinessId(
            InternalRow row, int position, String fieldType, int id) {
        switch (fieldType) {
            case "scalar":
                assertThat(row.getInt(position)).isEqualTo(id);
                break;
            case "row":
                assertThat(row.getRow(position, 1).getInt(0)).isEqualTo(id);
                break;
            case "array":
                assertThat(row.getArray(position).size()).isEqualTo(1);
                assertThat(row.getArray(position).getInt(0)).isEqualTo(id);
                break;
            case "map":
                InternalMap map = row.getMap(position);
                assertThat(map.size()).isEqualTo(1);
                assertThat(map.keyArray().getString(0).toString()).isEqualTo("value");
                assertThat(map.valueArray().getInt(0)).isEqualTo(id);
                break;
            default:
                throw new IllegalArgumentException("Unexpected business field type: " + fieldType);
        }
    }

    private static String scalarJson(int id, boolean explicitNull) {
        // Both layouts consume this same root object; nested does not require a value wrapper.
        return "{\"id\":"
                + id
                + ",\"amount\":12345678901234567890.1234567890,"
                + "\"received_at\":\"1969-12-31T23:59:59.999999\","
                + "\"emitted_at\":\"1970-01-01T07:59:59.999999+08:00\","
                + "\"float_value\":-0.0,\"double_value\":-0.0,"
                + "\"rounded_float\":1.0000000596046448"
                + (explicitNull ? ",\"optional\":null" : "")
                + "}";
    }

    private static void create(Admin admin, TablePath path, TableDescriptor descriptor)
            throws Exception {
        admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(path, descriptor, false).get();
        TableInfo table = admin.getTableInfo(path).get();
        CLUSTER.waitUntilTableReady(table.getTableId());
        for (int bucket = 0; bucket < table.getNumBuckets(); bucket++) {
            CLUSTER.waitAndGetLeaderReplica(new TableBucket(table.getTableId(), bucket));
        }
        CLUSTER.waitUntilAllGatewayHasSameMetadata();
    }

    private static TableDescriptor descriptor(
            RowType valueType,
            String layout,
            boolean withSystemColumns,
            boolean withRescue,
            int buckets) {
        Schema.Builder schema = Schema.newBuilder();
        if (withSystemColumns) {
            schema.column(KafkaTableSchema.KEY_COLUMN, DataTypes.BYTES());
        }
        if (layout.equals("nested")) {
            schema.column(KafkaTableSchema.VALUE_COLUMN, valueType);
        } else {
            for (int index = 0; index < valueType.getFieldCount(); index++) {
                schema.column(valueType.getFieldNames().get(index), valueType.getTypeAt(index));
            }
        }
        if (withSystemColumns) {
            schema.column(
                    KafkaTableSchema.HEADERS_COLUMN,
                    DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()));
            schema.column(
                    KafkaTableSchema.METADATA_COLUMN,
                    DataTypes.ROW(
                            DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                            DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                            DataTypes.FIELD("partition", DataTypes.INT())));
        }
        if (withRescue) {
            schema.column(KafkaTableSchema.RESCUE_COLUMN, DataTypes.STRING());
        }
        return TableDescriptor.builder()
                .schema(schema.build())
                .distributedBy(buckets)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "parse_json")
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(), layout)
                .build();
    }

    private static MemoryRecords timestampedRecords(
            TimestampType timestampType, long logAppendTime, SimpleRecord... records) {
        try (MemoryRecordsBuilder builder =
                MemoryRecords.builder(
                        ByteBuffer.allocate(1024),
                        RecordBatch.MAGIC_VALUE_V2,
                        Compression.NONE,
                        timestampType,
                        0L,
                        logAppendTime)) {
            for (SimpleRecord record : records) {
                builder.append(record);
            }
            return builder.build();
        }
    }

    private static SimpleRecord record(String json) {
        return new SimpleRecord(123L, KEY, json == null ? null : bytes(json), new Header[0]);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static ProduceResponse produce(
            Socket socket, String topic, MemoryRecords rejected, MemoryRecords accepted)
            throws Exception {
        TopicProduceData topicData =
                new TopicProduceData()
                        .setName(topic)
                        .setPartitionData(
                                Arrays.asList(
                                        new PartitionProduceData().setIndex(0).setRecords(rejected),
                                        new PartitionProduceData()
                                                .setIndex(1)
                                                .setRecords(accepted)));
        ProduceRequest request =
                new ProduceRequest(
                        new ProduceRequestData()
                                .setAcks((short) -1)
                                .setTimeoutMs(10000)
                                .setTopicData(
                                        new ProduceRequestData.TopicProduceDataCollection(
                                                Collections.singletonList(topicData).iterator())),
                        (short) 9);
        RequestHeader header =
                new RequestHeader(ApiKeys.PRODUCE, request.version(), "json-produce-test", 1);
        ByteBuffer encoded = request.serializeWithHeader(header);
        byte[] requestBytes = new byte[encoded.remaining()];
        encoded.get(requestBytes);
        socket.setSoTimeout(15000);
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(requestBytes.length);
        output.write(requestBytes);
        output.flush();
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int responseSize = input.readInt();
        assertThat(responseSize).isBetween(1, 1024 * 1024);
        byte[] response = new byte[responseSize];
        input.readFully(response);
        return (ProduceResponse) AbstractResponse.parseResponse(ByteBuffer.wrap(response), header);
    }

    private static PartitionProduceResponse partition(
            ProduceResponse response, String topic, int index) {
        return response.data().responses().find(topic).partitionResponses().stream()
                .filter(partition -> partition.index() == index)
                .findFirst()
                .get();
    }

    private static void read(
            Connection connection,
            TablePath path,
            int bucket,
            int expectedCount,
            BiConsumer<InternalRow, Integer> check)
            throws Exception {
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(bucket);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            int count = 0;
            while (count < expectedCount && System.nanoTime() < deadline) {
                for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                    assertThat(record.logOffset()).isEqualTo(count);
                    check.accept(record.getRow(), count);
                    count++;
                }
            }
            assertThat(count).isEqualTo(expectedCount);
            assertThat(scanner.poll(Duration.ofMillis(100))).isEmpty();
        }
    }

    private static KafkaProducer<byte[], byte[]> producer() {
        ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        return new KafkaProducer<>(config);
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        config.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);
        return config;
    }
}
