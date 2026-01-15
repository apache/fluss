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

package org.apache.fluss.flink.sink;

import org.apache.fluss.flink.sink.serializer.OrderSerializationSchema;
import org.apache.fluss.flink.source.testutils.Order;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussSinkBuilder} configuration and argument handling. */
class FlussSinkBuilderTest {
    private final String bootstrapServers = "localhost:9123";
    private final String databaseName = "testDb";
    private final String tableName = "testTable";

    private FlussSinkBuilder<Order> builder;

    @BeforeEach
    void setUp() {
        builder = new FlussSinkBuilder<>();
    }

    @Test
    void testConfigurationValidation() throws Exception {
        // Test missing bootstrap servers
        assertThatThrownBy(
                        () ->
                                new FlussSinkBuilder<Order>()
                                        .setDatabase("testDb")
                                        .setTable("testTable")
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("BootstrapServers is required but not provided.");

        // Test missing database
        assertThatThrownBy(
                        () ->
                                new FlussSinkBuilder<Order>()
                                        .setBootstrapServers(bootstrapServers)
                                        .setTable(tableName)
                                        .setSerializationSchema(new OrderSerializationSchema())
                                        .build())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Database is required but not provided.");

        // Test empty database
        assertThatThrownBy(
                        () ->
                                new FlussSinkBuilder<Order>()
                                        .setBootstrapServers(bootstrapServers)
                                        .setDatabase("")
                                        .setTable(tableName)
                                        .setSerializationSchema(new OrderSerializationSchema())
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Database cannot be empty");

        // Test missing table name
        assertThatThrownBy(
                        () ->
                                new FlussSinkBuilder<Order>()
                                        .setBootstrapServers(bootstrapServers)
                                        .setDatabase("testDb")
                                        .setSerializationSchema(new OrderSerializationSchema())
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Table name is required");

        // Test empty table name
        assertThatThrownBy(
                        () ->
                                new FlussSinkBuilder<Order>()
                                        .setBootstrapServers(bootstrapServers)
                                        .setDatabase("testDb")
                                        .setTable("")
                                        .setSerializationSchema(new OrderSerializationSchema())
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table name cannot be empty");
    }

    @Test
    void testTablePathSetting() throws Exception {
        // Using setDatabase and setTable
        builder.setBootstrapServers(bootstrapServers).setDatabase(databaseName).setTable(tableName);

        String database = getFieldValue(builder, "database");
        String tableName = getFieldValue(builder, "tableName");

        assertThat(database).isEqualTo(databaseName);
        assertThat(tableName).isEqualTo(this.tableName);
    }

    @Test
    void testConfigOptions() throws Exception {
        builder.setOption("custom.key", "custom.value");
        Map<String, String> configOptions = getFieldValue(builder, "configOptions");
        assertThat(configOptions).containsEntry("custom.key", "custom.value");

        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("option1", "value1");
        optionsMap.put("option2", "value2");

        builder.setOptions(optionsMap);
        configOptions = getFieldValue(builder, "configOptions");

        assertThat(configOptions)
                .containsEntry("custom.key", "custom.value")
                .containsEntry("option1", "value1")
                .containsEntry("option2", "value2");
    }

    @Test
    void testShuffleByBucketId() throws Exception {
        // Default should be true
        boolean shuffleByBucketId = getFieldValue(builder, "shuffleByBucketId");
        assertThat(shuffleByBucketId).isTrue();

        // Test setting to false
        builder.setShuffleByBucketId(false);
        shuffleByBucketId = getFieldValue(builder, "shuffleByBucketId");
        assertThat(shuffleByBucketId).isFalse();

        builder.setShuffleByBucketId(true);
        shuffleByBucketId = getFieldValue(builder, "shuffleByBucketId");
        assertThat(shuffleByBucketId).isTrue();
    }

    @Test
    void testBootstrapServersSetting() throws Exception {
        // Default should be null
        String bootstrapServers = getFieldValue(builder, "bootstrapServers");
        assertThat(bootstrapServers).isNull();

        // Test setting bootstrap servers
        builder.setBootstrapServers(this.bootstrapServers);
        bootstrapServers = getFieldValue(builder, "bootstrapServers");
        assertThat(bootstrapServers).isEqualTo(this.bootstrapServers);
    }

    @Test
    void testFluentChaining() {
        // Test that all methods can be chained
        FlussSinkBuilder<Order> chainedBuilder =
                new FlussSinkBuilder<Order>()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(databaseName)
                        .setTable(tableName)
                        .setOption("key1", "value1")
                        .setOptions(new HashMap<>())
                        .setShuffleByBucketId(false)
                        .setPartialUpdateColumns("id", "price");

        // Verify the builder instance is returned
        assertThat(chainedBuilder).isInstanceOf(FlussSinkBuilder.class);
    }

    @Test
    void testComputeTargetColumnIndexesFullUpdate() {
        int[] result =
                FlussSinkBuilder.computeTargetColumnIndexes(
                        Arrays.asList("id", "name", "price"), Arrays.asList("id"), null);
        assertThat(result).isNull();
    }

    @Test
    void testComputeTargetColumnIndexesValidPartialIncludesPk() {
        int[] result =
                FlussSinkBuilder.computeTargetColumnIndexes(
                        Arrays.asList("id", "name", "price", "ts"),
                        Arrays.asList("id"),
                        Arrays.asList("id", "price"));
        assertThat(result).containsExactly(0, 2);
    }

    @Test
    void testComputeTargetColumnIndexesMissingPkThrows() {
        assertThatThrownBy(
                        () ->
                                FlussSinkBuilder.computeTargetColumnIndexes(
                                        Arrays.asList("id", "name", "price"),
                                        Arrays.asList("id"),
                                        Arrays.asList("name", "price")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Partial updates must include all primary key columns");
    }

    @Test
    void testComputeTargetColumnIndexesUnknownColumnThrows() {
        assertThatThrownBy(
                        () ->
                                FlussSinkBuilder.computeTargetColumnIndexes(
                                        Arrays.asList("id", "name"),
                                        Arrays.asList("id"),
                                        Arrays.asList("id", "unknown")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found in table schema");
    }

    // Helper method to get private field values using reflection
    @SuppressWarnings("unchecked")
    private <T> T getFieldValue(Object object, String fieldName) throws Exception {
        Field field = FlussSinkBuilder.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(object);
    }
}
