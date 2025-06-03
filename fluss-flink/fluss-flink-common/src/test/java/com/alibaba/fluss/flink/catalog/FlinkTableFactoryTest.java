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

package com.alibaba.fluss.flink.catalog;

import com.alibaba.fluss.flink.FlinkConnectorOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkTableFactory}. */
class FlinkTableFactoryTest {

    public static final ObjectIdentifier OBJECT_IDENTIFIER =
            ObjectIdentifier.of("default", "default", "t1");


    @Test
    void testOptions() {
        ResolvedSchema schema = createBasicSchema();
        Map<String, String> properties = getBasicOptions();
        properties.put("k1", "v1");
        properties.put("k2", "v2");

        // test invalid options
        assertThatThrownBy(() -> createTableSource(schema, properties))
                .cause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options:\n" +
                        "\n" +
                        "k1\n" +
                        "k2");
        // test skip validate options
        createTableSource(schema, properties);

    }

    private ResolvedSchema createBasicSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("first", DataTypes.STRING().notNull()),
                        Column.physical("second", DataTypes.INT()),
                        Column.physical("third", DataTypes.STRING().notNull())),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("PK_first_third", Arrays.asList("first", "third")));
    }

    private static Map<String, String> getBasicOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "fluss");
        options.put(BOOTSTRAP_SERVERS.key(), "0.0.0.1:9092");
        return options;
    }

    private static DynamicTableSource createTableSource(
            ResolvedSchema schema, Map<String, String> options) {
        return FactoryUtil.createDynamicTableSource(
                null,
                OBJECT_IDENTIFIER,
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(schema).build(),
                                "mock source",
                                Collections.emptyList(),
                                options),
                        schema),
                Collections.emptyMap(),
                getConfiguration(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    private static DynamicTableSink createTableSink(
            ResolvedSchema schema, Map<String, String> options) {
        return FactoryUtil.createDynamicTableSink(
                null,
                OBJECT_IDENTIFIER,
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(schema).build(),
                                "mock sink",
                                Collections.emptyList(),
                                options),
                        schema),
                Collections.emptyMap(),
                getConfiguration(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    private static Configuration getConfiguration() {
        Configuration conf = new Configuration();
        return conf;
    }

}
