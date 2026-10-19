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

package org.apache.fluss.trino;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableMap;
import org.apache.fluss.types.DataTypes;

import io.trino.spi.session.PropertyMetadata;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.apache.fluss.config.ConfigOptions.TABLE_KV_FORMAT_VERSION;
import static org.apache.fluss.config.ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION;
import static org.apache.fluss.config.ConfigOptions.TABLE_REPLICATION_FACTOR;
import static org.apache.fluss.trino.TestingFlussMetadata.tableInfo;
import static org.apache.fluss.trino.TestingFlussMetadata.usersTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Tests attributes exposed by SHOW CREATE TABLE. */
final class FlussTablePropertiesTest {
    @Test
    void testPrimaryKeyTableProperties() {
        TableInfo info =
                tableInfo(
                        usersTable()
                                .toTableDescriptor()
                                .withProperties(
                                        ImmutableMap.of(
                                                TABLE_REPLICATION_FACTOR.key(), "2",
                                                TABLE_KV_FORMAT_VERSION.key(), "1",
                                                TABLE_KV_VALUE_LAYOUT_VERSION.key(), "1")));
        assertThat(FlussTableProperties.fromTableInfo(info))
                .containsOnly(
                        entry("primary_key", Arrays.asList("id", "region")),
                        entry("partitioned_by", Arrays.asList("region")),
                        entry("bucket_key", Arrays.asList("id")),
                        entry("bucket_count", 4),
                        entry("replication_factor", 2),
                        entry("kv_format_version", 1),
                        entry("kv_value_layout_version", 1));
    }

    @Test
    void testLogTableOmitsKeyAndKvProperties() {
        TableInfo info =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("message", DataTypes.STRING())
                                                .build())
                                .distributedBy(8)
                                .build());
        Map<String, Object> properties = FlussTableProperties.fromTableInfo(info);
        assertThat(properties)
                .containsEntry("bucket_count", 8)
                .doesNotContainKeys(
                        "primary_key",
                        "partitioned_by",
                        "bucket_key",
                        "kv_format_version",
                        "kv_value_layout_version");
    }

    @Test
    void testExposedPropertiesAreRegistered() {
        assertThat(FlussTableProperties.getTableProperties())
                .extracting(PropertyMetadata::getName)
                .containsExactly(
                        "primary_key",
                        "partitioned_by",
                        "bucket_key",
                        "bucket_count",
                        "replication_factor",
                        "kv_format_version",
                        "kv_value_layout_version");
        for (PropertyMetadata<?> property : FlussTableProperties.getTableProperties()) {
            assertThat(property.isHidden()).isFalse();
            assertThat(property.getDefaultValue()).isNull();
        }
    }
}
