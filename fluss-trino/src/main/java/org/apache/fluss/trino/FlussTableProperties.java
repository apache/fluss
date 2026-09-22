/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;

import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.fluss.config.ConfigOptions.TABLE_KV_FORMAT_VERSION;
import static org.apache.fluss.config.ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION;
import static org.apache.fluss.config.ConfigOptions.TABLE_REPLICATION_FACTOR;

/** Fluss table attributes exposed by Trino SHOW CREATE TABLE. */
final class FlussTableProperties {

    private static final String PRIMARY_KEY = "primary_key";
    private static final String PARTITIONED_BY = "partitioned_by";
    private static final String BUCKET_KEY = "bucket_key";
    private static final String BUCKET_COUNT = "bucket_count";
    private static final String REPLICATION_FACTOR = "replication_factor";
    private static final String KV_FORMAT_VERSION = "kv_format_version";
    private static final String KV_VALUE_LAYOUT_VERSION = "kv_value_layout_version";

    private static final List<PropertyMetadata<?>> TABLE_PROPERTIES =
            ImmutableList.of(
                    columnListProperty(PRIMARY_KEY, "Ordered Fluss primary key columns"),
                    columnListProperty(PARTITIONED_BY, "Ordered Fluss partition key columns"),
                    columnListProperty(BUCKET_KEY, "Ordered Fluss bucket key columns"),
                    integerProperty(BUCKET_COUNT, "Fluss table bucket count", null, false),
                    integerProperty(REPLICATION_FACTOR, "Fluss replication factor", null, false),
                    integerProperty(KV_FORMAT_VERSION, "Fluss KV format version", null, false),
                    integerProperty(
                            KV_VALUE_LAYOUT_VERSION, "Fluss KV value layout version", null, false));

    private FlussTableProperties() {}

    static List<PropertyMetadata<?>> getTableProperties() {
        return TABLE_PROPERTIES;
    }

    static Map<String, Object> fromTableInfo(TableInfo info) {
        Map<String, Object> properties = new LinkedHashMap<>();

        if (info.hasPrimaryKey()) {
            properties.put(PRIMARY_KEY, toTrinoColumnNames(info.getPrimaryKeys()));

            info.getProperties()
                    .getOptional(TABLE_KV_FORMAT_VERSION)
                    .ifPresent(value -> properties.put(KV_FORMAT_VERSION, value));

            info.getProperties()
                    .getOptional(TABLE_KV_VALUE_LAYOUT_VERSION)
                    .ifPresent(value -> properties.put(KV_VALUE_LAYOUT_VERSION, value));
        }

        if (info.isPartitioned()) {
            properties.put(PARTITIONED_BY, toTrinoColumnNames(info.getPartitionKeys()));
        }

        if (info.hasBucketKey()) {
            properties.put(BUCKET_KEY, toTrinoColumnNames(info.getBucketKeys()));
        }

        properties.put(BUCKET_COUNT, info.getNumBuckets());

        info.getProperties()
                .getOptional(TABLE_REPLICATION_FACTOR)
                .ifPresent(value -> properties.put(REPLICATION_FACTOR, value));

        return Collections.unmodifiableMap(properties);
    }

    private static PropertyMetadata<?> columnListProperty(String name, String description) {
        return new PropertyMetadata<>(
                name,
                description,
                new ArrayType(VARCHAR),
                List.class,
                null,
                false,
                value ->
                        ((List<?>) value)
                                .stream().map(String.class::cast).collect(Collectors.toList()),
                value -> value);
    }

    private static List<String> toTrinoColumnNames(List<String> columnNames) {
        return columnNames.stream()
                .map(name -> name.toLowerCase(Locale.ROOT))
                .collect(ImmutableList.toImmutableList());
    }
}
