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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaTableInfoCacheTest {

    private static final TablePath TABLE_PATH = TablePath.of("kafka", "orders");

    @Test
    void testReusesUnchangedResponseAndReloadsPropertyOnlyChange() {
        KafkaTableInfoCache cache = new KafkaTableInfoCache(10);
        AtomicInteger loadCount = new AtomicInteger();
        GetTableInfoResponse firstResponse = response(1L, 1, descriptor("v1"));

        TableInfo first =
                cache.getOrLoad(
                        TABLE_PATH,
                        firstResponse,
                        () -> load(TABLE_PATH, firstResponse, loadCount));
        GetTableInfoResponse equivalentResponse = response(1L, 1, descriptor("v1"));
        TableInfo reused =
                cache.getOrLoad(
                        TABLE_PATH,
                        equivalentResponse,
                        () -> load(TABLE_PATH, equivalentResponse, loadCount));

        assertThat(reused).isSameAs(first);
        assertThat(loadCount).hasValue(1);

        // A Kafka contract property may change without a schema ID or timestamp change.
        GetTableInfoResponse changedResponse = response(1L, 1, descriptor("v2"));
        TableInfo changed =
                cache.getOrLoad(
                        TABLE_PATH,
                        changedResponse,
                        () -> load(TABLE_PATH, changedResponse, loadCount));

        assertThat(changed).isNotSameAs(first);
        assertThat(loadCount).hasValue(2);
    }

    @Test
    void testReloadsDeleteAndRecreateAndSupportsExplicitInvalidation() {
        KafkaTableInfoCache cache = new KafkaTableInfoCache(10);
        AtomicInteger loadCount = new AtomicInteger();
        GetTableInfoResponse original = response(1L, 1, descriptor("v1"));
        TableInfo first =
                cache.getOrLoad(TABLE_PATH, original, () -> load(TABLE_PATH, original, loadCount));

        GetTableInfoResponse recreated = response(2L, 1, descriptor("v1"));
        TableInfo second =
                cache.getOrLoad(
                        TABLE_PATH, recreated, () -> load(TABLE_PATH, recreated, loadCount));
        assertThat(second).isNotSameAs(first);

        cache.invalidate(TABLE_PATH);
        TableInfo third =
                cache.getOrLoad(
                        TABLE_PATH, recreated, () -> load(TABLE_PATH, recreated, loadCount));
        assertThat(third).isNotSameAs(second);
        assertThat(loadCount).hasValue(3);
    }

    @Test
    void testCacheSizeIsBounded() {
        KafkaTableInfoCache cache = new KafkaTableInfoCache(2);
        for (int i = 0; i < 10; i++) {
            TablePath path = TablePath.of("kafka", "topic_" + i);
            GetTableInfoResponse response = response(i + 1L, 1, descriptor("v1"));
            cache.getOrLoad(path, response, () -> toTableInfo(path, response));
        }

        assertThat(cache.estimatedSize()).isLessThanOrEqualTo(2);
    }

    private static TableInfo load(
            TablePath tablePath, GetTableInfoResponse response, AtomicInteger loadCount) {
        loadCount.incrementAndGet();
        return toTableInfo(tablePath, response);
    }

    private static TableInfo toTableInfo(TablePath tablePath, GetTableInfoResponse response) {
        return TableInfo.of(
                tablePath,
                response.getTableId(),
                response.getSchemaId(),
                TableDescriptor.fromJsonBytes(response.getTableJson()),
                null,
                response.getCreatedTime(),
                response.getModifiedTime(),
                response.hasBucketCountEpoch() ? response.getBucketCountEpoch() : 0L);
    }

    private static GetTableInfoResponse response(
            long tableId, int schemaId, TableDescriptor descriptor) {
        return new GetTableInfoResponse()
                .setTableId(tableId)
                .setSchemaId(schemaId)
                .setTableJson(descriptor.toJsonBytes())
                .setCreatedTime(1L)
                .setModifiedTime(1L);
    }

    private static TableDescriptor descriptor(String version) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.BYTES())
                                        .build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                KafkaValueConversionType.BINARY);
        if ("v2".equals(version)) {
            builder.property(ConfigOptions.TABLE_KAFKA_ENABLED, false);
        }
        return builder.build();
    }
}
