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

package org.apache.fluss.client.metadata;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ClientSchemaGetter}. */
class ClientSchemaGetterTest {

    @Test
    void testGetSchemaCachesSynchronouslyFetchedSchema() {
        TablePath tablePath = TablePath.of("db", "table");
        SchemaInfo initialSchemaInfo = new SchemaInfo(schema("id"), 1);
        SchemaInfo fetchedSchemaInfo = new SchemaInfo(schema("id", "name"), 2);
        Map<Integer, SchemaInfo> schemasById = new HashMap<>();
        schemasById.put(fetchedSchemaInfo.getSchemaId(), fetchedSchemaInfo);
        AtomicInteger fetchCount = new AtomicInteger();
        Admin admin = adminWithSchemas(schemasById, fetchCount);
        ClientSchemaGetter schemaGetter =
                new ClientSchemaGetter(tablePath, initialSchemaInfo, admin);

        assertThat(schemaGetter.getSchema(fetchedSchemaInfo.getSchemaId()))
                .isEqualTo(fetchedSchemaInfo.getSchema());
        assertThat(schemaGetter.getSchema(fetchedSchemaInfo.getSchemaId()))
                .isEqualTo(fetchedSchemaInfo.getSchema());

        assertThat(fetchCount.get()).isEqualTo(1);
        assertThat(schemaGetter.getLatestSchemaInfo()).isEqualTo(fetchedSchemaInfo);
    }

    @Test
    void testRefreshLatestSchemaFetchesAndCachesIt() {
        TablePath path = TablePath.of("db", "table");
        SchemaInfo initial = new SchemaInfo(schema("id"), 1);
        SchemaInfo evolved = new SchemaInfo(schema("id", "name"), 2);
        AtomicInteger schemaFetches = new AtomicInteger();
        AtomicInteger latestFetches = new AtomicInteger();
        Admin admin =
                adminWithSchemas(
                        new HashMap<>(),
                        schemaFetches,
                        CompletableFuture.completedFuture(tableInfo(path, evolved)),
                        latestFetches,
                        null);
        ClientSchemaGetter getter = new ClientSchemaGetter(path, initial, admin);

        assertThat(getter.refreshLatestSchemaInfo()).isEqualTo(evolved);
        assertThat(getter.getLatestSchemaInfo()).isEqualTo(evolved);
        assertThat(getter.getSchema(2)).isEqualTo(evolved.getSchema());
        assertThat(latestFetches.get()).isEqualTo(1);
        assertThat(schemaFetches.get()).isZero();
    }

    @Test
    void testRefreshFailurePreservesCachedSchema() {
        TablePath path = TablePath.of("db", "table");
        SchemaInfo initial = new SchemaInfo(schema("id"), 1);
        CompletableFuture<TableInfo> response = new CompletableFuture<>();
        response.completeExceptionally(new TableNotExistException("table was dropped"));
        Admin admin =
                adminWithSchemas(
                        new HashMap<>(), new AtomicInteger(), response, new AtomicInteger(), null);
        ClientSchemaGetter getter = new ClientSchemaGetter(path, initial, admin);

        assertThatThrownBy(getter::refreshLatestSchemaInfo)
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("table was dropped");
        assertThat(getter.getLatestSchemaInfo()).isEqualTo(initial);
    }

    @Test
    void testRefreshDoesNotOverwriteNewerFetchedSchema() throws Exception {
        TablePath path = TablePath.of("db", "table");
        SchemaInfo initial = new SchemaInfo(schema("id"), 1);
        SchemaInfo refreshed = new SchemaInfo(schema("id", "name"), 2);
        SchemaInfo newer = new SchemaInfo(schema("id", "name", "extra"), 3);
        Map<Integer, SchemaInfo> schemas = new HashMap<>();
        schemas.put(3, newer);
        CompletableFuture<TableInfo> response = new CompletableFuture<>();
        CountDownLatch refreshStarted = new CountDownLatch(1);
        Admin admin =
                adminWithSchemas(
                        schemas,
                        new AtomicInteger(),
                        response,
                        new AtomicInteger(),
                        refreshStarted);
        ClientSchemaGetter getter = new ClientSchemaGetter(path, initial, admin);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<SchemaInfo> result = executor.submit(getter::refreshLatestSchemaInfo);
            assertThat(refreshStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(getter.getSchema(3)).isEqualTo(newer.getSchema());
            response.complete(tableInfo(path, refreshed));
            assertThat(result.get(10, TimeUnit.SECONDS)).isEqualTo(newer);
            assertThat(getter.getLatestSchemaInfo()).isEqualTo(newer);
        } finally {
            response.cancel(true);
            executor.shutdownNow();
        }
    }

    private static TableInfo tableInfo(TablePath path, SchemaInfo info) {
        return TableInfo.of(
                path,
                1L,
                info.getSchemaId(),
                TableDescriptor.builder().schema(info.getSchema()).distributedBy(1).build(),
                "file:///unused",
                0L,
                0L);
    }

    private static Schema schema(String... fields) {
        Schema.Builder builder = Schema.newBuilder();
        for (String field : fields) {
            builder.column(field, DataTypes.STRING());
        }
        return builder.build();
    }

    private static Admin adminWithSchemas(
            Map<Integer, SchemaInfo> schemasById, AtomicInteger fetchCount) {
        return adminWithSchemas(schemasById, fetchCount, null, new AtomicInteger(), null);
    }

    private static Admin adminWithSchemas(
            Map<Integer, SchemaInfo> schemasById,
            AtomicInteger fetchCount,
            CompletableFuture<TableInfo> latestResponse,
            AtomicInteger latestFetches,
            CountDownLatch refreshStarted) {
        InvocationHandler invocationHandler =
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) {
                        if ("getTableSchema".equals(method.getName())
                                && args != null
                                && args.length == 2) {
                            fetchCount.incrementAndGet();
                            return CompletableFuture.completedFuture(schemasById.get(args[1]));
                        } else if ("getTableInfo".equals(method.getName())) {
                            latestFetches.incrementAndGet();
                            if (refreshStarted != null) {
                                refreshStarted.countDown();
                            }
                            return latestResponse;
                        } else if ("close".equals(method.getName())) {
                            return null;
                        } else if ("toString".equals(method.getName())) {
                            return "TestingAdmin";
                        }
                        throw new UnsupportedOperationException(method.getName());
                    }
                };
        return (Admin)
                Proxy.newProxyInstance(
                        Admin.class.getClassLoader(),
                        new Class<?>[] {Admin.class},
                        invocationHandler);
    }
}
