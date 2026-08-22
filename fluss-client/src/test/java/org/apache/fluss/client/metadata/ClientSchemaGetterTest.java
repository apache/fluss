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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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

    private static Schema schema(String... fields) {
        Schema.Builder builder = Schema.newBuilder();
        for (String field : fields) {
            builder.column(field, DataTypes.STRING());
        }
        return builder.build();
    }

    private static Admin adminWithSchemas(
            Map<Integer, SchemaInfo> schemasById, AtomicInteger fetchCount) {
        InvocationHandler invocationHandler =
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) {
                        if ("getTableSchema".equals(method.getName())
                                && args != null
                                && args.length == 2) {
                            fetchCount.incrementAndGet();
                            return CompletableFuture.completedFuture(schemasById.get(args[1]));
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
