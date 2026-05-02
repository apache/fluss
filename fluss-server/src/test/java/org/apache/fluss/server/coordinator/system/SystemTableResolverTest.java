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

package org.apache.fluss.server.coordinator.system;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SystemTableResolver}. */
class SystemTableResolverTest {

    private SystemTableResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new SystemTableResolver();
    }

    @Test
    void testRegisterViewDefinitionOnly() {
        TablePath viewPath = TablePath.of("sys", "test_view");
        SystemViewDefinition definition = createTestViewDefinition(viewPath);

        resolver.registerViewDefinition(definition);

        // Definition should be visible
        assertThat(resolver.isSystemView(viewPath)).isTrue();
        assertThat(resolver.getViewDefinition(viewPath)).isPresent();
        assertThat(resolver.getViewDefinition(viewPath).get().viewPath()).isEqualTo(viewPath);

        // But no provider should be available (cannot serve data)
        assertThat(resolver.getViewProvider(viewPath)).isNotPresent();
    }

    @Test
    void testRegisterViewProvider() {
        TablePath viewPath = TablePath.of("sys", "test_view");
        SystemViewProvider provider = createTestViewProvider(viewPath);

        resolver.registerViewProvider(provider);

        // Both definition and provider should be available
        assertThat(resolver.isSystemView(viewPath)).isTrue();
        assertThat(resolver.getViewDefinition(viewPath)).isPresent();
        assertThat(resolver.getViewProvider(viewPath)).isPresent();
    }

    @Test
    void testIsSystemViewNotFound() {
        assertThat(resolver.isSystemView(TablePath.of("sys", "unknown"))).isFalse();
    }

    @Test
    void testGetViewProviderNotFound() {
        assertThat(resolver.getViewProvider(TablePath.of("sys", "unknown"))).isNotPresent();
    }

    @Test
    void testGetViewDefinitionNotFound() {
        assertThat(resolver.getViewDefinition(TablePath.of("sys", "unknown"))).isNotPresent();
    }

    @Test
    void testGetAllSystemViewNames() {
        TablePath viewPath = TablePath.of("sys", "test_view");
        resolver.registerViewDefinition(createTestViewDefinition(viewPath));

        // Only view names
        List<String> viewNames = resolver.getAllSystemViewNames();
        assertThat(viewNames).containsExactly("test_view");
    }

    @Test
    void testGetAllViewDefinitions() {
        TablePath viewPath1 = TablePath.of("sys", "view1");
        TablePath viewPath2 = TablePath.of("sys", "view2");
        resolver.registerViewDefinition(createTestViewDefinition(viewPath1));
        resolver.registerViewProvider(createTestViewProvider(viewPath2));

        List<SystemViewDefinition> definitions = resolver.getAllViewDefinitions();
        assertThat(definitions).hasSize(2);
    }

    @Test
    void testGetAllViewProviders() {
        TablePath viewPath1 = TablePath.of("sys", "view1");
        TablePath viewPath2 = TablePath.of("sys", "view2");
        // Register one as definition-only, one as full provider
        resolver.registerViewDefinition(createTestViewDefinition(viewPath1));
        resolver.registerViewProvider(createTestViewProvider(viewPath2));

        // Only view2 should have a provider
        List<SystemViewProvider> providers = resolver.getAllViewProviders();
        assertThat(providers).hasSize(1);
        assertThat(providers.get(0).viewPath()).isEqualTo(viewPath2);
    }

    private static SystemViewDefinition createTestViewDefinition(TablePath viewPath) {
        return new SystemViewDefinition() {
            @Override
            public TablePath viewPath() {
                return viewPath;
            }

            @Override
            public Schema schema() {
                return Schema.newBuilder().column("col1", DataTypes.INT()).build();
            }

            @Override
            public TableDescriptor tableDescriptor() {
                return null;
            }
        };
    }

    private static SystemViewProvider createTestViewProvider(TablePath viewPath) {
        return new SystemViewProvider() {
            @Override
            public TablePath viewPath() {
                return viewPath;
            }

            @Override
            public Schema schema() {
                return Schema.newBuilder().column("col1", DataTypes.INT()).build();
            }

            @Override
            public byte[] scanRows(
                    @Nullable int[] projectedFields, @Nullable Predicate filterPredicate) {
                return new byte[0];
            }
        };
    }
}
