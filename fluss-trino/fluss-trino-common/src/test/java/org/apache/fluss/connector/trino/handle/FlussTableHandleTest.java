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

package org.apache.fluss.connector.trino.handle;

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FlussTableHandle}.
 */
class FlussTableHandleTest {

    @Test
    void testBasicConstruction() {
        TableInfo tableInfo = Mockito.mock(TableInfo.class);
        FlussTableHandle handle = new FlussTableHandle("db1", "table1", tableInfo);
        
        assertThat(handle.getSchemaName()).isEqualTo("db1");
        assertThat(handle.getTableName()).isEqualTo("table1");
        assertThat(handle.getTableInfo()).isEqualTo(tableInfo);
        assertThat(handle.getTablePath()).isEqualTo(new TablePath("db1", "table1"));
    }

    @Test
    void testWithConstraint() {
        TableInfo tableInfo = Mockito.mock(TableInfo.class);
        FlussTableHandle handle = new FlussTableHandle("db1", "table1", tableInfo);
        
        TupleDomain<ColumnHandle> constraint = TupleDomain.all();
        FlussTableHandle newHandle = handle.withConstraint(constraint);
        
        assertThat(newHandle.getConstraint()).isEqualTo(constraint);
        assertThat(newHandle.getSchemaName()).isEqualTo("db1");
    }

    @Test
    void testWithProjectedColumns() {
        TableInfo tableInfo = Mockito.mock(TableInfo.class);
        FlussTableHandle handle = new FlussTableHandle("db1", "table1", tableInfo);
        
        FlussColumnHandle col1 = new FlussColumnHandle("id", IntegerType.INTEGER, 0);
        Set<ColumnHandle> columns = Set.of(col1);
        
        FlussTableHandle newHandle = handle.withProjectedColumns(columns);
        
        assertThat(newHandle.getProjectedColumns()).isPresent();
        assertThat(newHandle.getProjectedColumns().get()).hasSize(1);
    }

    @Test
    void testWithLimit() {
        TableInfo tableInfo = Mockito.mock(TableInfo.class);
        FlussTableHandle handle = new FlussTableHandle("db1", "table1", tableInfo);
        
        FlussTableHandle newHandle = handle.withLimit(100);
        
        assertThat(newHandle.getLimit()).isEqualTo(Optional.of(100L));
    }

    @Test
    void testHasOptimizations() {
        TableInfo tableInfo = Mockito.mock(TableInfo.class);
        FlussTableHandle handle = new FlussTableHandle("db1", "table1", tableInfo);
        
        assertThat(handle.hasOptimizations()).isFalse();
        
        FlussTableHandle withLimit = handle.withLimit(100);
        assertThat(withLimit.hasOptimizations()).isTrue();
    }

    @Test
    void testUnionReadEnabled() {
        TableInfo tableInfo = Mockito.mock(TableInfo.class);
        FlussTableHandle handle = new FlussTableHandle("db1", "table1", tableInfo);
        
        assertThat(handle.isUnionReadEnabled()).isTrue();
        
        FlussTableHandle disabled = handle.withUnionRead(false);
        assertThat(disabled.isUnionReadEnabled()).isFalse();
    }
}
