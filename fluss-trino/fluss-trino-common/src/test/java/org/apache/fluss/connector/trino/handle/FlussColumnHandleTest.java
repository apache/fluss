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

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FlussColumnHandle}.
 */
class FlussColumnHandleTest {

    @Test
    void testBasicConstruction() {
        FlussColumnHandle handle = new FlussColumnHandle("id", IntegerType.INTEGER, 0);
        
        assertThat(handle.getName()).isEqualTo("id");
        assertThat(handle.getType()).isEqualTo(IntegerType.INTEGER);
        assertThat(handle.getOrdinalPosition()).isEqualTo(0);
        assertThat(handle.isPartitionKey()).isFalse();
        assertThat(handle.isPrimaryKey()).isFalse();
        assertThat(handle.isNullable()).isTrue();
    }

    @Test
    void testFullConstruction() {
        FlussColumnHandle handle = new FlussColumnHandle(
                "user_id",
                IntegerType.INTEGER,
                1,
                false,
                true,
                false,
                Optional.of("User ID"),
                Optional.empty());
        
        assertThat(handle.getName()).isEqualTo("user_id");
        assertThat(handle.isPrimaryKey()).isTrue();
        assertThat(handle.isNullable()).isFalse();
        assertThat(handle.getComment()).isEqualTo(Optional.of("User ID"));
    }

    @Test
    void testWithPrimaryKey() {
        FlussColumnHandle handle = new FlussColumnHandle("id", IntegerType.INTEGER, 0);
        FlussColumnHandle withPK = handle.withPrimaryKey(true);
        
        assertThat(withPK.isPrimaryKey()).isTrue();
        assertThat(withPK.getName()).isEqualTo("id");
    }

    @Test
    void testWithPartitionKey() {
        FlussColumnHandle handle = new FlussColumnHandle("date", VarcharType.VARCHAR, 0);
        FlussColumnHandle withPartition = handle.withPartitionKey(true);
        
        assertThat(withPartition.isPartitionKey()).isTrue();
        assertThat(withPartition.canPrunePartition()).isTrue();
    }

    @Test
    void testWithComment() {
        FlussColumnHandle handle = new FlussColumnHandle("name", VarcharType.VARCHAR, 0);
        FlussColumnHandle withComment = handle.withComment("User name");
        
        assertThat(withComment.getComment()).isEqualTo(Optional.of("User name"));
    }

    @Test
    void testToColumnMetadata() {
        FlussColumnHandle handle = new FlussColumnHandle(
                "age",
                IntegerType.INTEGER,
                2,
                false,
                false,
                true,
                Optional.of("User age"),
                Optional.empty());
        
        ColumnMetadata metadata = handle.toColumnMetadata();
        
        assertThat(metadata.getName()).isEqualTo("age");
        assertThat(metadata.getType()).isEqualTo(IntegerType.INTEGER);
        assertThat(metadata.isNullable()).isTrue();
        assertThat(metadata.getComment()).isEqualTo("User age");
    }

    @Test
    void testCanPointQuery() {
        FlussColumnHandle handle = new FlussColumnHandle("id", IntegerType.INTEGER, 0);
        assertThat(handle.canPointQuery()).isFalse();
        
        FlussColumnHandle withPK = handle.withPrimaryKey(true);
        assertThat(withPK.canPointQuery()).isTrue();
    }
}
