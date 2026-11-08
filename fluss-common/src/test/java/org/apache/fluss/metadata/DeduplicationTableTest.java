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

package org.apache.fluss.metadata;

import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the schema and routing contracts of log tables with deduplication. */
class DeduplicationTableTest {

    @Test
    void testSchemaRoundTripPreservesDeduplicationWithoutPrimaryKey() {
        Schema schema = schemaBuilder().deduplicationKey("dt", "tenant", "id").build();
        Schema restored = Schema.fromJsonBytes(schema.toJsonBytes());

        assertThat(restored).isEqualTo(schema);
        assertThat(restored.getPrimaryKey()).isEmpty();
        assertThat(restored.getDeduplicationKeyColumnNames()).containsExactly("dt", "tenant", "id");
        assertThat(restored.getColumn("id").getDataType().isNullable()).isFalse();
        assertThat(restored.getColumn("payload").getDataType().isNullable()).isTrue();
        assertThat(restored.getRowType().equalsWithFieldId(schema.getRowType())).isTrue();
        assertThat(Schema.newBuilder().fromSchema(schema).build()).isEqualTo(schema);
    }

    @Test
    void testDeduplicationKeyIsImmutable() {
        List<String> columns = new ArrayList<>(Arrays.asList("tenant", "id"));
        Schema schema = schemaBuilder().deduplicationKey(columns).build();
        columns.clear();

        assertThat(schema.getDeduplicationKeyColumnNames()).containsExactly("tenant", "id");
        assertThatThrownBy(() -> schema.getDeduplicationKeyColumnNames().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testRejectsInvalidDeduplicationKeys() {
        assertThatThrownBy(() -> schemaBuilder().deduplicationKey(Collections.emptyList()).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> schemaBuilder().deduplicationKey("id", "id").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("duplicate");
        assertThatThrownBy(() -> schemaBuilder().deduplicationKey("missing").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing");
        assertThatThrownBy(() -> schemaBuilder().primaryKey("id").deduplicationKey("id").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mutually exclusive");
        assertThatThrownBy(() -> schemaBuilder().deduplicationKey("id").primaryKey("id").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mutually exclusive");
        assertThatThrownBy(() -> schemaBuilder().deduplicationKey("id").deduplicationKey("tenant"))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("id", DataTypes.BIGINT())
                                        .column("amount", DataTypes.BIGINT(), AggFunctions.SUM())
                                        .deduplicationKey("id")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Aggregation functions");
    }

    @Test
    void testDefaultRoutingUsesNonPartitionDeduplicationColumns() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schemaBuilder().deduplicationKey("dt", "tenant", "id").build())
                        .partitionedBy("dt")
                        .distributedBy(8)
                        .build();

        assertThat(descriptor.hasPrimaryKey()).isFalse();
        assertThat(descriptor.isDefaultBucketKey()).isTrue();
        assertThat(descriptor.getBucketKeys()).containsExactly("tenant", "id");
        assertThat(TableDescriptor.fromJsonBytes(descriptor.toJsonBytes())).isEqualTo(descriptor);
        TableInfo tableInfo =
                TableInfo.of(TablePath.of("db", "events"), 1L, 1, descriptor, null, 0L, 0L);
        assertThat(tableInfo.hasPrimaryKey()).isFalse();
        assertThat(tableInfo.hasDeduplicationKey()).isTrue();
        assertThat(tableInfo.isDefaultBucketKey()).isTrue();
        assertThat(tableInfo.getDeduplicationKeys()).containsExactly("dt", "tenant", "id");
    }

    @Test
    void testAllowsBucketKeySubsetButRejectsUnrelatedRouting() {
        Schema schema = schemaBuilder().deduplicationKey("dt", "tenant", "id").build();
        assertThat(
                        TableDescriptor.builder()
                                .schema(schema)
                                .partitionedBy("dt")
                                .distributedBy(8, "tenant")
                                .build()
                                .getBucketKeys())
                .containsExactly("tenant");
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(schema)
                                        .partitionedBy("dt")
                                        .distributedBy(8, "payload")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deduplication");
    }

    @Test
    void testPartitionColumnsMustBelongToDeduplicationKey() {
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(schemaBuilder().deduplicationKey("id").build())
                                        .partitionedBy("dt")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Partition");
        assertThatThrownBy(
                        () ->
                                TableDescriptor.builder()
                                        .schema(schemaBuilder().deduplicationKey("dt").build())
                                        .partitionedBy("dt")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-partition");
    }

    private static Schema.Builder schemaBuilder() {
        return Schema.newBuilder()
                .column("dt", DataTypes.STRING())
                .column("tenant", DataTypes.STRING())
                .column("id", DataTypes.BIGINT())
                .column("payload", DataTypes.STRING());
    }
}
