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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

/** Coordinator-local snapshot of the columns of a Fluss table. */
final class FlussColumnsSystemTable implements SystemTable {

    static final String SUFFIX = "$columns";

    private static final String COLUMN_NAME = "column_name";
    private static final String ORDINAL_POSITION = "ordinal_position";
    private static final String FLUSS_TYPE = "fluss_type";
    private static final String TRINO_TYPE = "trino_type";
    private static final String IS_NULLABLE = "is_nullable";
    private static final String PRIMARY_KEY_POSITION = "primary_key_position";
    private static final String PARTITION_KEY_POSITION = "partition_key_position";
    private static final String BUCKET_KEY_POSITION = "bucket_key_position";
    private static final String COMMENT = "comment";

    private final ConnectorTableMetadata metadata;
    private final InMemoryRecordSet records;

    FlussColumnsSystemTable(SchemaTableName name, TableInfo tableInfo) {
        metadata =
                new ConnectorTableMetadata(
                        name,
                        ImmutableList.of(
                                requiredColumn(COLUMN_NAME, VARCHAR),
                                requiredColumn(ORDINAL_POSITION, BIGINT),
                                requiredColumn(FLUSS_TYPE, VARCHAR),
                                requiredColumn(TRINO_TYPE, VARCHAR),
                                requiredColumn(IS_NULLABLE, BOOLEAN),
                                nullableColumn(PRIMARY_KEY_POSITION, BIGINT),
                                nullableColumn(PARTITION_KEY_POSITION, BIGINT),
                                nullableColumn(BUCKET_KEY_POSITION, BIGINT),
                                nullableColumn(COMMENT, VARCHAR)));

        Map<String, Long> primaryKeyPositions = indexPositions(tableInfo.getPrimaryKeys());
        Map<String, Long> partitionKeyPositions = indexPositions(tableInfo.getPartitionKeys());
        Map<String, Long> bucketKeyPositions = indexPositions(tableInfo.getBucketKeys());

        InMemoryRecordSet.Builder builder = InMemoryRecordSet.builder(metadata);
        List<Schema.Column> columns = tableInfo.getSchema().getColumns();

        for (int i = 0; i < columns.size(); i++) {
            Schema.Column column = columns.get(i);

            builder.addRow(
                    column.getName(),
                    (long) i + 1,
                    column.getDataType().toString(),
                    FlussTypeConverter.toTrinoType(column.getDataType()).getDisplayName(),
                    column.getDataType().isNullable(),
                    primaryKeyPositions.get(column.getName()),
                    partitionKeyPositions.get(column.getName()),
                    bucketKeyPositions.get(column.getName()),
                    column.getComment().orElse(null));
        }

        records = builder.build();
    }

    @Override
    public Distribution getDistribution() {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor cursor(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            TupleDomain<Integer> constraint) {
        if (constraint.isNone()) {
            return InMemoryRecordSet.builder(metadata).build().cursor();
        }

        // Constraint pushdown is intentionally not implemented for this small
        // coordinator-local metadata snapshot.
        return records.cursor();
    }

    private static ColumnMetadata requiredColumn(String name, Type type) {
        return ColumnMetadata.builder().setName(name).setType(type).setNullable(false).build();
    }

    private static ColumnMetadata nullableColumn(String name, Type type) {
        return ColumnMetadata.builder().setName(name).setType(type).setNullable(true).build();
    }

    private static Map<String, Long> indexPositions(List<String> columnNames) {
        Map<String, Long> positions = new LinkedHashMap<>();

        for (int i = 0; i < columnNames.size(); i++) {
            positions.put(columnNames.get(i), (long) i + 1);
        }

        return positions;
    }
}
