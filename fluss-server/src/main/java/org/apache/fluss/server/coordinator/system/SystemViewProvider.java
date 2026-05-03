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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Provider for a system view — a virtual table with no independent storage.
 *
 * <p>A provider extends {@link SystemViewDefinition} with the ability to actually produce data via
 * {@link #scanRows(int[], Predicate)}. Providers are registered only on servers that have access to
 * the required data sources (e.g., the coordinator server for coordinator-specific views).
 *
 * <p>For servers that only need to know a view exists (for metadata queries like {@code listTables}
 * or {@code getTableInfo}), the {@link SystemViewDefinition} is sufficient.
 *
 * @see SystemViewDefinition
 */
@Internal
public interface SystemViewProvider extends SystemViewDefinition {

    /**
     * Fetches rows for this system view, applies an optional projection and filter, and returns
     * them as a serialized byte array in {@link DefaultValueRecordBatch} format.
     *
     * @param projectedFields optional column indices to project, or {@code null} for all columns
     * @param filterPredicate optional server-side filter, or {@code null} for no filtering
     * @return serialized record batch bytes
     * @throws Exception if fetching or serializing data fails
     */
    byte[] scanRows(@Nullable int[] projectedFields, @Nullable Predicate filterPredicate)
            throws Exception;

    /**
     * Utility method to serialize a list of {@link InternalRow} into a byte array using {@link
     * DefaultValueRecordBatch} format.
     *
     * <p>Implementations can call this from {@link #scanRows(int[], Predicate)} after fetching and
     * filtering rows.
     *
     * @param rows the rows to serialize
     * @param schema the schema of the rows
     * @param schemaVersion the schema version to stamp on each record
     * @return serialized record batch bytes
     */
    static byte[] serializeRows(List<InternalRow> rows, Schema schema, int schemaVersion) {
        DataType[] fieldTypes = schema.getRowType().getFieldTypes().toArray(new DataType[0]);

        RowSerializer rowSerializer =
                new RowSerializer(fieldTypes, BinaryRow.BinaryRowFormat.INDEXED);

        try {
            DefaultValueRecordBatch.Builder batchBuilder = DefaultValueRecordBatch.builder();
            for (InternalRow row : rows) {
                BinaryRow binaryRow = rowSerializer.toBinaryRow(row);
                batchBuilder.append((short) schemaVersion, binaryRow);
            }
            DefaultValueRecordBatch batch = batchBuilder.build();

            byte[] result = new byte[batch.sizeInBytes()];
            batch.getSegment().get(batch.getPosition(), result, 0, result.length);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
