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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.client.converter.PojoToRowConverter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * A typed {@link UpsertWriter} that converts POJOs to {@link InternalRow} and delegates to the
 * existing internal-row based writer implementation.
 */
class TypedUpsertWriterImpl<T> implements TypedUpsertWriter<T> {

    private final UpsertWriter delegate;
    private final TableInfo tableInfo;
    private final RowType tableSchema;
    @Nullable private final int[] targetColumns;

    private final RowType pkProjection;
    @Nullable private final RowType targetProjection;

    private final PojoToRowConverter<T> pojoToRowConverter;
    private final PojoToRowConverter<T> pkConverter;
    @Nullable private final PojoToRowConverter<T> targetConverter;

    TypedUpsertWriterImpl(
            UpsertWriter delegate, Class<T> pojoClass, TableInfo tableInfo, int[] targetColumns) {
        this.delegate = delegate;
        this.tableInfo = tableInfo;
        this.tableSchema = tableInfo.getRowType();
        this.targetColumns = targetColumns;

        // Precompute projections
        this.pkProjection = this.tableSchema.project(tableInfo.getPhysicalPrimaryKeys());
        this.targetProjection =
                (targetColumns == null) ? null : this.tableSchema.project(targetColumns);

        // Initialize reusable converters
        this.pojoToRowConverter = PojoToRowConverter.of(pojoClass, tableSchema, tableSchema);
        this.pkConverter = PojoToRowConverter.of(pojoClass, tableSchema, pkProjection);
        this.targetConverter =
                (targetProjection == null)
                        ? null
                        : PojoToRowConverter.of(pojoClass, tableSchema, targetProjection);
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public CompletableFuture<UpsertResult> upsert(T record) {
        if (record instanceof InternalRow) {
            return delegate.upsert((InternalRow) record);
        }
        InternalRow row = convertPojo(record, /*forDelete=*/ false);
        return delegate.upsert(row);
    }

    @Override
    public CompletableFuture<DeleteResult> delete(T record) {
        if (record instanceof InternalRow) {
            return delegate.delete((InternalRow) record);
        }
        InternalRow pkOnly = convertPojo(record, /*forDelete=*/ true);
        return delegate.delete(pkOnly);
    }

    private InternalRow convertPojo(T pojo, boolean forDelete) {
        final RowType projection;
        final PojoToRowConverter<T> converter;
        if (forDelete) {
            projection = pkProjection;
            converter = pkConverter;
        } else if (targetProjection != null && targetConverter != null) {
            projection = targetProjection;
            converter = targetConverter;
        } else {
            projection = tableSchema;
            converter = pojoToRowConverter;
        }

        GenericRow projected = converter.toRow(pojo);
        if (projection == tableSchema) {
            return projected;
        }
        // expand projected row to full row if needed
        GenericRow full = new GenericRow(tableSchema.getFieldCount());
        if (forDelete) {
            // set PK fields, others null
            for (String pk : tableInfo.getPhysicalPrimaryKeys()) {
                int projIndex = projection.getFieldIndex(pk);

                // TODO: this can be optimized by pre-computing
                // the index mapping in the constructor?
                int fullIndex = tableSchema.getFieldIndex(pk);
                full.setField(fullIndex, projected.getField(projIndex));
            }
        } else if (targetColumns != null) {
            for (int i = 0; i < projection.getFieldCount(); i++) {
                String name = projection.getFieldNames().get(i);
                int fullIdx = tableSchema.getFieldIndex(name);
                full.setField(fullIdx, projected.getField(i));
            }
        }
        return full;
    }
}
