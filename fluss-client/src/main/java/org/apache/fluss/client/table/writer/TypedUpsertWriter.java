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

import java.util.concurrent.CompletableFuture;

/**
 * A typed {@link UpsertWriter} that converts POJOs to {@link InternalRow} and delegates to the
 * existing internal-row based writer implementation.
 */
class TypedUpsertWriter<T> implements UpsertWriter<T> {

    @Override
    public void flush() {
        delegate.flush();
    }

    private final UpsertWriterImpl delegate;
    private final Class<T> pojoClass;
    private final TableInfo tableInfo;
    private final RowType tableSchema;
    private final int[] targetColumns; // may be null

    TypedUpsertWriter(
            UpsertWriterImpl delegate,
            Class<T> pojoClass,
            TableInfo tableInfo,
            int[] targetColumns) {
        this.delegate = delegate;
        this.pojoClass = pojoClass;
        this.tableInfo = tableInfo;
        this.tableSchema = tableInfo.getRowType();
        this.targetColumns = targetColumns;
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
        RowType projection;
        if (forDelete) {
            // for delete we only need primary key columns
            projection = tableSchema.project(tableInfo.getPhysicalPrimaryKeys());
        } else if (targetColumns != null) {
            projection = tableSchema.project(targetColumns);
        } else {
            projection = tableSchema;
        }

        // TODO: initialize this on the constructor and reuse
        PojoToRowConverter<T> converter = PojoToRowConverter.of(pojoClass, tableSchema, projection);
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
