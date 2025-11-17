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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import java.util.concurrent.CompletableFuture;

/**
 * A typed {@link AppendWriter} that converts POJOs to {@link InternalRow} and delegates to the
 * existing internal-row based writer implementation.
 */
class TypedAppendWriter<T> implements AppendWriter<T> {

    private final AppendWriterImpl delegate;
    private final Class<T> pojoClass;
    private final RowType tableSchema;

    TypedAppendWriter(AppendWriterImpl delegate, Class<T> pojoClass, TableInfo tableInfo) {
        this.delegate = delegate;
        this.pojoClass = pojoClass;
        this.tableSchema = tableInfo.getRowType();
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public CompletableFuture<AppendResult> append(T record) {
        if (record instanceof InternalRow) {
            return delegate.append((InternalRow) record);
        }
        // TODO: initialize this on the constructor and reuse
        PojoToRowConverter<T> converter =
                PojoToRowConverter.of(pojoClass, tableSchema, tableSchema);
        InternalRow row = converter.toRow(record);
        return delegate.append(row);
    }
}
