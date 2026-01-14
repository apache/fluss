package org.apache.fluss.client.lookup;

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

import org.apache.fluss.client.converter.PojoToRowConverter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Decorator for {@link Lookuper} that enables generic key lookup via {@link
 * TypedLookuper#lookup(Object)}. Converts POJO keys to {@link InternalRow} using existing
 * converters based on table schema and active lookup columns, and directly delegates when the key
 * is already an {@link InternalRow}.
 */
final class TypedLookuperImpl<K> implements TypedLookuper<K> {

    private final Lookuper delegate;
    private final TableInfo tableInfo;
    @Nullable private final List<String> lookupColumnNames;
    private final PojoToRowConverter<K> keyConv;

    TypedLookuperImpl(
            Lookuper delegate,
            TableInfo tableInfo,
            @Nullable List<String> lookupColumnNames,
            Class<K> keyClass) {
        this.delegate = delegate;
        this.tableInfo = tableInfo;
        this.lookupColumnNames = lookupColumnNames;
        this.keyConv = createPojoToRowConverter(keyClass);
    }

    @Override
    public CompletableFuture<LookupResult> lookup(K key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        // Fast-path: already an InternalRow
        if (key instanceof InternalRow) {
            return delegate.lookup((InternalRow) key);
        }

        InternalRow keyRow = keyConv.toRow(key);
        return delegate.lookup(keyRow);
    }

    private PojoToRowConverter<K> createPojoToRowConverter(Class<K> keyClass) {
        RowType tableSchema = tableInfo.getRowType();
        RowType keyProjection;
        if (lookupColumnNames == null) {
            keyProjection = tableSchema.project(tableInfo.getPrimaryKeys());
        } else {
            keyProjection = tableSchema.project(lookupColumnNames);
        }
        return PojoToRowConverter.of(keyClass, tableSchema, keyProjection);
    }
}
