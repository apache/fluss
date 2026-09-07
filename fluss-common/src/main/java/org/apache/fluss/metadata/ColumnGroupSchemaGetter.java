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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.Internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A {@link SchemaGetter} view of a column-group table (FIP-45) that answers the <em>physical</em>
 * schema of one of its logs: either the base log (the default-group columns only) or the shadow log
 * of one column group (that group's columns only). Anything that decodes or projects physical
 * batches of such a table must resolve schemas through one of these views.
 */
@Internal
public final class ColumnGroupSchemaGetter implements SchemaGetter {

    private final SchemaGetter delegate;
    private final Function<Schema, Schema> mapper;
    private final Map<Integer, Schema> cache = new ConcurrentHashMap<>();

    private ColumnGroupSchemaGetter(SchemaGetter delegate, Function<Schema, Schema> mapper) {
        this.delegate = delegate;
        this.mapper = mapper;
    }

    /** A view answering the base physical schema (default-group columns). */
    public static ColumnGroupSchemaGetter base(SchemaGetter delegate) {
        return new ColumnGroupSchemaGetter(delegate, ColumnGroupSchemaGetter::toBaseSchema);
    }

    /** A view answering the physical schema of column group {@code groupName}. */
    public static ColumnGroupSchemaGetter group(SchemaGetter delegate, String groupName) {
        return new ColumnGroupSchemaGetter(delegate, schema -> toGroupSchema(schema, groupName));
    }

    /** The base physical schema of {@code schema}: only its default-group columns. */
    public static Schema toBaseSchema(Schema schema) {
        if (!schema.hasColumnGroups()) {
            return schema;
        }
        return subset(schema, schema.getDefaultGroupColumnIndices());
    }

    /** The physical schema of column group {@code groupName}: only that group's columns. */
    public static Schema toGroupSchema(Schema schema, String groupName) {
        return subset(schema, schema.getColumnGroupColumnIndices(groupName));
    }

    private static Schema subset(Schema schema, int[] indices) {
        List<Schema.Column> columns = new ArrayList<>(indices.length);
        for (int index : indices) {
            // drop the group tag: the physical schema of a log has no groups of its own
            columns.add(schema.getColumns().get(index).withColumnGroup(null));
        }
        return Schema.newBuilder().fromColumns(columns).build();
    }

    @Override
    public Schema getSchema(int schemaId) {
        return cache.computeIfAbsent(schemaId, id -> mapper.apply(delegate.getSchema(id)));
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchemaInfoAsync(int schemaId) {
        return delegate.getSchemaInfoAsync(schemaId)
                .thenApply(
                        info -> new SchemaInfo(mapper.apply(info.getSchema()), info.getSchemaId()));
    }

    @Override
    public SchemaInfo getLatestSchemaInfo() {
        SchemaInfo latest = delegate.getLatestSchemaInfo();
        return new SchemaInfo(mapper.apply(latest.getSchema()), latest.getSchemaId());
    }

    @Override
    public void release() {
        // the delegate is owned by its creator
        cache.clear();
    }
}
