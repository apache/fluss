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

package org.apache.fluss.server.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * How a fetch projection over a column-group table maps onto the physical logs (FIP-45): which
 * base-log columns to project (in base physical positions) and which column groups it touches.
 */
@Internal
public final class ColumnGroupFetchPlan {

    private static final ColumnGroupFetchPlan NONE =
            new ColumnGroupFetchPlan(null, Collections.emptyList(), null);

    /** Base physical column positions to project, or null for the whole base row. */
    @Nullable private final int[] baseFields;

    /** Column groups touched by the projection, in schema order of first appearance. */
    private final List<String> groups;

    /** Schema getter answering the base physical schema, or null when the table has no groups. */
    @Nullable private final SchemaGetter baseSchemaGetter;

    private ColumnGroupFetchPlan(
            @Nullable int[] baseFields,
            List<String> groups,
            @Nullable SchemaGetter baseSchemaGetter) {
        this.baseFields = baseFields;
        this.groups = groups;
        this.baseSchemaGetter = baseSchemaGetter;
    }

    /**
     * Plans a fetch against {@code schema}.
     *
     * @param schema the latest table schema
     * @param projectedFields the projected table column positions, or null for all columns
     * @param baseSchemaGetter schema getter answering the base physical schema
     */
    public static ColumnGroupFetchPlan plan(
            Schema schema, @Nullable int[] projectedFields, SchemaGetter baseSchemaGetter) {
        if (!schema.hasColumnGroups()) {
            return NONE;
        }
        int[] baseIndices = schema.getDefaultGroupColumnIndices();
        if (projectedFields == null) {
            // SELECT *: every group is touched, whole base row is shipped.
            List<String> allGroups = new ArrayList<>();
            for (int i = 0; i < schema.getColumns().size(); i++) {
                String group = schema.getColumnGroupOf(i);
                if (group != null && !allGroups.contains(group)) {
                    allGroups.add(group);
                }
            }
            return new ColumnGroupFetchPlan(null, allGroups, baseSchemaGetter);
        }
        List<Integer> baseFields = new ArrayList<>();
        Set<String> groups = new LinkedHashSet<>();
        for (int field : projectedFields) {
            String group = schema.getColumnGroupOf(field);
            if (group == null) {
                baseFields.add(basePosition(baseIndices, field));
            } else {
                groups.add(group);
            }
        }
        if (baseFields.isEmpty()) {
            // The server cannot project zero columns; carry the first base column so the fetch
            // still advances offsets. The client drops it when building the output row.
            baseFields.add(0);
        }
        int[] base = baseFields.stream().mapToInt(Integer::intValue).toArray();
        return new ColumnGroupFetchPlan(base, new ArrayList<>(groups), baseSchemaGetter);
    }

    private static int basePosition(int[] baseIndices, int tableColumn) {
        for (int i = 0; i < baseIndices.length; i++) {
            if (baseIndices[i] == tableColumn) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column " + tableColumn + " is not a base column.");
    }

    public boolean hasColumnGroups() {
        return baseSchemaGetter != null;
    }

    /** The projection to apply to the base log, in base physical positions. */
    @Nullable
    public int[] baseProjection(@Nullable int[] projectedFields) {
        return hasColumnGroups() ? baseFields : projectedFields;
    }

    /** The schema getter to resolve the base log's physical schema with. */
    public SchemaGetter schemaGetter(SchemaGetter tableSchemaGetter) {
        return baseSchemaGetter != null ? baseSchemaGetter : tableSchemaGetter;
    }

    public List<String> touchedGroups() {
        return groups;
    }
}
