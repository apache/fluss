/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.annotation.Internal;

/**
 * Information of a schema, includes {@link Schema} and the schema id which represents the unique
 * identifier of the schema of the table. This makes sure that every schema change will always have
 * unique schema identifiers.
 *
 * <p>The schema id of each table begins from 1 and is incremented by 1 for each schema change.
 */
@Internal
public final class SchemaInfo {
    private final Schema schema;
    private final int schemaId;

    public SchemaInfo(Schema schema, int schemaId) {
        this.schema = schema;
        this.schemaId = schemaId;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getSchemaId() {
        return schemaId;
    }

    @Override
    public String toString() {
        return "SchemaInfo{id=" + schemaId + ", schema=" + schema + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SchemaInfo that = (SchemaInfo) o;

        if (schemaId != that.schemaId) {
            return false;
        }
        return schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + schemaId;
        return result;
    }
}
