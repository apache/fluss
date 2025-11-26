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

package org.apache.fluss.record;

import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;

import java.util.HashMap;
import java.util.Map;

/** A testing schema metadata subscriber. */
public class TestingSchemaGetter implements SchemaGetter {
    private SchemaInfo schemaInfo;
    private Map<Integer, Schema> schemaCaches;

    public TestingSchemaGetter() {
        this.schemaInfo = null;
        this.schemaCaches = new HashMap<>();
    }

    // todo: remove it(简化测试代码）
    public TestingSchemaGetter(SchemaInfo schemaInfo) {
        this.schemaInfo = schemaInfo;
        this.schemaCaches = new HashMap<>();
        this.schemaCaches.put(schemaInfo.getSchemaId(), schemaInfo.getSchema());
    }

    public TestingSchemaGetter(int schemaId, Schema schema) {
        this.schemaInfo = new SchemaInfo(schema, schemaId);
        this.schemaCaches = new HashMap<>();
        this.schemaCaches.put(schemaId, schema);
    }

    @Override
    public Schema getSchema(int schemaId) {
        if (!schemaCaches.containsKey(schemaId)) {
            throw new SchemaNotExistException("Schema not exist");
        }
        return schemaCaches.get(schemaId);
    }

    @Override
    public SchemaInfo getLatestSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public void release() {}

    public void updateLatestSchemaInfo(SchemaInfo schemaInfo) {
        if (schemaInfo.getSchemaId() >= this.schemaInfo.getSchemaId()) {
            this.schemaInfo = schemaInfo;
        }
        this.schemaCaches.put(schemaInfo.getSchemaId(), schemaInfo.getSchema());
    }
}
