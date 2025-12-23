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

package org.apache.fluss.flink.adapter;

import org.apache.flink.table.api.Schema;

import java.util.List;

/**
 * An adapter for the schema with Index.
 *
 * <p>TODO: remove this class when no longer support all the Flink 1.x series.
 */
public class SchemaAdapter {
    private SchemaAdapter() {}

    public static Schema withIndex(Schema unresolvedSchema, List<List<String>> indexes) {
        Schema.Builder newSchemaBuilder = Schema.newBuilder().fromSchema(unresolvedSchema);
        if (!indexes.isEmpty()) {
            throw new UnsupportedOperationException("Index is not supported.");
        }
        return newSchemaBuilder.build();
    }

    public static boolean supportIndex() {
        return false;
    }
}
