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

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.types.DataType;

import java.util.HashMap;
import java.util.Map;

/** A default implementation of {@link ValueRecordBatch.ReadContext} . */
public class ValueRecordReadContext implements ValueRecordBatch.ReadContext {
    private final Map<Integer, RowDecoder> rowDecoderCache;
    private final SchemaGetter schemaGetter;
    private final KvFormat kvFormat;

    private ValueRecordReadContext(SchemaGetter schemaGetter, KvFormat kvFormat) {
        this.rowDecoderCache = new HashMap<>();
        this.schemaGetter = schemaGetter;
        this.kvFormat = kvFormat;
    }

    public static ValueRecordReadContext createReadContext(
            SchemaGetter schemaGetter, KvFormat kvFormat) {
        return new ValueRecordReadContext(schemaGetter, kvFormat);
    }

    @Override
    public RowDecoder getRowDecoder(int schemaId) {
        return rowDecoderCache.computeIfAbsent(
                schemaId,
                (id) -> {
                    Schema schema = schemaGetter.getSchema(schemaId);
                    return RowDecoder.create(
                            kvFormat, schema.getRowType().getChildren().toArray(new DataType[0]));
                });
    }
}
