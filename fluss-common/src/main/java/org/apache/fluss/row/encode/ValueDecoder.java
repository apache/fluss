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

package org.apache.fluss.row.encode;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.types.DataType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;

/** Decodes a schema id and {@link BinaryRow} from bytes described by a {@link KvValueLayout}. */
public class ValueDecoder {

    private final Map<Short, RowDecoder> rowDecoders;
    private final SchemaGetter schemaGetter;
    private final KvFormat kvFormat;
    private final KvValueLayout kvValueLayout;

    public ValueDecoder(SchemaGetter schemaGetter, KvFormat kvFormat) {
        this(schemaGetter, kvFormat, KV_FORMAT_VERSION_2);
    }

    public ValueDecoder(SchemaGetter schemaGetter, KvFormat kvFormat, int kvFormatVersion) {
        this(schemaGetter, kvFormat, KvValueLayout.forKvFormatVersion(kvFormatVersion));
    }

    public ValueDecoder(SchemaGetter schemaGetter, KvFormat kvFormat, KvValueLayout kvValueLayout) {
        this.rowDecoders = new ConcurrentHashMap<>();
        this.schemaGetter = schemaGetter;
        this.kvFormat = kvFormat;
        this.kvValueLayout = kvValueLayout;
    }

    /** Decode the value bytes and return the schema id and the row encoded in the value bytes. */
    public BinaryValue decodeValue(byte[] valueBytes) {
        MemorySegment memorySegment = MemorySegment.wrap(valueBytes);
        short schemaId = kvValueLayout.readSchemaId(memorySegment);

        RowDecoder rowDecoder =
                rowDecoders.computeIfAbsent(
                        schemaId,
                        (id) -> {
                            Schema schema = schemaGetter.getSchema(schemaId);
                            return RowDecoder.create(
                                    kvFormat,
                                    schema.getRowType().getChildren().toArray(new DataType[0]));
                        });

        BinaryRow row =
                rowDecoder.decode(
                        memorySegment,
                        kvValueLayout.rowPayloadOffset(),
                        kvValueLayout.rowPayloadLength(valueBytes.length));
        return new BinaryValue(schemaId, row);
    }
}
