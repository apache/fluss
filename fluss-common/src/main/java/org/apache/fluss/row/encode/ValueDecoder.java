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
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.types.DataType;

import java.util.Map;

import static org.apache.fluss.row.encode.ValueEncoder.SCHEMA_ID_LENGTH;
import static org.apache.fluss.utils.MapUtils.newConcurrentHashMap;

/**
 * A decoder to decode a schema id and {@link BinaryRow} from a byte array value which is encoded by
 * {@link ValueEncoder#encodeValue(short, BinaryRow)}.
 */
public class ValueDecoder {

    private final Map<Short, RowDecoder> rowDecoders;
    private final SchemaGetter schemaGetter;
    private final KvFormat kvFormat;

    public ValueDecoder(SchemaGetter schemaGetter, KvFormat kvFormat) {
        this.rowDecoders = newConcurrentHashMap();
        this.schemaGetter = schemaGetter;
        this.kvFormat = kvFormat;
    }

    /** Decode the value bytes and return the schema id and the row encoded in the value bytes. */
    public Value decodeValue(byte[] valueBytes) {
        MemorySegment memorySegment = MemorySegment.wrap(valueBytes);
        short schemaId = memorySegment.getShort(0);

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
                        memorySegment, SCHEMA_ID_LENGTH, valueBytes.length - SCHEMA_ID_LENGTH);
        return new Value(schemaId, row);
    }

    /** The schema id and {@link BinaryRow} stored as the value of kv store. */
    public static class Value {
        public final short schemaId;
        public final BinaryRow row;

        private Value(short schemaId, BinaryRow row) {
            this.schemaId = schemaId;
            this.row = row;
        }
    }
}
