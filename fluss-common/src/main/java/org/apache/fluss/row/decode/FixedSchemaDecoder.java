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

package org.apache.fluss.row.decode;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.SchemaUtil;

import static org.apache.fluss.row.encode.ValueEncoder.SCHEMA_ID_LENGTH;

/**
 * A decoder that deserializes raw byte arrays of {@link ValueRecord} with dynamic or
 * self-describing schemas into structured {@link InternalRow} objects conforming to a predefined,
 * fixed output schema.
 */
public class FixedSchemaDecoder {

    /** The underlying row decoder used to decode the source schema rows. */
    private final RowDecoder rowDecoder;

    /**
     * Schema projection mapping from source schema id to target schema field index mapping. The
     * projection mapping is an array where the index is the target schema field index and the value
     * is the source schema field index.
     */
    private final int[] fieldIdMapping;

    /** Indicates whether there is no projection between source schema and target schema. */
    private final boolean noProjection;

    public FixedSchemaDecoder(KvFormat kvFormat, Schema sourceSchema, Schema targetSchema) {
        this.rowDecoder =
                RowDecoder.create(
                        kvFormat, sourceSchema.getRowType().getChildren().toArray(new DataType[0]));
        this.fieldIdMapping = SchemaUtil.getIndexMapping(sourceSchema, targetSchema);
        this.noProjection = false;
    }

    /**
     * Creates a FixedSchemaDecoder without projection, i.e., the source schema is the same as the
     * target schema.
     */
    public FixedSchemaDecoder(KvFormat kvFormat, Schema schema) {
        this.rowDecoder =
                RowDecoder.create(
                        kvFormat, schema.getRowType().getChildren().toArray(new DataType[0]));
        this.fieldIdMapping = null;
        this.noProjection = true;
    }

    /**
     * Decode the bytes in the memory segment to {@link InternalRow} that adheres to the fixed
     * {@code targetSchema}.
     *
     * @param segment the memory segment to read.
     * @param offset the offset in the memory segment to read from.
     * @param sizeInBytes the total size in bytes to read.
     */
    public InternalRow decode(MemorySegment segment, int offset, int sizeInBytes) {
        if (noProjection) {
            return rowDecoder.decode(segment, offset, sizeInBytes);
        } else {
            InternalRow sourceRow = rowDecoder.decode(segment, offset, sizeInBytes);
            return ProjectedRow.from(fieldIdMapping).replaceRow(sourceRow);
        }
    }

    /**
     * Decode the value memory segment (in {@link ValueRecord} format) to {@link InternalRow} that
     * adheres to the fixed {@code targetSchema}.
     */
    public InternalRow decode(MemorySegment valueSegment) {
        return decode(valueSegment, SCHEMA_ID_LENGTH, valueSegment.size() - SCHEMA_ID_LENGTH);
    }
}
