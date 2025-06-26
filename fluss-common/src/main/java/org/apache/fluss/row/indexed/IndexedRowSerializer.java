/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.indexed;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.serializer.InternalRowSerializer;
import org.apache.fluss.row.serializer.InternalSerializers;
import org.apache.fluss.row.serializer.Serializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;

import static org.apache.fluss.types.DataTypeChecks.getFieldTypes;

/** IndexedRowSerializer. */
public class IndexedRowSerializer extends InternalRowSerializer {
    /** Creates a {@link Serializer} for internal data structures of the given {@link DataType}. */
    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> create(DataType type) {
        return (Serializer<T>) createInternal(type);
    }

    private static Serializer<?> createInternal(DataType type) {
        if (type.getTypeRoot() == DataTypeRoot.ROW) {
            return new InternalRowSerializer(
                    getFieldTypes(type).toArray(new DataType[0]), KvFormat.INDEXED);
        }
        return InternalSerializers.create(type);
    }

    private IndexedRowSerializer() {
        super(new DataType[0], KvFormat.INDEXED);
    }
}
