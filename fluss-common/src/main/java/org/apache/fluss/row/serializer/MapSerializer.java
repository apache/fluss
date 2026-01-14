/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.serializer;

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryMap;
import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.map.AlignedMap;
import org.apache.fluss.row.map.CompactedMap;
import org.apache.fluss.row.map.IndexedMap;
import org.apache.fluss.row.map.PrimitiveBinaryMap;
import org.apache.fluss.types.DataType;

import java.io.Serializable;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.ALIGNED;
import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.COMPACTED;
import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;

/** Serializer for {@link InternalMap} to {@link BinaryMap}. */
public class MapSerializer implements Serializable {
    private static final long serialVersionUID = 1L;

    private final DataType keyType;
    private final DataType valueType;
    private final BinaryRowFormat rowFormat;

    private transient ArraySerializer keyArraySerializer;
    private transient ArraySerializer valueArraySerializer;
    private transient BinaryMap reuseMap;

    public MapSerializer(DataType keyType, DataType valueType, BinaryRowFormat rowFormat) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.rowFormat = rowFormat;
    }

    public BinaryMap toBinaryMap(InternalMap from) {
        if (from instanceof BinaryMap) {
            if (from instanceof PrimitiveBinaryMap
                    || rowFormat == INDEXED && from instanceof IndexedMap
                    || rowFormat == COMPACTED && from instanceof CompactedMap
                    || rowFormat == ALIGNED && from instanceof AlignedMap) {
                // directly return the original array iff the array is in the expected format
                return (BinaryMap) from;
            }
        }

        if (keyArraySerializer == null) {
            keyArraySerializer = new ArraySerializer(keyType, rowFormat);
        }
        if (valueArraySerializer == null) {
            valueArraySerializer = new ArraySerializer(valueType, rowFormat);
        }
        if (reuseMap == null) {
            reuseMap = createBinaryMapInstance();
        }

        InternalArray keyArray = from.keyArray();
        InternalArray valueArray = from.valueArray();

        BinaryArray binaryKeyArray = keyArraySerializer.toBinaryArray(keyArray);
        BinaryArray binaryValueArray = valueArraySerializer.toBinaryArray(valueArray);

        return BinaryMap.valueOf(binaryKeyArray, binaryValueArray, reuseMap);
    }

    private BinaryMap createBinaryMapInstance() {
        switch (rowFormat) {
            case COMPACTED:
                return new CompactedMap(keyType, valueType);
            case INDEXED:
                return new IndexedMap(keyType, valueType);
            case ALIGNED:
                return new AlignedMap();
            default:
                throw new IllegalArgumentException("Unsupported binary row format: " + rowFormat);
        }
    }
}
