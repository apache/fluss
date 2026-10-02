/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.compression.ColumnValueCodec;

import java.util.BitSet;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Base row encoder that applies column-value encoding before writing the physical row. */
@Internal
public abstract class AbstractRowEncoder implements RowEncoder {

    private final BitSet encodedColumns;
    private final ColumnValueCodec columnValueCodec;

    protected AbstractRowEncoder(BitSet encodedColumns, Map<Integer, String> tableConfigs) {
        this.encodedColumns =
                (BitSet) checkNotNull(encodedColumns, "encodedColumns must not be null").clone();
        this.columnValueCodec = new ColumnValueCodec(tableConfigs);
    }

    @Override
    public final boolean hasColumnValueEncoding() {
        return !encodedColumns.isEmpty();
    }

    @Override
    public final void encodeField(int pos, Object value) {
        Object encodedValue = value;
        if (value != null && encodedColumns.get(pos)) {
            encodedValue = columnValueCodec.encode(pos, (byte[]) value);
        }
        encode(pos, encodedValue);
    }

    /** Writes a field to the concrete physical row format. */
    protected abstract void encode(int pos, Object value);
}
