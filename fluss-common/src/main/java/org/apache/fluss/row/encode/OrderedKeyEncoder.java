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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ordered.OrderedKeyCodec;
import org.apache.fluss.types.RowType;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;

/** Encodes non-null keys in the ascending bytewise format defined by {@link OrderedKeyCodec}. */
@Internal
@NotThreadSafe
public final class OrderedKeyEncoder implements KeyEncoder {

    private final OrderedKeyCodec codec;

    private OrderedKeyEncoder(RowType rowType, List<String> keyFields) {
        codec = new OrderedKeyCodec(rowType, keyFields);
    }

    /** Creates an encoder; selecting leading key fields also supports field-prefix lookups. */
    public static OrderedKeyEncoder createKeyEncoder(RowType rowType, List<String> keyFields) {
        return new OrderedKeyEncoder(rowType, keyFields);
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        return codec.encodeKey(row);
    }
}
