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

package org.apache.fluss.client.lookup;

import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.OrderedKeyEncoder;
import org.apache.fluss.row.ordered.OrderedKeyCodec;
import org.apache.fluss.shaded.guava32.com.google.common.primitives.UnsignedBytes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.Range;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Encoded inclusive start and exclusive end for an ordered index prefix. */
final class IndexKeyRange {
    @Nullable private final byte[] startKey;
    @Nullable private final byte[] endKey;

    private IndexKeyRange(@Nullable byte[] startKey, @Nullable byte[] endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }

    /** Captures and encodes one-column bounds within the supplied equality prefix. */
    static IndexKeyRange encode(
            byte[] prefix, RowType endpointType, Range<? extends InternalRow> bounds) {
        checkNotNull(bounds, "bounds");
        OrderedKeyEncoder encoder =
                OrderedKeyEncoder.createKeyEncoder(endpointType, endpointType.getFieldNames());
        byte[] lower = encodeEndpoint(bounds.getLowerBound(), encoder);
        byte[] upper = encodeEndpoint(bounds.getUpperBound(), encoder);
        if (lower != null && upper != null) {
            checkArgument(
                    UnsignedBytes.lexicographicalComparator().compare(lower, upper) <= 0,
                    "Range lower bound exceeds upper bound.");
        }
        byte[] start = lower == null ? prefix : concatenate(prefix, lower);
        byte[] end =
                upper == null
                        ? OrderedKeyCodec.prefixUpperBound(prefix)
                        : concatenate(prefix, upper);
        if (lower != null && !bounds.isLowerInclusive()) {
            start = OrderedKeyCodec.prefixUpperBound(start);
        }
        if (upper != null && bounds.isUpperInclusive()) {
            end = OrderedKeyCodec.prefixUpperBound(end);
        }
        return new IndexKeyRange(start, end);
    }

    /** Returns the inclusive start key, or null for an exhausted lower bound. */
    @Nullable
    byte[] getStartKey() {
        return startKey;
    }

    /** Returns the exclusive end key, or null if unbounded. */
    @Nullable
    byte[] getEndKey() {
        return endKey;
    }

    /** Whether the interval contains no physical keys. */
    boolean isEmpty() {
        return startKey == null
                || (endKey != null
                        && UnsignedBytes.lexicographicalComparator().compare(startKey, endKey)
                                >= 0);
    }

    @Nullable
    private static byte[] encodeEndpoint(
            @Nullable InternalRow endpoint, OrderedKeyEncoder encoder) {
        if (endpoint == null) {
            return null;
        }
        checkArgument(
                endpoint.getFieldCount() == 1 && !endpoint.isNullAt(0),
                "Range endpoints must contain exactly one non-null field.");
        return encoder.encodeKey(endpoint);
    }

    private static byte[] concatenate(byte[] prefix, byte[] value) {
        byte[] result = Arrays.copyOf(prefix, prefix.length + value.length);
        System.arraycopy(value, 0, result, prefix.length, value.length);
        return result;
    }
}
