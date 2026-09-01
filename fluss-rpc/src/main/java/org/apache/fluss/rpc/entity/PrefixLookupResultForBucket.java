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

package org.apache.fluss.rpc.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.utils.ByteArraySlice;

import java.util.List;

/**
 * Result of {@link PrefixLookupRequest} for each table bucket.
 *
 * <p>Successful lookup values are already converted to the RPC value representation.
 */
public class PrefixLookupResultForBucket extends ResultForBucket {

    private final List<List<ByteArraySlice>> values;

    /** Creates a successful prefix lookup result with RPC-ready values. */
    public PrefixLookupResultForBucket(TableBucket tableBucket, List<List<ByteArraySlice>> values) {
        this(tableBucket, values, ApiError.NONE);
    }

    /** Creates a failed prefix lookup result. */
    public PrefixLookupResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, null, error);
    }

    private PrefixLookupResultForBucket(
            TableBucket tableBucket, List<List<ByteArraySlice>> values, ApiError error) {
        super(tableBucket, error);
        this.values = values;
    }

    /** Returns the RPC-ready values for each prefix key. */
    public List<List<ByteArraySlice>> prefixLookupValues() {
        return values;
    }
}
