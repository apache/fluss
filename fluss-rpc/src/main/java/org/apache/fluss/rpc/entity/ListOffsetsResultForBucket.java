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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.protocol.ApiError;

/** Result of {@link ListOffsetsRequest} for each table bucket. */
@Internal
public class ListOffsetsResultForBucket extends ResultForBucket {
    /**
     * The visible offset for this table bucket. if the {@link ListOffsetsRequest} is from follower,
     * the offset is LogEndOffset(LEO), otherwise, the request is from client, it will be
     * HighWatermark(HW).
     */
    private final long offset;

    public ListOffsetsResultForBucket(TableBucket tableBucket, long offset) {
        this(tableBucket, offset, ApiError.NONE);
    }

    public ListOffsetsResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, -1, error);
    }

    private ListOffsetsResultForBucket(TableBucket tableBucket, long offset, ApiError error) {
        super(tableBucket, error);
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ListOffsetsResultForBucket that = (ListOffsetsResultForBucket) o;
        return offset == that.offset;
    }

    @Override
    public String toString() {
        return "ListOffsetsResultForBucket{"
                + "offset="
                + offset
                + ", tableBucket="
                + tableBucket
                + ", error="
                + error
                + '}';
    }
}
