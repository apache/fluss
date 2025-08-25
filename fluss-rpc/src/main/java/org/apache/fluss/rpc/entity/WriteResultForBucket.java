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
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;

/**
 * The abstract write operation result of per table bucket. the write operation may be ProduceLog or
 * PutKv.
 */
@Internal
public abstract class WriteResultForBucket extends ResultForBucket {
    /**
     * The end offset of write log to leader replica in local. If the write operation is ProduceLog,
     * this field is logEndOffset, if the write operation is PutKv, this field is
     * changeLogEndOffset.
     */
    private final long writeLogEndOffset;

    public WriteResultForBucket(TableBucket tableBucket, long writeLogEndOffset, ApiError error) {
        super(tableBucket, error);
        this.writeLogEndOffset = writeLogEndOffset;
    }

    public long getWriteLogEndOffset() {
        return writeLogEndOffset;
    }

    /** Copy the instance of this write result with the new error. */
    public abstract <T extends WriteResultForBucket> T copy(Errors newError);

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
        WriteResultForBucket that = (WriteResultForBucket) o;
        return writeLogEndOffset == that.writeLogEndOffset;
    }
}
