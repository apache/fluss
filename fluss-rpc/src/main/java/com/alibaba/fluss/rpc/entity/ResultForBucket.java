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

package com.alibaba.fluss.rpc.entity;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.protocol.ApiError;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The abstract class for request result for each table bucket. This will be converted to
 * protoBuffer class.
 */
@Internal
public abstract class ResultForBucket {
    protected final TableBucket tableBucket;
    protected final ApiError error;

    public ResultForBucket(TableBucket tableBucket) {
        this(tableBucket, ApiError.NONE);
    }

    public ResultForBucket(TableBucket tableBucket, ApiError error) {
        this.tableBucket = tableBucket;
        this.error = error;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public long getTableId() {
        return tableBucket.getTableId();
    }

    public int getBucketId() {
        return tableBucket.getBucket();
    }

    /** Returns true if the request is failed. */
    public boolean failed() {
        return error.isFailure();
    }

    /** Returns true if the request is succeeded. */
    public boolean succeeded() {
        return error.isSuccess();
    }

    public int getErrorCode() {
        return error.error().code();
    }

    public @Nullable String getErrorMessage() {
        return error.message();
    }

    public ApiError getError() {
        return error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResultForBucket that = (ResultForBucket) o;
        if (!Objects.equals(tableBucket, that.tableBucket)) {
            return false;
        }
        return Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableBucket, error);
    }

    @Override
    public String toString() {
        return "ResultForBucket{" + "tableBucket=" + tableBucket + ", error=" + error + '}';
    }
}
